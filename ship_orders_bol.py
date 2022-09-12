import asyncio
import configparser
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def install(package):
    subprocess.call([sys.executable, "-m", "pip", "install", package])


try:
    import pandas as pd
    import numpy as np
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("pandas")
    import pandas as pd
    import numpy as np
try:
    import httpx
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("httpx")
    import httpx
try:
    import requests
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("requests")
    import requests

try:
    import openpyxl
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("openpyxl")
    import openpyxl
try:
    from sqlalchemy import exc
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
except ModuleNotFoundError as ve:
    print(f"{ve} trying to install")
    install("mysqlclient")
    install("sqlalchemy")
    from sqlalchemy import create_engine
    from sqlalchemy import exc
    from sqlalchemy.engine.url import URL

config = configparser.ConfigParser()
config.read(Path.home() / "Dropbox" / "MACRO" / "bol_export_files.ini")
date_now = datetime.now().strftime("%c").replace(":", "-")

dpd_shipment_winkels = [x.strip() for x in config.get("welke_bol_winkels_dpd_shiping", "winkels").lower().split(",") if x]

config_db = dict(
    drivername="mariadb",
    username=config.get("database odin", "user"),
    password=config.get("database odin", "password"),
    host=config.get("database odin", "host"),
    port=config.get("database odin", "port"),
    database=config.get("database odin", "database"),
)
engine_db = create_engine(URL.create(**config_db))


class BOL_API:
    host = None
    key = None
    secret = None
    access_token = None
    access_token_expiration = None

    def __init__(self, host, key, secret):
        # set init values on creation
        self.host = host
        self.key = key
        self.secret = secret

        try:
            self.access_token = self.getAccessToken()
            if self.access_token is None:
                raise Exception("Request for access token failed.")
        except Exception as e:
            print(e)
        else:
            self.access_token_expiration = time.time() + 220

    def getAccessToken(self):
        # request the JWT
        try:
            # request an access token
            init_request = requests.post(self.host, auth=(self.key, self.secret))
            init_request.raise_for_status()
        except Exception as e:
            print(e)
            return None
        else:
            token = json.loads(init_request.text)["access_token"]
            if token:  # add right headers
                post_header = {
                    "Accept": "application/vnd.retailer.v7+json",
                    "Content-Type": "application/vnd.retailer.v7+json",
                    "Authorization": "Bearer " + token,
                    "Connection": "keep-alive",
                }
            return post_header

    class Decorators:
        @staticmethod
        def refreshToken(decorated):
            # check the JWT and refresh if necessary
            def wrapper(api, *args, **kwargs):
                if time.time() > api.access_token_expiration:
                    api.access_token = api.getAccessToken()
                return decorated(api, *args, **kwargs)

            return wrapper

        @staticmethod
        def handle_url_exceptions(f):
            async def wrapper(*args, **kw):
                try:
                    return await f(*args, **kw)
                except httpx.HTTPStatusError as exc:
                    print(f"HTTPStatus response {exc.response.status_code} while requesting {exc.request.url!r}.")
                except httpx.ConnectError as e:
                    print(f">connectie fout naar bol {e.request.url}")
                except httpx.ConnectTimeout as e:
                    print(f"> timeout van bol api {e.request.url}")
                except httpx.HTTPError as exc:
                    print(f"HTTPError response {exc.response.status_code} while requesting {exc.request.url!r}.")

            return wrapper

    def open_orders_pd(bol_winkel):
        return pd.read_sql(
            f"""SELECT I.orderid,
            I.order_orderitemid,
            shipmentdetails_salutationcode,
            shipmentdetails_firstname,
            shipmentdetails_surname,
            shipmentdetails_zipcode,
            order_offerreference,
            order_ean
            FROM   orders_bol O
                LEFT JOIN orders_info_bol I
                        ON O.orderid = I.orderid
            WHERE O.active_order = True AND O.winkel LIKE '{bol_winkel}' ORDER BY O.datetimeorderplaced DESC """,
            engine_db,
        )

    @Decorators.handle_url_exceptions
    @Decorators.refreshToken
    async def send_shipment(self, client, url, info):
        resp = await client.put(url, headers=self.access_token, data=info)
        resp.raise_for_status()
        return resp

    async def send_shiping_info_to_bol(self, items):
        timeout = httpx.Timeout(5, read=None)
        limits = httpx.Limits(max_keepalive_connections=2, max_connections=4)
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            tasks = []
            for item in items:
                for it in item:
                    all_order_items = it["order_orderitemid_all"].split("_")
                    for order_item_ids in all_order_items:
                        transport_info_dict = {
                            "orderItems": [{"orderItemId": order_item_ids}],
                            "shipmentReference": None,
                            "transport": {"transporterCode": "DPD-NL", "trackAndTrace": it["parcel_number"]},
                        }
                        transport_info_json = json.dumps(transport_info_dict, indent=4)
                    url = f"https://api.bol.com/retailer/orders/shipment"
                    tasks.append(asyncio.ensure_future(self.send_shipment(client, url, transport_info_json)))
            processed_items = await asyncio.gather(*tasks)
        result_list_filterd = [i for i in processed_items if i]
        process_id_posted_products = [resp.json().get("processStatusId") for resp in result_list_filterd]
        process_id_posted_producs_list = [process_id_posted_products[i : i + 100] for i in range(0, len(process_id_posted_products), 100)]
        return process_id_posted_producs_list

    @Decorators.handle_url_exceptions
    @Decorators.refreshToken
    async def status_bol_proces(self, client_res, url):
        resp = await client_res.get(url, headers=self.access_token)
        resp.raise_for_status()
        return resp

    async def results_bol_upload(self, updated, winkel, import_file):
        timeout = httpx.Timeout(30, read=None)
        limits = httpx.Limits(max_keepalive_connections=2, max_connections=6)
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client_res:
            tasks_res = []
            for proces_ids in updated:
                for proces_id in proces_ids:
                    url = f"https://api.bol.com/retailer/process-status/{proces_id}"
                    tasks_res.append(asyncio.ensure_future(self.status_bol_proces(client_res, url)))
            result_list = await asyncio.gather(*tasks_res)
        result_list_filterd = [i for i in result_list if i]
        if result_list_filterd:
            results = [status_result.json() if status_result else {} for status_result in result_list_filterd]
            verwerkte_resultaten = pd.DataFrame(results)
            verwerkte_resultaten_ref = verwerkte_resultaten.merge(import_file, left_on="entityId", right_on="order_orderitemid", how="left")
            verwerkte_resultaten_ref.to_csv(f"{Path.cwd() / 'verwerkt'}{os.sep}dpd_bol_api_tt_{winkel}_{date_now}.csv", index=False)


# read info all dpd exports
export_files = [x for x in (Path.cwd() / "import").glob(f"*.csv") if x.is_file()]
if len(export_files) > 1:
    dpd_shipment_info = pd.concat(
        [
            pd.read_csv(
                f,
                sep=";",
                dtype={"parcel_number": object},
                usecols=["parcel_number", "parcel_reference1", "recipient_zip"],
            )
            for f in export_files
        ]
    )
elif len(export_files) == 1:
    dpd_shipment_info = pd.read_csv(
        max(export_files),
        sep=";",
        dtype={"parcel_number": object},
        usecols=["parcel_number", "parcel_reference1", "recipient_zip"],
    )
else:
    dpd_shipment_info = pd.DataFrame(columns=["parcel_number", "parcel_reference1", "recipient_zip"])
    print("oeps, je moet een export dpd bestand in de import map plaatsen")


for winkel in dpd_shipment_winkels:
    client_id, client_secret, bol_winkel, winkel_letter = [x.strip() for x in config.get("bol_winkels_api", winkel).split(",")]

    bol_open_orders = BOL_API.open_orders_pd(bol_winkel).assign(orderid=lambda x: x.orderid.str.split("_").str[0].astype("int64"))
    order_to_sent_to_bol = bol_open_orders.merge(
        dpd_shipment_info,
        left_on=["orderid", "shipmentdetails_zipcode"],
        right_on=["parcel_reference1", "recipient_zip"],
        how="left",
    ).dropna(subset="parcel_reference1")
    gb = (
        order_to_sent_to_bol.groupby("orderid")["order_orderitemid"]
        .apply("_".join)
        .reset_index()
        .rename(columns={"order_orderitemid": "order_orderitemid_all"})
    )# voor als een order meerdere item id's heeft
    order_to_sent_to_bol = order_to_sent_to_bol.merge(gb, on="orderid").drop_duplicates("orderid")
    order_to_sent_to_bol_dict = order_to_sent_to_bol.to_dict("records")
    bol_items_max_per_request = [order_to_sent_to_bol_dict[i : i + 100] for i in range(0, len(order_to_sent_to_bol_dict), 100)]
    bol_call_upload = BOL_API(config["bol_api_urls"]["authorize_url"], client_id, client_secret)
    results = asyncio.run(bol_call_upload.send_shiping_info_to_bol(bol_items_max_per_request))
    if len(results) > 0:
        time.sleep(30)
        bol_call_results = BOL_API(config["bol_api_urls"]["authorize_url"], client_id, client_secret)
        asyncio.run(
            bol_call_results.results_bol_upload(results, bol_winkel, order_to_sent_to_bol[["orderid", "order_orderitemid", "parcel_number"]])
        )

for file in export_files: # opruimen zodra verzonden
    file.unlink()
