import argparse
import hashlib
import json
import logging
import uuid
import requests
import sys
import threading
import time
import yaml
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

semaphore = threading.Semaphore(16)


def connect_es(config: dict, reset) -> Elasticsearch:
    if "api_key" in config:
        try:
            client = Elasticsearch(
                cloud_id=config["cloud_id"], api_key=config["api_key"]
            )
            # Test the connection
            client.info()
        except Exception:
            pass
    if "user" in config and "password" in config:
        try:
            client = Elasticsearch(
                cloud_id=config["cloud_id"],
                basic_auth=(config["user"], config["password"]),
            )
            # Test the connection
            client.info()
        except Exception:
            raise Exception(
                "Failed to connect to Elasticsearch with provided credentials."
            )

    if reset and client.indices.exists(index=config["index"]):
        logging.info("Deleting index " + config["index"])
        client.indices.delete(index=config["index"])

    if not client.indices.exists(index=config["index"]):
        logging.debug(f"Creating index {config["index"]}")
        mapping = {
            "properties": {
                "@timestamp": {"type": "date"},
                "organization.id": {"type": "keyword"},
                "organization.name": {"type": "keyword"},
                "deployment.id": {"type": "keyword"},
                "deployment.name": {"type": "keyword"},
            }
        }
        client.indices.create(index=config["index"], mappings=mapping)
    return client


def read_config(config_path):
    logging.debug(f"Reading {config_path}")
    with open(config_path) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    return cfg


def lookback(n):
    yesterday = datetime.now() - timedelta(days=1)
    dates = set()
    for i in range(n):
        current_date = yesterday - timedelta(days=i)
        formatted_date = current_date.strftime("%Y-%m-%d")
        dates.add(formatted_date)
    return dates


def flatten(data):
    # start with costs
    dimensions = data["costs"].pop("dimensions")
    for dimension in dimensions:
        data["costs"][dimension["type"]] = dimension["cost"]

    # now do data_transfer_and_storage
    items = data.pop("data_transfer_and_storage")
    data["dts"] = {}
    for item in items:
        item_key = item.pop("type")
        data["dts"][item_key] = item

    # do resources? more complicated (TODO)
    return data


def worker_thread(day, org_id, org_name, headers, results):
    with semaphore:
        do_work(day, org_id, org_name, headers, results)


def do_work(day, org_id, org_name, headers, results):
    ok = False
    tries = 0
    while not ok:
        res = requests.get(
            f"{base_url}/billing/costs/{org_id}/charts?from={day}&to={day}",
            headers=headers,
        )
        if res.status_code == 200:
            ok = True
            data = res.json()
            for deployment in data["data"][0]["values"]:
                doc = {}
                doc["@timestamp"] = day
                doc["_id"] = create_uuid_from_string(day + deployment["id"])
                doc["organization.id"] = org_id
                doc["organization.name"] = org_name
                doc["deployment.id"] = deployment["id"]
                doc["deployment.name"] = deployment["name"]
                res = requests.get(
                    f"{base_url}/billing/costs/{org_id}/deployments/{deployment['id']}/items?from={day}&to={day}",
                    headers=headers,
                )
                if res.status_code == 200:
                    data = res.json()
                    doc["deployment.items"] = flatten(data)
                results.append(doc)
            return
        else:
            tries += 1
            wait = 5
            logging.debug(
                f"API call attempt {tries} failed with status code {res.status_code}. Retrying in {wait} secs..."
            )
            time.sleep(wait)


def create_uuid_from_string(val: str):
    hex_string = hashlib.md5(val.encode("UTF-8")).hexdigest()
    return str(uuid.UUID(hex=hex_string))


def get_org_name(base_url, headers, org_id):
    # get org base info (just the name for now)
    try:
        res = requests.get(f"{base_url}/organizations/{org_id}", headers=headers)
        data = json.loads(res.text)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to reach API when looking up org {org_id}: {e}")
    if res.status_code == 200:
        return data["name"]
    else:
        logging.debug(f"Org {org_id} not found in {base_url}")
        return False


def bulk_ingest(es, index, docs):
    logging.debug("Sending data to output Elasticsearch")
    for ok, action in streaming_bulk(client=es, index=index, actions=yield_doc(docs)):
        if not ok:
            logging.error(f"{ok} {action}")


def yield_doc(docs):
    for doc in docs:
        yield doc


def add_credits(org_id, day, ecus, es, index):
    doc = {}
    doc["@timestamp"] = day
    doc["organization.id"] = str(org_id)
    doc["organization.credits"] = ecus
    es.index(
        index=index,
        document=doc,
        id=create_uuid_from_string(day + str(org_id) + "purchase"),
    )


def delete_and_add_forecast(org_id, base_url, headers):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"organization.id": {"value": org_id}}},
                    {"term": {"organization.forecast": {"value": "true"}}},
                ]
            }
        }
    }
    delete_resp = es.delete_by_query(index=cfg["output"]["index"], body=query_body)
    logging.debug(
        f'Deleted {delete_resp["deleted"]} forecast docs. (Re)calculating forecast now'
    )
    look_back = 7
    look_forward = 91
    start = (datetime.now().date() - timedelta(days=look_back)).strftime("%Y-%m-%d")
    end = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    logging.debug(f"Getting average daily costs {org_id} from {start} to {end}")
    res = requests.get(
        f"{base_url}/billing/costs/{org_id}?from={start}&to={end}",
        headers=headers,
    )
    logging.debug(f"Computing forecast for {org_id}")
    total = float(json.loads(res.text)["costs"]["total"])
    daily = float(total) / look_back
    docs = []
    td = datetime.today()
    for x in range(1, look_forward):
        doc = {}
        ts = str((td + timedelta(days=x)).strftime("%Y-%m-%d"))
        doc["@timestamp"] = ts
        doc["_id"] = create_uuid_from_string(ts + str(org_id) + "forecast")
        doc["organization.id"] = str(org_id)
        doc["organization.forecast"] = True
        doc["organization.forecast_credits"] = daily
        docs.append(doc)

    logging.debug(f"Ingesting forecast for {org_id} from {start} to {end}")
    bulk_ingest(es, cfg["output"]["index"], docs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Elastic cloud billing & usage data"
    )
    parser.add_argument(
        "-c", "--config", action="store", dest="config_path", default="config.yml"
    )
    parser.add_argument("-d", "--debug", action="store_true", default=False)
    parser.add_argument("-r", "--reset", action="store_true", default=False)
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = read_config(args.config_path)
    api_keys = cfg["billing_api_keys"]

    results = []

    es = connect_es(
        cfg["output"],
        args.reset,
    )

    for org in cfg["organizations"]:
        org_id = org["id"]
        logging.info(f"Processing {org_id}")

        if org["system"] == "govcloud":
            base_url = "https://admin.us-gov-east-1.aws.elastic-cloud.com/api/v1"
            api_key = api_keys["govcloud"]
        elif org["system"] == "commercial":
            base_url = "https://adminconsole.found.no/api/v1"
            api_key = api_keys["commercial"]
        else:
            logging.fatal(
                "You must specify either govcloud or commercial for system in organization config"
            )
            sys.exit()

        headers = {
            "Authorization": f"ApiKey {api_key}",
            "Content-Type": "application/json",
        }

        org_name = get_org_name(base_url, headers, org_id)
        if not org_name:
            continue
        logging.debug(f"Found {org_id} in {org['system']} with name: {org_name}")

        today = datetime.now().strftime("%Y-%m-%d")

        if org["lookback"] >= 60:
            logging.warning(
                f"Lookback of {org['lookback']} is high, APIs may reject due to too many requests. If you encounter errors, try re-running the script."
            )

        logging.debug("Spinning threads to pull data from APIs")
        threads = []
        for day in lookback(org["lookback"]):
            t = threading.Thread(
                target=worker_thread, args=(day, org_id, org_name, headers, results)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        bulk_ingest(es, cfg["output"]["index"], results)

        if "purchases" in org:
            logging.info("Adding ECU purchase info")
            for purchase in org["purchases"]:
                add_credits(
                    org_id,
                    purchase["date"].strftime("%Y-%m-%d"),
                    purchase["ecu"],
                    es,
                    cfg["output"]["index"],
                )

        logging.info("Calculating consumption forecast")
        delete_and_add_forecast(org_id, base_url, headers)
