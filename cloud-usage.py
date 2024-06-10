import argparse
import hashlib
import json
import logging
import uuid
import requests
import sys
import threading
import time
import tomllib
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

semaphore = threading.Semaphore(30)


def setup_es(cloud_id, user, pw, index, reset):
    # create connection to elasticsearch for output
    es = Elasticsearch(cloud_id=cloud_id, basic_auth=(user, pw))
    ping = es.ping()
    if ping:
        logging.debug("Connection to ES output succeeded")
    else:
        logging.fatal("Failed to connect to ES output! Check cloud ID and creds")
        sys.exit()

    if reset and es.indices.exists(index=index):
        logging.info("Deleting index " + index)
        es.indices.delete(index=index)

    if not es.indices.exists(index=index):
        logging.debug(f"Creating index {index}")
        mapping = {
            "properties": {
                "@timestamp": {"type": "date"},
                "organization.id": {"type": "keyword"},
                "organization.name": {"type": "keyword"},
                "deployment.id": {"type": "keyword"},
                "deployment.name": {"type": "keyword"},
            }
        }
        es.indices.create(index=index, mappings=mapping)
    return es


def read_config(config_path):
    logging.debug(f"Reading {config_path}")
    with open(config_path, "rb") as f:
        config_data = tomllib.load(f)

    return config_data


def lookback(n):
    yesterday = datetime.now() - timedelta(days=1)
    dates = set()
    for i in range(n):
        current_date = yesterday - timedelta(days=i)
        formatted_date = current_date.strftime("%Y-%m-%d")
        dates.add(formatted_date)
    return dates


def flatten_dat(data):
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

    # do resources? more complicated
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
                    doc["deployment.items"] = flatten_dat(data)
                results.append(doc)
            return
        else:
            tries += 1
            wait = 3
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
        logging.error(f"Failed to reach API when looking up org {org_id}")
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


def add_overage(org_id, day, ecus, es, index):
    doc = {}
    doc["@timestamp"] = day
    doc["organization.id"] = str(org_id)
    doc["organization.overage"] = ecus
    es.index(
        index=index,
        document=doc,
        id=create_uuid_from_string(day + str(org_id) + "overage"),
    )


def delete_and_add_forecast(org_id, base_url, headers):
    query_body = {"query": {"term": {"organization.forecast": {"value": "true"}}}}
    delete_resp = es.delete_by_query(
        index=config_data["output"]["index"], body=query_body
    )
    logging.debug(
        f'Deleted {delete_resp["deleted"]} forecast docs. Recalculating forecast now'
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
    bulk_ingest(es, config_data["output"]["index"], docs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Elastic cloud billing & usage data"
    )
    parser.add_argument(
        "-c", "--config", action="store", dest="config_path", default="config.toml"
    )
    parser.add_argument("-d", "--debug", action="store_true", default=False)
    parser.add_argument("-r", "--reset", action="store_true", default=False)
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    config_data = read_config(args.config_path)
    api_keys = config_data["api_keys"]

    results = []

    es = setup_es(
        config_data["output"]["cloud_id"],
        config_data["output"]["user"],
        config_data["output"]["pw"],
        config_data["output"]["index"],
        args.reset,
    )

    for customer in config_data["customer"]:
        org_id = customer["org_id"]
        logging.info(f"Processing {org_id}")

        if customer["system"] == "govcloud":
            base_url = "https://admin.us-gov-east-1.aws.elastic-cloud.com/api/v1"
            api_key = api_keys["govcloud"]
        elif customer["system"] == "commercial":
            base_url = "https://adminconsole.found.no/api/v1"
            api_key = api_keys["commercial"]
        else:
            logging.fatal(
                "Need to specify either govcloud or commercial for system in config"
            )
            sys.exit()

        headers = {
            "Authorization": f"ApiKey {api_key}",
            "Content-Type": "application/json",
        }

        org_name = get_org_name(base_url, headers, org_id)
        logging.debug(
            f"Found {org_id} in {customer['system']} with org name: {org_name}"
        )

        today = datetime.now().strftime("%Y-%m-%d")

        if customer["lookback"] >= 60:
            logging.warning(
                f"Lookback of {customer['lookback']} is high, APIs may reject due to too many requests"
            )

        logging.debug("Spinning threads to pull data from APIs")
        threads = []
        for day in lookback(customer["lookback"]):
            t = threading.Thread(
                target=worker_thread, args=(day, org_id, org_name, headers, results)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        bulk_ingest(es, config_data["output"]["index"], results)

        logging.info("Adding purchases")
        for purchase in customer["purchase"]:
            add_credits(
                org_id,
                purchase["date"].strftime("%Y-%m-%d"),
                purchase["ecu"],
                es,
                config_data["output"]["index"],
            )

        logging.info("Adding overage charges")
        for overage in customer["overage"]:
            add_overage(
                org_id,
                overage["date"].strftime("%Y-%m-%d"),
                overage["ecu"],
                es,
                config_data["output"]["index"],
            )

        logging.info("Calculating consumption forecast")
        delete_and_add_forecast(org_id, base_url, headers)
