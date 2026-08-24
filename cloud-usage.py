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

semaphore = threading.Semaphore(8)


def connect_es(config: dict, reset) -> Elasticsearch:
    connected = False
    client: Elasticsearch | None = None

    # Determine connection parameters based on cloud_id or endpoint
    if "cloud_id" in config:
        # Use Elastic Cloud connection
        es_params = {"cloud_id": config["cloud_id"]}
    elif "endpoint" in config:
        # Use direct Elasticsearch endpoint
        es_params = {"hosts": config["endpoint"]}
    else:
        raise Exception(
            "Either 'cloud_id' or 'endpoint' must be specified in the output config."
        )

    # Try API key authentication first
    if "api_key" in config:
        try:
            client = Elasticsearch(api_key=config["api_key"], **es_params)
            # Test the connection
            client.info()
            connected = True
        except Exception:
            pass

    # Fall back to basic auth if API key didn't work or wasn't provided
    if not connected and "user" in config and "password" in config:
        try:
            client = Elasticsearch(
                basic_auth=(config["user"], config["password"]), **es_params
            )
            # Test the connection
            client.info()
            connected = True
        except Exception:
            raise Exception(
                "Failed to connect to Elasticsearch with provided credentials."
            )

    if not connected or client is None:
        raise Exception(
            "Failed to connect to Elasticsearch. Please provide either 'api_key' or both 'user' and 'password' in the output config."
        )

    if reset and client.indices.exists(index=config["index"]):
        logging.info("Deleting index " + config["index"])
        client.indices.delete(index=config["index"])

    if not client.indices.exists(index=config["index"]):
        logging.debug(f"Creating index {config['index']}")
        mapping = {
            "dynamic_templates": [
                {
                    "resources_by_kind_instance_count": {
                        "path_match": "deployment.items.resources.by_kind.*.instance_count",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_kind_instance_hours": {
                        "path_match": "deployment.items.resources.by_kind.*.instance_hours",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_kind_hours": {
                        "path_match": "deployment.items.resources.by_kind.*.hours",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_kind_resource_count": {
                        "path_match": "deployment.items.resources.by_kind.*.resource_count",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_kind_avg_instance_count": {
                        "path_match": "deployment.items.resources.by_kind.*.avg_instance_count",
                        "mapping": {"type": "double"},
                    }
                },
                {
                    "resources_by_kind_price": {
                        "path_match": "deployment.items.resources.by_kind.*.price",
                        "mapping": {"type": "double"},
                    }
                },
                {
                    "resources_by_kind_price_per_hour": {
                        "path_match": "deployment.items.resources.by_kind.*.price_per_hour",
                        "mapping": {"type": "double"},
                    }
                },
                {
                    "resources_by_es_tier_instance_count": {
                        "path_match": "deployment.items.resources.by_es_tier.*.instance_count",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_es_tier_instance_hours": {
                        "path_match": "deployment.items.resources.by_es_tier.*.instance_hours",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_es_tier_hours": {
                        "path_match": "deployment.items.resources.by_es_tier.*.hours",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_es_tier_resource_count": {
                        "path_match": "deployment.items.resources.by_es_tier.*.resource_count",
                        "mapping": {"type": "long"},
                    }
                },
                {
                    "resources_by_es_tier_avg_instance_count": {
                        "path_match": "deployment.items.resources.by_es_tier.*.avg_instance_count",
                        "mapping": {"type": "double"},
                    }
                },
                {
                    "resources_by_es_tier_price": {
                        "path_match": "deployment.items.resources.by_es_tier.*.price",
                        "mapping": {"type": "double"},
                    }
                },
                {
                    "resources_by_es_tier_price_per_hour": {
                        "path_match": "deployment.items.resources.by_es_tier.*.price_per_hour",
                        "mapping": {"type": "double"},
                    }
                },
            ],
            "properties": {
                "@timestamp": {"type": "date"},
                "organization.id": {"type": "keyword"},
                "organization.name": {"type": "keyword"},
                "organization.name_internal": {"type": "keyword"},
                "organization.parent_name": {"type": "keyword"},
                "organization.credits": {"type": "double"},
                "organization.forecast": {"type": "boolean"},
                "organization.forecast_credits": {"type": "double"},
                "deployment.id": {"type": "keyword"},
                "deployment.name": {"type": "keyword"},
                "deployment.items.resources.totals.instance_count": {"type": "long"},
                "deployment.items.resources.totals.instance_hours": {"type": "long"},
                "deployment.items.resources.totals.avg_instance_count": {
                    "type": "double"
                },
                "deployment.items.resources.totals.hours": {"type": "long"},
                "deployment.items.resources.totals.price": {"type": "double"},
                "deployment.items.resources.totals.price_per_hour": {"type": "double"},
                "deployment.items.resources.totals.resource_count": {"type": "long"},
                "deployment.items.resources.totals.day_hours": {"type": "long"},
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


def get_es_tier(resource):
    sku = (resource.get("sku", "") or "").lower()
    name = (resource.get("name", "") or "").lower()
    identifiers = f"{sku} {name}"

    if "datahot" in identifiers:
        return "hot"
    if "datawarm" in identifiers:
        return "warm"
    if "datacold" in identifiers:
        return "cold"
    if "datafrozen" in identifiers:
        return "frozen"
    if ".master." in identifiers or " master " in identifiers:
        return "master"
    if ".ml." in identifiers or " ml " in identifiers:
        return "ml"
    if "coordinating" in identifiers:
        return "coordinating"
    if "ingest" in identifiers:
        return "ingest"
    if "datacontent" in identifiers or "data_content" in identifiers:
        return "content"
    return "other"


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

    # flatten resources into numeric summary fields for easier aggregations
    resources = data.pop("resources", [])
    data["resources"] = {
        "totals": {
            "instance_count": 0,
            "instance_hours": 0,
            "avg_instance_count": 0.0,
            "hours": 0,
            "price": 0.0,
            "price_per_hour": 0.0,
            "resource_count": 0,
            "day_hours": 0,
        },
        "by_kind": {},
        "by_es_tier": {},
    }
    day_hours = 0

    for resource in resources:
        kind = resource.get("kind", "unknown")
        instance_count = resource.get("instance_count", 0) or 0
        hours = resource.get("hours", 0) or 0
        price = resource.get("price", 0.0) or 0.0
        price_per_hour = resource.get("price_per_hour", 0.0) or 0.0

        if hours > day_hours:
            day_hours = hours

        kind_bucket = data["resources"]["by_kind"].setdefault(
            kind,
            {
                "instance_count": 0,
                "instance_hours": 0,
                "avg_instance_count": 0.0,
                "hours": 0,
                "price": 0.0,
                "price_per_hour": 0.0,
                "resource_count": 0,
            },
        )

        kind_bucket["instance_count"] += instance_count
        kind_bucket["instance_hours"] += instance_count * hours
        kind_bucket["hours"] += hours
        kind_bucket["price"] += price
        kind_bucket["price_per_hour"] += price_per_hour
        kind_bucket["resource_count"] += 1

        data["resources"]["totals"]["instance_count"] += instance_count
        data["resources"]["totals"]["instance_hours"] += instance_count * hours
        data["resources"]["totals"]["hours"] += hours
        data["resources"]["totals"]["price"] += price
        data["resources"]["totals"]["price_per_hour"] += price_per_hour
        data["resources"]["totals"]["resource_count"] += 1

        if kind == "elasticsearch":
            es_tier = get_es_tier(resource)
            tier_bucket = data["resources"]["by_es_tier"].setdefault(
                es_tier,
                {
                    "instance_count": 0,
                    "instance_hours": 0,
                    "avg_instance_count": 0.0,
                    "hours": 0,
                    "price": 0.0,
                    "price_per_hour": 0.0,
                    "resource_count": 0,
                },
            )
            tier_bucket["instance_count"] += instance_count
            tier_bucket["instance_hours"] += instance_count * hours
            tier_bucket["hours"] += hours
            tier_bucket["price"] += price
            tier_bucket["price_per_hour"] += price_per_hour
            tier_bucket["resource_count"] += 1

    if day_hours > 0:
        data["resources"]["totals"]["day_hours"] = day_hours
        data["resources"]["totals"]["avg_instance_count"] = (
            data["resources"]["totals"]["instance_hours"] / day_hours
        )
        for kind_bucket in data["resources"]["by_kind"].values():
            kind_bucket["avg_instance_count"] = kind_bucket["instance_hours"] / day_hours
        for tier_bucket in data["resources"]["by_es_tier"].values():
            tier_bucket["avg_instance_count"] = tier_bucket["instance_hours"] / day_hours

    return data


def worker_thread(day, org_id, org_name, org_name_internal, parent_name, headers, results):
    with semaphore:
        do_work(day, org_id, org_name, org_name_internal, parent_name, headers, results)


def do_work(day, org_id, org_name, org_name_internal, parent_name, headers, results):
    org_get_ok = False
    org_get_tries = 0
    wait = 10
    while not org_get_ok:
        res = requests.get(
            f"{base_url}/billing/costs/{org_id}/charts?from={day}&to={day}",
            headers=headers,
        )
        if res.status_code == 200:
            org_get_ok = True
            data = res.json()
            for deployment in data["data"][0]["values"]:
                logging.debug(
                    f"Fetching single day deployment usage for {org_name}/{org_id}/{deployment['name']}/{deployment['id']} for {day}"
                )
                doc = {}
                doc["@timestamp"] = day
                doc["_id"] = create_uuid_from_string(day + str(deployment["id"]))
                doc["organization.id"] = org_id
                doc["organization.name"] = org_name
                if org_name_internal:
                    doc["organization.name_internal"] = org_name_internal
                if parent_name:
                    doc["organization.parent_name"] = parent_name
                doc["deployment.id"] = deployment["id"]
                doc["deployment.name"] = deployment["name"]
                dep_get_ok = False
                dep_get_tries = 0
                while not dep_get_ok:
                    res = requests.get(
                        f"{base_url}/billing/costs/{org_id}/deployments/{deployment['id']}/items?from={day}&to={day}",
                        headers=headers,
                    )
                    if res.status_code == 200:
                        dep_get_ok = True
                        data = res.json()
                        doc["deployment.items"] = flatten(data)
                        results.append(doc)
                        break  # Exit while loop to move to next deployment
                    elif res.status_code == 404:
                        # Check if it's a resource_not_found error
                        try:
                            error_data = res.json()
                            if "errors" in error_data and len(error_data["errors"]) > 0:
                                error_code = error_data["errors"][0].get("code", "")
                                if error_code == "root.resource_not_found":
                                    dep_get_ok = (
                                        True  # Mark as handled to exit retry loop
                                    )
                                    break  # Exit while loop to move to next deployment
                        except (json.JSONDecodeError, KeyError, IndexError):
                            # If we can't parse the error, fall through to retry logic
                            pass

                    # For non-404 errors or unparseable 404s, retry
                    if not dep_get_ok:
                        dep_get_tries += 1
                        # Parse error message for cleaner logging
                        error_msg = ""
                        try:
                            error_data = res.json()
                            if "errors" in error_data and len(error_data["errors"]) > 0:
                                error_code = error_data["errors"][0].get("code", "")
                                error_message = error_data["errors"][0].get(
                                    "message", ""
                                )
                                if res.status_code == 429:
                                    error_msg = f"Rate limited ({error_code})"
                                else:
                                    error_msg = f"{error_code}: {error_message}"
                        except (json.JSONDecodeError, KeyError, IndexError):
                            error_msg = (
                                res.text[:100]
                                if len(res.text) <= 100
                                else res.text[:100] + "..."
                            )

                        logging.debug(
                            f"Deployment get API call attempt {dep_get_tries} failed with status code {res.status_code} - {error_msg}. Retrying in {wait} secs..."
                        )
                        time.sleep(wait)
        else:
            org_get_tries += 1
            # Parse error message for cleaner logging
            error_msg = ""
            try:
                error_data = res.json()
                if "errors" in error_data and len(error_data["errors"]) > 0:
                    error_code = error_data["errors"][0].get("code", "")
                    error_message = error_data["errors"][0].get("message", "")
                    if res.status_code == 429:
                        error_msg = f"Rate limited ({error_code})"
                    else:
                        error_msg = f"{error_code}: {error_message}"
            except (json.JSONDecodeError, KeyError, IndexError):
                error_msg = (
                    res.text[:100] if len(res.text) <= 100 else res.text[:100] + "..."
                )

            logging.debug(
                f"Org get API call attempt {org_get_tries} failed with status code {res.status_code} - {error_msg}. Retrying in {wait} secs..."
            )
            time.sleep(wait)


def create_uuid_from_string(val: str):
    hex_string = hashlib.md5(val.encode("UTF-8")).hexdigest()
    return str(uuid.UUID(hex=hex_string))


def get_org_name(base_url, headers, org_id):
    # get org base info (just the name for now)
    try:
        res = requests.get(f"{base_url}/organizations/{org_id}", headers=headers)
        if res.status_code == 200:
            data = json.loads(res.text)
            return data["name"]
        else:
            logging.debug(f"Org {org_id} not found in {base_url}")
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to reach API when looking up org {org_id}: {e}")
        return False


def bulk_ingest(es, index, docs):
    logging.debug("Sending data to output Elasticsearch")
    for ok, action in streaming_bulk(client=es, index=index, actions=yield_doc(docs)):
        if not ok:
            logging.error(f"{ok} {action}")


def yield_doc(docs):
    for doc in docs:
        yield doc


def add_credits(org_id, org_name, org_name_internal, parent_name, day, ecus, es, index):
    doc = {}
    doc["@timestamp"] = day
    doc["organization.id"] = str(org_id)
    doc["organization.name"] = org_name
    if org_name_internal:
        doc["organization.name_internal"] = org_name_internal
    if parent_name:
        doc["organization.parent_name"] = parent_name
    doc["organization.credits"] = ecus
    es.index(
        index=index,
        document=doc,
        id=create_uuid_from_string(day + str(org_id) + "purchase"),
    )


def delete_and_add_forecast(
    org_id, org_name, org_name_internal, parent_name, base_url, headers
):
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
        f"Deleted {delete_resp['deleted']} forecast docs. (Re)calculating forecast now"
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
    today = datetime.now().date()
    ninety_first_day = today + timedelta(days=look_forward)
    first_day_next_month = (ninety_first_day.replace(day=1) + timedelta(days=32)).replace(
        day=1
    )
    forecast_end_date = first_day_next_month - timedelta(days=1)
    forecast_days = (forecast_end_date - today).days

    for x in range(1, forecast_days + 1):
        doc = {}
        ts = str((today + timedelta(days=x)).strftime("%Y-%m-%d"))
        doc["@timestamp"] = ts
        doc["_id"] = create_uuid_from_string(ts + str(org_id) + "forecast")
        doc["organization.id"] = str(org_id)
        doc["organization.name"] = org_name
        if org_name_internal:
            doc["organization.name_internal"] = org_name_internal
        if parent_name:
            doc["organization.parent_name"] = parent_name
        doc["organization.forecast"] = True
        doc["organization.forecast_credits"] = daily
        docs.append(doc)

    logging.debug(
        f"Ingesting forecast for {org_id} through {forecast_end_date} ({forecast_days} days)"
    )
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
        org_name_internal = org.get("name_internal")
        parent_name = org.get("parent_name")
        display_name = org_name_internal if org_name_internal else org_id
        logging.info(f"Processing {display_name}")

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
                target=worker_thread,
                args=(
                    day,
                    org_id,
                    org_name,
                    org_name_internal,
                    parent_name,
                    headers,
                    results,
                ),
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
                    org_name,
                    org_name_internal,
                    parent_name,
                    purchase["date"].strftime("%Y-%m-%d"),
                    purchase["ecu"],
                    es,
                    cfg["output"]["index"],
                )

        logging.info("Calculating consumption forecast")
        delete_and_add_forecast(
            org_id, org_name, org_name_internal, parent_name, base_url, headers
        )
