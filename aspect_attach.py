import argparse
import json
import sys
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ----------------- Argument Parsing -----------------
parser = argparse.ArgumentParser(description="Attach Dataplex aspects to BigQuery entries.")
parser.add_argument("--entry_type", required=True, choices=["asset", "table"], help="Entry type: asset or table")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=True, help="Asset (dataset) name")
parser.add_argument("--table", required=False, help="Table name (required if entry_type is table)")
parser.add_argument("--aspects", required=True, help="Select 'mandatory' or provide comma-separated aspects/groups")
args = parser.parse_args()

# ----------------- Load Aspects -----------------
try:
    with open("aspects.json", "r") as f:
        aspects_json = json.load(f)
except FileNotFoundError:
    print("Error: aspects.json file not found.")
    sys.exit(1)

aspect_groups = aspects_json.get("groups", {})
all_aspects = {k: v for k, v in aspects_json.items() if k != "groups"}

# ----------------- Parse & Expand Input -----------------
input_aspects = [a.strip() for a in args.aspects.split(",") if a.strip()]

if not input_aspects:
    print("Error: You must select 'mandatory' or provide at least one aspect/group.")
    sys.exit(1)

expanded_aspects = []
for aspect_name in input_aspects:
    if aspect_name in aspect_groups:
        expanded_aspects.extend(aspect_groups[aspect_name])
    else:
        expanded_aspects.append(aspect_name)

expanded_aspects = list(dict.fromkeys(expanded_aspects))  # remove duplicates

# ----------------- Validate Aspects -----------------
invalid_aspects = [a for a in expanded_aspects if a not in all_aspects]
if invalid_aspects:
    print(f"Error: These aspects are not defined: {', '.join(invalid_aspects)}")
    print(f"Available aspects: {', '.join(all_aspects.keys())}")
    sys.exit(1)

TARGET_ASPECTS = expanded_aspects

# ----------------- Config -----------------
PROJECT_ID = "clean-aleph-411709"
LOCATION = "us-central1"
ENTRY_GROUP = "@bigquery"
BASE_URL = "https://dataplex.googleapis.com/v1"
ASPECTS_FILE = "aspects.json"

# ----------------- Helper: Attach Aspects -----------------
def attach_aspects(session, entry_name, aspects_data):
    url = f"{BASE_URL}/{entry_name}"
    aspects_payload = {}
    for aspect_name in TARGET_ASPECTS:
        aspects_payload[f"{PROJECT_ID}.{LOCATION}.{aspect_name}"] = {"data": aspects_data[aspect_name]}

    payload = {"name": entry_name, "aspects": aspects_payload}

    response = session.patch(url, json=payload)
    if response.status_code == 200:
        print(f"Aspects attached successfully to {entry_name}")
        return True
    else:
        try:
            error_message = response.json().get("error", {}).get("message", response.text)
        except:
            error_message = response.text
        print(f"Failed ({response.status_code}): {error_message}")
        return False

# ----------------- Main Logic -----------------
def main():
    try:
        credentials, _ = google.auth.default()
        authed_session = AuthorizedSession(credentials)
        bq_client = bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)

    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    success_count = 0

    # Entry type: asset
    if args.entry_type == "asset":
        entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote(args.asset)}"
        attached = attach_aspects(authed_session, entry_name, aspects_data)
        if attached:
            success_count += 1

    # Entry type: table
    else:
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, args.asset)
        try:
            dataset_obj = bq_client.get_dataset(dataset_ref)
        except NotFound:
            print(f"Dataset {PROJECT_ID}.{args.asset} not found.")
            sys.exit(1)
        tables = [t.table_id for t in bq_client.list_tables(dataset_ref)]
        if args.table:
            if args.table in tables:
                tables = [args.table]
            else:
                print(f"Table {args.table} not found in dataset {args.asset}.")
                sys.exit(1)

        for table_id in tables:
            entry_id = f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{args.asset}/tables/{table_id}"
            entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote(entry_id)}"
            attached = attach_aspects(authed_session, entry_name, aspects_data)
            if attached:
                success_count += 1

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} entries.")

if __name__ == "__main__":
    main()


