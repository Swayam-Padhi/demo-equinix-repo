import argparse
import json
import sys
import os
import requests
from urllib.parse import quote
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import Request
import google.auth

# ----------------- Argument Parsing -----------------
parser = argparse.ArgumentParser(description="Attach Dataplex aspects to BigQuery assets/tables.")
parser.add_argument("--entry_type", required=True, choices=["asset", "table"], help="Entry type: asset or table")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=False, help="Asset (dataset) name")
parser.add_argument("--table", required=False, help="Table name (only if entry_type=table)")
parser.add_argument("--aspects", required=True, help="Comma-separated aspects/groups")
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
    print("Error: You must provide at least one aspect/group.")
    sys.exit(1)

expanded_aspects = []
for aspect_name in input_aspects:
    if aspect_name in aspect_groups:
        expanded_aspects.extend(aspect_groups[aspect_name])
    else:
        expanded_aspects.append(aspect_name)
expanded_aspects = list(dict.fromkeys(expanded_aspects))

invalid_aspects = [a for a in expanded_aspects if a not in all_aspects]
if invalid_aspects:
    print(f"Error: These aspects are not defined: {', '.join(invalid_aspects)}")
    sys.exit(1)

TARGET_ASPECTS = expanded_aspects

# ----------------- Config -----------------
PROJECT_ID = "clean-aleph-411709"
LOCATION = "us-central1"
ENTRY_GROUP = "@bigquery"
BASE_URL = "https://dataplex.googleapis.com/v1"
ASPECTS_FILE = "aspects.json"

# ----------------- Helper: Attach Aspects -----------------
def attach_aspects(headers, entry_name, aspects_data):
    entry_url = f"{BASE_URL}/{entry_name}"  # Full URL
    payload = {
        "name": entry_name,
        "aspects": {f"{PROJECT_ID}.{LOCATION}.{a}": {"data": aspects_data[a]} for a in TARGET_ASPECTS}
    }
    response = requests.patch(entry_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print(f"Aspects attached successfully to {entry_name}")
        return True
    try:
        error_msg = response.json().get("error", {}).get("message", response.text)
    except:
        error_msg = response.text
    print(f"Failed ({response.status_code}) on {entry_name}: {error_msg}")
    return False

# ----------------- Main Logic -----------------
def main():
    # Authenticate using GCP Service Account Key
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("GOOGLE_APPLICATION_CREDENTIALS not set. Exiting.")
        sys.exit(1)

    try:
        credentials, _ = google.auth.load_credentials_from_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        if not credentials.valid:
            credentials.refresh(Request())
    except Exception as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}
    bq_client = bigquery.Client(project=PROJECT_ID)

    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    success_count = 0
    lake_path = f"projects/{PROJECT_ID}/locations/{LOCATION}/lakes/{args.lake}"
    print(f"Processing Lake: {args.lake}")

    zones_resp = requests.get(f"{BASE_URL}/{lake_path}/zones", headers=headers)
    zones_resp.raise_for_status()
    zones = zones_resp.json().get("zones", [])

    for zone in zones:
        assets_resp = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers)
        assets_resp.raise_for_status()
        assets = assets_resp.json().get("assets", [])

        for asset in assets:
            asset_id = asset['name'].split('/')[-1]  # Dataplex asset ID
            asset_type = asset.get('resourceSpec', {}).get('type')
            if args.asset and asset_id != args.asset:
                continue

            bq_resource = asset['resourceSpec'].get('resource') or asset['resourceSpec'].get('name')
            if not bq_resource:
                continue

            # ----------------- Asset Entry -----------------
            if args.entry_type == "asset":
                entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote(asset_id)}"
                print("Attaching aspects to asset entry:", entry_name)
                if attach_aspects(headers, entry_name, aspects_data):
                    success_count += 1

            # ----------------- Table Entries -----------------
            elif args.entry_type == "table" and asset_type == "BIGQUERY_DATASET":
                parts = bq_resource.replace("//bigquery.googleapis.com/", "").split('/')
                if len(parts) != 4:
                    continue
                bq_project, bq_dataset = parts[1], parts[3]
                bq_dataset_ref = bigquery.DatasetReference(bq_project, bq_dataset)

                try:
                    tables = list(bq_client.list_tables(bq_dataset_ref))
                except NotFound:
                    continue

                for table in tables:
                    if args.table and table.table_id != args.table:
                        continue
                    table_entry_id = f"bigquery.googleapis.com/projects/{bq_project}/datasets/{bq_dataset}/tables/{table.table_id}"
                    table_entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote(table_entry_id)}"
                    print("Attaching aspects to table entry:", table_entry_name)
                    if attach_aspects(headers, table_entry_name, aspects_data):
                        success_count += 1

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} entry(s).")


if __name__ == "__main__":
    main()



