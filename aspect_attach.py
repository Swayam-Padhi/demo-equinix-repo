import argparse
import json
import sys
import requests
import google.auth
from google.auth.transport.requests import Request
from google.cloud import bigquery
from urllib.parse import quote
from google.api_core.exceptions import NotFound

# ----------------- Argument Parsing -----------------
parser = argparse.ArgumentParser(description="Attach Dataplex aspects to BigQuery tables or assets.")
parser.add_argument("--entry_type", required=True, choices=["asset", "table"], help="Entry type: asset or table")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=False, help="Asset (dataset) name")
parser.add_argument("--table", required=False, help="Table name (only for table entry type)")
parser.add_argument("--aspects", required=True, help="Select 'mandatory' or provide comma-separated aspects/groups")
args = parser.parse_args()

ENTRY_TYPE = args.entry_type
TARGET_LAKE_ID = args.lake.strip()
TARGET_DATASET = args.asset.strip() if args.asset else None
TARGET_TABLE = args.table.strip() if args.table else None

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
def attach_aspects(headers, entry_name, aspects_data):
    aspects_payload = {}
    for aspect_name in TARGET_ASPECTS:
        aspects_payload[f"{PROJECT_ID}.{LOCATION}.{aspect_name}"] = {"data": aspects_data[aspect_name]}

    payload = {"name": entry_name, "aspects": aspects_payload}
    response = requests.patch(f"{BASE_URL}/{entry_name}", headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print(f"Aspects attached successfully to {entry_name}")
        return True
    else:
        try:
            error_json = response.json()
            error_message = error_json.get("error", {}).get("message", "")
        except json.JSONDecodeError:
            error_message = response.text
        print(f"Failed ({response.status_code}): {error_message}")
        return False

# ----------------- Main Logic -----------------
def main():
    try:
        credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if not credentials.valid:
            credentials.refresh(Request())
        access_token = credentials.token
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    except Exception:
        print("Authentication failed. Ensure GOOGLE_APPLICATION_CREDENTIALS is set.")
        sys.exit(1)

    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    success_count = 0

    # Fetch zones
    lake_path = f"projects/{PROJECT_ID}/locations/{LOCATION}/lakes/{TARGET_LAKE_ID}"
    lake_resp = requests.get(f"{BASE_URL}/{lake_path}", headers=headers)
    lake_resp.raise_for_status()
    zones_resp = requests.get(f"{BASE_URL}/{lake_path}/zones", headers=headers)
    zones_resp.raise_for_status()
    zones = zones_resp.json().get("zones", [])

    if not zones:
        print("No zones found for the target lake.")
        sys.exit(1)

    for zone in zones:
        assets_resp = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers)
        assets_resp.raise_for_status()
        assets = assets_resp.json().get("assets", [])

        if not assets:
            continue

        for asset in assets:
            asset_id = asset['name'].split('/')[-1]
            asset_type = asset.get('resourceSpec', {}).get('type')

            if TARGET_DATASET and asset_id != TARGET_DATASET:
                continue

            if ENTRY_TYPE == "asset" and asset_type == "BIGQUERY_DATASET":
                # Asset-level entry
                bq_resource_full_path = asset['resourceSpec'].get('resource') or asset['resourceSpec'].get('name')
                entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{bq_resource_full_path.lstrip('//')}"
                attached = attach_aspects(headers, entry_name, aspects_data)
                if attached:
                    success_count += 1

            elif ENTRY_TYPE == "table" and asset_type == "BIGQUERY_DATASET":
                bq_resource_full_path = asset['resourceSpec'].get('resource') or asset['resourceSpec'].get('name')
                parts = bq_resource_full_path.replace("//bigquery.googleapis.com/", "").split('/')
                if len(parts) < 4:
                    print(f"Skipping asset {asset_id}: Unexpected resource path format.")
                    continue
                bq_project_id, bq_dataset_id = parts[1], parts[3]

                client = bigquery.Client(project=PROJECT_ID)
                dataset_ref = bigquery.DatasetReference(bq_project_id, bq_dataset_id)
                try:
                    tables = list(client.list_tables(dataset_ref))
                except NotFound:
                    print(f"Dataset {bq_project_id}.{bq_dataset_id} not found.")
                    continue

                for table in tables:
                    if TARGET_TABLE and table.table_id != TARGET_TABLE:
                        continue
                    entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/bigquery.googleapis.com/projects/{bq_project_id}/datasets/{bq_dataset_id}/tables/{table.table_id}"
                    attached = attach_aspects(headers, entry_name, aspects_data)
                    if attached:
                        success_count += 1

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} entries.")

if __name__ == "__main__":
    main()


