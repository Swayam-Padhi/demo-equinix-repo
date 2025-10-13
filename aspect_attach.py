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
parser = argparse.ArgumentParser(description="Attach Dataplex aspects to BigQuery assets/tables.")
parser.add_argument("--entry_type", required=True, choices=["asset", "table"], help="Entry type: asset or table")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=True, help="Dataplex Asset (dataset) name")
parser.add_argument("--table", required=False, help="Table name (required if entry_type=table)")
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

# ----------------- Assign Targets -----------------
ENTRY_TYPE = args.entry_type
TARGET_LAKE_ID = args.lake.strip()
TARGET_DATASET = args.asset.strip()
TARGET_TABLE = args.table.strip() if args.table else None

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
            error_message = error_json.get("error", {}).get("message", response.text)
        except:
            error_message = response.text
        print(f"Failed ({response.status_code}): {error_message}")
        return False

# ----------------- Main Logic -----------------
def main():
    try:
        # Authenticate using GitHub Actions SA key
        credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    except google.auth.exceptions.DefaultCredentialsError:
        print("Authentication failed. Ensure GOOGLE_APPLICATION_CREDENTIALS is set.")
        sys.exit(1)

    if not credentials.valid:
        credentials.refresh(Request())
    access_token = credentials.token
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    bq_client = bigquery.Client(project=PROJECT_ID)

    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    success_count = 0

    # Fetch asset details from Dataplex
    lake_path = f"projects/{PROJECT_ID}/locations/{LOCATION}/lakes/{TARGET_LAKE_ID}"
    assets_response = requests.get(f"{BASE_URL}/{lake_path}/zones", headers=headers)
    assets_response.raise_for_status()
    zones = assets_response.json().get("zones", [])

    if not zones:
        print("No zones found in the lake.")
        sys.exit(1)

    target_asset = None
    for zone in zones:
        zone_assets = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers).json().get("assets", [])
        for asset in zone_assets:
            if asset['name'].split('/')[-1] == TARGET_DATASET:
                target_asset = asset
                break
        if target_asset:
            break

    if not target_asset:
        print(f"Asset {TARGET_DATASET} not found.")
        sys.exit(1)

    if ENTRY_TYPE == "asset":
        # Asset-level entry
        entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{TARGET_DATASET}"
        attached = attach_aspects(headers, entry_name, aspects_data)
        if attached:
            success_count += 1
    else:
        # Table-level entry
        bq_resource_full_path = target_asset['resourceSpec'].get('resource') or target_asset['resourceSpec'].get('name')
        display_path = bq_resource_full_path.replace("//bigquery.googleapis.com/", "")
        parts = display_path.split('/')
        bq_project_id = parts[1]
        bq_dataset_id = parts[3]
        dataset_ref = bigquery.DatasetReference(bq_project_id, bq_dataset_id)

        try:
            tables = list(bq_client.list_tables(dataset_ref))
            if not tables:
                print("No tables found in dataset.")
                sys.exit(1)
            for table in tables:
                if TARGET_TABLE and table.table_id != TARGET_TABLE:
                    continue
                entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/bigquery.googleapis.com/projects/{bq_project_id}/datasets/{bq_dataset_id}/tables/{table.table_id}"
                attached = attach_aspects(headers, entry_name, aspects_data)
                if attached:
                    success_count += 1
        except NotFound:
            print(f"Dataset {bq_project_id}.{bq_dataset_id} not found.")
            sys.exit(1)

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} entries.")

if __name__ == "__main__":
    main()


