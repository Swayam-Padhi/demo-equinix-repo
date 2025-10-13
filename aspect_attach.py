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
parser.add_argument("--entry_type", required=True, choices=["asset", "table"], help="Type of entry to attach aspects to.")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=True, help="Asset (dataset) name")
parser.add_argument("--table", required=False, help="Table name (optional; applies to all if omitted)")
parser.add_argument("--aspects", required=True, help="Select 'mandatory' or provide comma-separated aspects/groups")
args = parser.parse_args()

# ----------------- Constants -----------------
PROJECT_ID = "clean-aleph-411709"
LOCATION = "us-central1"
ENTRY_GROUP = "@bigquery"
BASE_URL = "https://dataplex.googleapis.com/v1"
ASPECTS_FILE = "aspects.json"

# ----------------- Load Aspects -----------------
try:
    with open(ASPECTS_FILE, "r") as f:
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
expanded_aspects = list(dict.fromkeys(expanded_aspects))  # Remove duplicates

# ----------------- Validate Aspects -----------------
invalid_aspects = [a for a in expanded_aspects if a not in all_aspects]
if invalid_aspects:
    print(f"Error: Invalid aspects: {', '.join(invalid_aspects)}")
    print(f"Available aspects: {', '.join(all_aspects.keys())}")
    sys.exit(1)

TARGET_ASPECTS = expanded_aspects

# ----------------- Auth -----------------
try:
    credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not credentials.valid:
        credentials.refresh(Request())
    headers = {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}
except Exception as e:
    print(f"Authentication error: {e}")
    sys.exit(1)

bq_client = bigquery.Client(project=PROJECT_ID)

# ----------------- Helper: Attach Aspects -----------------
def attach_aspects(entry_name, aspects_data):
    aspects_payload = {}
    for aspect_name in TARGET_ASPECTS:
        key = f"{PROJECT_ID}.{LOCATION}.{aspect_name}"
        aspects_payload[key] = {"data": aspects_data[aspect_name]}

    payload = {"name": entry_name, "aspects": aspects_payload}
    response = requests.patch(f"{BASE_URL}/{entry_name}", headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print(f"Aspects attached successfully to {entry_name}")
        return True
    else:
        try:
            err = response.json()
            msg = err.get("error", {}).get("message", response.text)
        except:
            msg = response.text
        print(f"Failed ({response.status_code}): {msg}")
        return False

# ----------------- Main -----------------
def main():
    success_count = 0
    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    if args.entry_type == "asset":
        # Attach directly to the asset entry
        entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{args.asset}"
        if attach_aspects(entry_name, aspects_data):
            success_count += 1

    elif args.entry_type == "table":
        # Attach to one or all tables in the specified dataset
        lake_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/lakes/{args.lake}"
        lake_response = requests.get(f"{BASE_URL}/{lake_name}/zones", headers=headers)
        zones = lake_response.json().get("zones", [])

        for zone in zones:
            assets_response = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers)
            assets = assets_response.json().get("assets", [])

            for asset in assets:
                asset_id = asset["name"].split("/")[-1]
                if asset_id != args.asset:
                    continue

                bq_resource = asset["resourceSpec"].get("resource")
                if not bq_resource or "datasets" not in bq_resource:
                    continue

                parts = bq_resource.split("/")
                bq_project_id = parts[3]
                bq_dataset_id = parts[5]
                dataset_ref = bigquery.DatasetReference(bq_project_id, bq_dataset_id)
                try:
                    dataset_obj = bq_client.get_dataset(dataset_ref)
                except NotFound:
                    print(f"Dataset {bq_dataset_id} not found. Skipping.")
                    continue

                dataset_location = dataset_obj.location.lower()
                tables = list(bq_client.list_tables(dataset_ref))

                if not tables:
                    print(f"No tables found in dataset {bq_dataset_id}.")
                    continue

                for table in tables:
                    if args.table and table.table_id != args.table:
                        continue  # Skip others if specific table is given

                    entry_id = f"bigquery.googleapis.com/projects/{bq_project_id}/datasets/{bq_dataset_id}/tables/{table.table_id}"
                    entry_name = f"projects/{PROJECT_ID}/locations/{dataset_location}/entryGroups/{ENTRY_GROUP}/entries/{quote(entry_id)}"
                    if attach_aspects(entry_name, aspects_data):
                        success_count += 1

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} target(s).")

if __name__ == "__main__":
    main()
