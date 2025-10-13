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
parser.add_argument("--entry_type", required=True, choices=["table", "asset"], help="Entry type: table or asset")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=False, help="Asset (dataset) name (optional for table, required for asset)")
parser.add_argument("--table", required=False, help="Table name (optional, only for table entry type)")
parser.add_argument("--aspects", required=True, help="Comma-separated aspects or groups")
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
    url = f"{BASE_URL}/{entry_name}"

    aspects_payload = {}
    for aspect_name in TARGET_ASPECTS:
        aspects_payload[f"{PROJECT_ID}.{LOCATION}.{aspect_name}"] = {"data": aspects_data[aspect_name]}

    payload = {"name": entry_name, "aspects": aspects_payload}
    response = requests.patch(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print(f"Aspects attached successfully to {entry_name}")
        return True
    elif response.status_code == 403:
        try:
            error_json = response.json()
            error_message = error_json.get("error", {}).get("message", "")
        except:
            error_message = response.text
        print(f"Permission denied ({response.status_code}): {error_message}")
        return False
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
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
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

    lake_path = f"projects/{PROJECT_ID}/locations/{LOCATION}/lakes/{args.lake}"
    lake_resp = requests.get(f"{BASE_URL}/{lake_path}", headers=headers)
    if lake_resp.status_code != 200:
        print(f"Failed to fetch lake {args.lake}")
        sys.exit(1)
    print(f"Processing Lake: {args.lake}")

    zones_resp = requests.get(f"{BASE_URL}/{lake_path}/zones", headers=headers)
    zones_resp.raise_for_status()
    zones = zones_resp.json().get("zones", [])

    if not zones:
        print("No zones found for the lake.")
        sys.exit(1)

    for zone in zones:
        zone_id = zone['name'].split('/')[-1]
        print(f"Zone: {zone_id}")
        assets_resp = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers)
        assets_resp.raise_for_status()
        assets = assets_resp.json().get("assets", [])

        for asset in assets:
            asset_id = asset['name'].split('/')[-1]
            asset_type = asset.get('resourceSpec', {}).get('type')
            if args.asset and asset_id != args.asset:
                continue

            if args.entry_type == "asset":
                entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote(asset['name'])}"
                if attach_aspects(headers, entry_name, aspects_data):
                    success_count += 1
            elif args.entry_type == "table" and asset_type == "BIGQUERY_DATASET":
                bq_path = asset['resourceSpec'].get('resource') or asset['resourceSpec'].get('name')
                if not bq_path:
                    continue
                display_path = bq_path.replace("//bigquery.googleapis.com/", "")
                parts = display_path.split('/')
                if len(parts) < 4:
                    continue
                bq_dataset_ref = bigquery.DatasetReference(parts[1], parts[3])
                try:
                    dataset_obj = bq_client.get_dataset(bq_dataset_ref)
                except NotFound:
                    continue

                tables = list(bq_client.list_tables(bq_dataset_ref))
                for table in tables:
                    if args.table and table.table_id != args.table:
                        continue
                    table_entry = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{quote('bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}'.format(parts[1], parts[3], table.table_id))}"
                    if attach_aspects(headers, table_entry, aspects_data):
                        success_count += 1

    if success_count == 0:
        print("No aspects were attached successfully. Exiting with failure.")
        sys.exit(1)
    else:
        print(f"Aspects attached successfully to {success_count} entry(s).")


if __name__ == "__main__":
    main()

