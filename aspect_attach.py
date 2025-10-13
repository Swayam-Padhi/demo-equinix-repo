import argparse
import json
import sys
import subprocess
import requests
import google.auth
from google.cloud import bigquery
from urllib.parse import quote
from google.api_core.exceptions import NotFound
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# ----------------- Argument Parsing -----------------
parser = argparse.ArgumentParser(description="Attach Dataplex aspects to BigQuery tables.")
parser.add_argument("--lake", required=True, help="Dataplex Lake ID")
parser.add_argument("--asset", required=False, help="Asset (dataset) name (optional)")
parser.add_argument("--table", required=False, help="Table name (optional)")
parser.add_argument("--aspects", required=True, help="Select 'mandatory' or provide comma-separated aspects/groups")
args = parser.parse_args()

# ----------------- Load Aspects -----------------
try:
    with open("aspects.json", "r") as f:
        aspects_json = json.load(f)
except FileNotFoundError:
    print(f"{Fore.RED}Error: aspects.json file not found.")
    sys.exit(1)

aspect_groups = aspects_json.get("groups", {})
all_aspects = {k: v for k, v in aspects_json.items() if k != "groups"}

# ----------------- Parse & Expand Input -----------------
input_aspects = [a.strip() for a in args.aspects.split(",") if a.strip()]

if not input_aspects:
    print(f"{Fore.RED}Error: You must select 'mandatory' or provide at least one aspect/group.")
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
    print(f"{Fore.RED}Error: These aspects are not defined: {', '.join(invalid_aspects)}")
    print(f"{Fore.YELLOW}Available aspects: {', '.join(all_aspects.keys())}")
    sys.exit(1)

TARGET_ASPECTS = expanded_aspects

# ----------------- Assign Targets -----------------
TARGET_LAKE_ID = args.lake.strip()
TARGET_DATASET = args.asset.strip() if args.asset else None
TARGET_TABLE = args.table.strip() if args.table else None

# ----------------- Config -----------------
PROJECT_ID = "clean-aleph-411709"
LOCATION = "us-central1"
ENTRY_GROUP = "@bigquery"
BASE_URL = "https://dataplex.googleapis.com/v1"
access_token = subprocess.getoutput("gcloud auth print-access-token")
ASPECTS_FILE = "aspects.json"

# ----------------- Helper: Attach Aspects -----------------
def attach_aspects(headers, bq_project_id, bq_dataset_id, table_id, dataset_location, aspects_data):
    entry_id = f"bigquery.googleapis.com/projects/{bq_project_id}/datasets/{bq_dataset_id}/tables/{table_id}"
    entry_name = f"projects/{PROJECT_ID}/locations/{dataset_location}/entryGroups/{ENTRY_GROUP}/entries/{quote(entry_id)}"
    url = f"{BASE_URL}/{entry_name}"

    aspects_payload = {}
    for aspect_name in TARGET_ASPECTS:
        aspects_payload[f"{PROJECT_ID}.{dataset_location}.{aspect_name}"] = {"data": aspects_data[aspect_name]}

    payload = {"name": entry_name, "aspects": aspects_payload}
    response = requests.patch(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print(f"{Fore.GREEN}Aspects attached successfully to {table_id}")
    else:
        try:
            error_json = response.json()
            error_message = error_json.get("error", {}).get("message", "")
            print(f"{Fore.RED}Failed ({response.status_code}): {error_message}")
        except json.JSONDecodeError:
            print(f"{Fore.RED}Failed ({response.status_code}): {response.text}")

# ----------------- Main Logic -----------------
def list_dataplex_assets_safely():
    try:
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        print(f"{Fore.GREEN}Authentication successful.")
    except google.auth.exceptions.DefaultCredentialsError:
        print(f"{Fore.RED}Authentication failed. Run: gcloud auth application-default login")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    bq_client = bigquery.Client(project=PROJECT_ID)
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"

    with open(ASPECTS_FILE, "r") as f:
        aspects_data = json.load(f)

    try:
        lake_name_path = f"{parent}/lakes/{TARGET_LAKE_ID}"
        lake_response = requests.get(f"{BASE_URL}/{lake_name_path}", headers=headers)
        lake_response.raise_for_status()
        print(f"{Fore.BLUE}Processing Lake: {TARGET_LAKE_ID}")

        zones_response = requests.get(f"{BASE_URL}/{lake_name_path}/zones", headers=headers)
        zones_response.raise_for_status()
        zones_list = zones_response.json().get("zones", [])

        if not zones_list:
            print(f"{Fore.YELLOW}No zones found for the target lake.")
            return

        for zone in zones_list:
            zone_id = zone["name"].split("/")[-1]
            print(f"{Fore.BLUE}Zone: {zone_id}")

            assets_response = requests.get(f"{BASE_URL}/{zone['name']}/assets", headers=headers)
            assets_response.raise_for_status()
            assets_list = assets_response.json().get("assets", [])

            if not assets_list:
                print(f"{Fore.YELLOW}No assets found in this zone.")
                continue

            for asset in assets_list:
                asset_id = asset['name'].split('/')[-1]
                asset_type = asset.get('resourceSpec', {}).get('type')
                print(f"{Fore.CYAN}Asset: {asset_id} ({asset_type})")

                if TARGET_DATASET and asset_id != TARGET_DATASET:
                    continue

                if asset_type == 'BIGQUERY_DATASET':
                    bq_resource_full_path = asset['resourceSpec'].get('resource')
                    display_path = bq_resource_full_path.replace("//bigquery.googleapis.com/", "")
                    parts = display_path.split('/')
                    bq_project_id = parts[1]
                    bq_dataset_id = parts[3]
                    dataset_ref = bigquery.DatasetReference(bq_project_id, bq_dataset_id)
                    bq_dataset_obj = bq_client.get_dataset(dataset_ref)
                    dataset_location = bq_dataset_obj.location.lower()

                    tables = list(bq_client.list_tables(dataset_ref))
                    if not tables:
                        print(f"{Fore.YELLOW}No tables found.")
                        continue

                    for table in tables:
                        table_id = table.table_id
                        if TARGET_TABLE and table_id != TARGET_TABLE:
                            continue
                        attach_aspects(headers, bq_project_id, bq_dataset_id, table_id, dataset_location, aspects_data)
                else:
                    print(f"{Fore.YELLOW}Skipping non-BigQuery asset.")

    except requests.exceptions.HTTPError as http_err:
        print(f"{Fore.RED}HTTP error: {http_err}")
        print(f"{Fore.RED}Response Body: {http_err.response.text}")
    except Exception as e:
        print(f"{Fore.RED}Unexpected error: {e}")

if __name__ == "__main__":
    list_dataplex_assets_safely()
