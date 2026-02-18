import json
import logging
import requests
import pandas as pd
from datetime import datetime
from utils.bigquery import get_bq_client, get_payload_data_from_bq
from utils.helper import decrypt_qianyi_auth, generate_qianyi_wms_sign

# --- CONFIGURATION ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

CTX = {
    "owner_code": "ERB2",
    "gcp_project_id": "bigquery-poc-366607",
    "merchant_table_id": "shipping.mysql_merchantdb_merchant_by_honeywell",
    "warehouse_table_id": "fulfillment.mysql_productdb_qianyi_warehouse"
}

# Change this to a list of exact SKUs you want to find
TARGET_SKUS = ["00ERXK0260"]
BASE_URL = "http://edi-glink.800best.com/gateway/api/glink"

def fetch_and_validate():
    logger.info(f"Fetching BQ payloads for owner: {CTX['owner_code']}...")
    payloads = get_payload_data_from_bq(CTX, logger)
    
    if not payloads:
        logger.error("No data found in BQ.")
        return

    first_payload = payloads[0]
    try:
        decrypted_str = decrypt_qianyi_auth(
            first_payload.get("authentication_info"), 
            first_payload.get("wms_type"), 
            first_payload.get("merchant_id")
        )
        auth_info = json.loads(decrypted_str)
        partner_id = auth_info.get("partner_id")
        partner_key = auth_info.get("partner_key") or auth_info.get("partnerKey")
    except Exception as e:
        logger.error(f"Decryption failed: {e}")
        return

    all_items_with_page = [] # List to store dicts including page info
    page = 1
    page_size = 200
    
    logger.info(f"Starting API pagination for {CTX['owner_code']}...")
    while True:
        biz_data_dict = {
            "customerCode": CTX["owner_code"], # Changed from "AFBP" to use CTX
            "page": page,
            "pageSize": page_size,
            "updatedTimeFrom": "2020-01-01T00:00:00Z",
            "updatedTimeTo": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        biz_data_str = json.dumps(biz_data_dict, separators=(",", ":"))
        sign = generate_qianyi_wms_sign(biz_data_str, partner_key)

        params = {
            "serviceType": "GLINK_QUERY_ITEM_NOTIFY",
            "partnerId": partner_id,
            "bizData": biz_data_str,
            "sign": sign
        }

        try:
            response = requests.post(BASE_URL, params=params, json=biz_data_dict)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("itemList", [])
            
            # Tag each item with the current page number before adding to the list
            for item in items:
                item["found_on_page"] = page
                all_items_with_page.append(item)
                
            total = data.get("total", 0)
            logger.info(f"Page {page}: Fetched {len(items)} items.")

            if len(all_items_with_page) >= total or not items:
                break
            page += 1
            
        except Exception as e:
            logger.error(f"API Request failed at page {page}: {e}")
            break

    # --- EXACT SKU VALIDATION ---
    if not all_items_with_page:
        logger.warning("No items retrieved.")
        return

    df = pd.DataFrame(all_items_with_page)
    
    # 1. Exact Match Logic: Using .isin() for high performance on exact strings
    matches = df[df['sku'].isin(TARGET_SKUS)].copy()

    print(f"\n--- Validation Summary ---")
    print(f"Total Items Scanned: {len(df)}")
    print(f"Target SKUs: {TARGET_SKUS}")
    
    if not matches.empty:
        # 2. Show the SKU, Description, and the Page Number
        print(f"Matches Found: {len(matches)}")
        print(matches[['sku', 'found_on_page', 'description']].to_string(index=False))
    else:
        print("Result: No exact SKU matches found across all pages.")

if __name__ == "__main__":
    fetch_and_validate()