# Import Libraries
import asyncio
import click
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

from sdk.extract_external_api import pipeline_extract_external_api
from sdk.utils.config import parse_config
from sdk.utils import ArgumentError, Logger, ProcessError
import aiohttp

# Import from your existing utils
from utils.bigquery import get_bq_client, fetch_qianyi_targets, get_payload_data_from_bq
from utils.helper import (
    _load_time, camel_to_snake, decrypt_qianyi_auth,
    generate_qianyi_wms_sign, enforce_string_on_null_columns,
    fillna_column, uppercase_specific_columns
)

# --- Search Utility ---
def search_and_report(df: pd.DataFrame, logger, task_name, search_cfg):
    """Filters the dataframe for keywords and logs the findings."""
    if df.empty or not search_cfg:
        return

    keywords = search_cfg.get("keywords", [])
    target_cols = search_cfg.get("target_columns", ["sku"])
    
    # Filter columns that actually exist in the DF
    valid_cols = [col for col in target_cols if col in df.columns]
    
    for kw in keywords:
        # Case-insensitive search
        mask = df[valid_cols].apply(
            lambda x: x.astype(str).str.contains(kw, case=False, na=False)
        ).any(axis=1)
        
        matches = df[mask]
        if not matches.empty:
            logger.info(f"MATCH FOUND [{kw}] in {task_name}: {len(matches)} rows.")
            # Print a snippet to the terminal
            print(f"\n--- Result for {kw} in {task_name} ---")
            print(matches[valid_cols].to_string(index=False))
        else:
            logger.debug(f"No match for '{kw}' in {task_name}")

async def fetch_one(session, payload_row, logger, ctx, config_params, api_endpoint):
    auth_info_str = payload_row.get("authentication_info") or ""
    wms_type = payload_row.get("wms_type")
    merchant_id = payload_row.get("merchant_id")
    warehouse_code = payload_row.get("warehouse_code")
    try:
        decrypted_json_str = decrypt_qianyi_auth(auth_info_str, wms_type, merchant_id)
        auth_info = json.loads(decrypted_json_str)
        # logger.warning(f" [DEBUG] Decrypted Auth Keys: {list(auth_info.keys())}")
        is_erp = config_params.get("is_erp", False)
        if is_erp:
            app_id = auth_info.get("appId") or auth_info.get("app_id")
            signing_key = auth_info.get("appSecret") or auth_info.get("app_secret") or auth_info.get("partner_key")
            partner_id = None
        else:
            partner_id = auth_info.get("partner_id")
            signing_key = auth_info.get("partner_key")
            app_id = None
    except Exception as error:
        logger.error(f"FATAL: Failed to decrypt auth for {warehouse_code}. Error: {error}")
        return None
    if not signing_key:
        logger.error(f"Missing signing key (partner_key or app_secret) for warehouse {warehouse_code}.")
        return None
    if is_erp and not app_id:
        logger.error(f"SKIPPING {warehouse_code}: Task is ERP but 'app_id' is missing from decrypted auth!")
        return []
    customer_code = payload_row.get("warehouse_owner_code")
    if not is_erp and not customer_code:
        logger.error(f"Missing warehouse_owner_code for {warehouse_code}. Skipping WMS task.")
        return []
    service_type = config_params.get("serviceType")
    page_size = config_params.get("page_size", 200)
    try:
        exec_date_dt = datetime.fromisoformat(ctx.get("execution_date"))
    except ValueError:
        exec_date_dt = datetime.now()
    task_type = ctx.get("task_type", "incremental")
    hours_to_subtract = config_params.get("fullload_hours", 720) if task_type == "fullload" else config_params.get("incremental_hours", 48)
    time_format = config_params.get("time_format", "%Y-%m-%dT%H:%M:%S")
    from_time_dt = exec_date_dt - timedelta(hours=int(hours_to_subtract))
    time_from_str = from_time_dt.strftime(time_format)
    time_to_str = exec_date_dt.strftime(time_format)
    stall_timeout_seconds = config_params.get("api_timeout", 3600) # 1 Hour
    single_request_timeout = 300 # 5 Minutes
    start_time = datetime.now()
    last_success_time = datetime.now()
    current_page = 1
    all_items = []
    MAX_RETRIES = 5
    current_retries = 0
    debug_page_limit = os.environ.get("DEBUG_PAGE_LIMIT")
    response_list_key = config_params.get("response_list_key")
    while True:
        time_since_last_success = (datetime.now() - last_success_time).total_seconds()
        if time_since_last_success > stall_timeout_seconds:
            logger.error(f"FATAL: Pipeline stalled. No new data fetched for {time_since_last_success:.0f}s (Limit: {stall_timeout_seconds}s). Stopping.")
            raise TimeoutError("Pipeline stalled (inactivity timeout).")
        total_elapsed = (datetime.now() - start_time).total_seconds()
        if debug_page_limit and current_page > int(debug_page_limit):
            logger.warning(f"DEBUG: Stopping at page {current_page - 1}")
            break
        biz_data_dict = {
            "page": current_page,
            "pageSize": page_size
        }
        if is_erp:
            if config_params.get("serviceType") == "QUERY_ASN_LIST":
                biz_data_dict["timeType"] = "UPDATE_TIME"
                biz_data_dict["timeFrom"] = time_from_str
                biz_data_dict["timeEnd"] = time_to_str
            elif config_params.get("serviceType") == "QUERY_SALES_ODO_LIST":
                biz_data_dict["createTimeFrom"] = time_from_str
                biz_data_dict["createTimeTo"] = time_to_str
            else:
                biz_data_dict["updateTimeFrom"] = time_from_str
                biz_data_dict["updateTimeTo"] = time_to_str
        else:
            is_snapshot = config_params.get("is_snapshot", False)
            if is_snapshot:
                snapshot_dt = exec_date_dt - timedelta(days=1)
                biz_data_dict["eventDate"] = snapshot_dt.strftime("%Y-%m-%d")
            else:
                time_field_prefix = config_params.get("time_field_prefix", "updated")
                biz_data_dict[f"{time_field_prefix}TimeFrom"] = time_from_str
                biz_data_dict[f"{time_field_prefix}TimeTo"] = time_to_str
            customer_code = payload_row.get("warehouse_owner_code")
            biz_data_dict["customerCode"] = customer_code
            if config_params.get("need_batch_info", False):
                biz_data_dict["ifNeedBatchInfo"] = True
        if config_params.get("include_warehouse_code", False) and warehouse_code:
            wh_key = config_params.get("warehouse_code_key", "warehouseCode")
            biz_data_dict[wh_key] = warehouse_code
        exclude_keys = config_params.get("exclude_keys", [])
        for k in exclude_keys:
            biz_data_dict.pop(k, None)
        biz_data_str = json.dumps(biz_data_dict, separators=(",", ":"))
        query_params = {}
        request_json = biz_data_dict
        if is_erp:
            timestamp_ms = str(int(datetime.now().timestamp() * 1000))
            sign = generate_qianyi_erp_sign(biz_data_str, signing_key)
            query_params = {
                "appId": app_id,
                "serviceType": service_type,
                "sign": sign,
                "timestamp": timestamp_ms,
                "bizParam": biz_data_str
            }
        else:
            sign = generate_qianyi_wms_sign(biz_data_str, signing_key)
            query_params = {
                "serviceType": service_type,
                "partnerId": partner_id,
                "bizData": biz_data_str,
                "sign": sign
            }
        try:
            request_timeout_obj = aiohttp.ClientTimeout(total=single_request_timeout)
            async with session.post(api_endpoint, params=query_params, json=request_json, timeout=request_timeout_obj) as response:
                if response.status != 200:
                    if response.status in [500, 502, 503, 504]:
                        current_retries += 1
                        if current_retries > MAX_RETRIES:
                            msg = f"Max retries ({MAX_RETRIES}) reached for {warehouse_code} on Page {current_page}."
                            if ctx.get("strict_mode", False):
                                logger.error(f"FATAL: {msg} Stopping pipeline (Strict Mode).")
                                raise ProcessError(f"Strict Mode Failed: {msg}")
                            else:
                                logger.error(f"SKIP: {msg} Skipping this warehouse (Batch Mode).")
                                break
                        logger.warning(f"Server Error {response.status} for {warehouse_code} on Page {current_page}. Retry {current_retries}/{MAX_RETRIES} in 5s...")
                        await asyncio.sleep(5)
                        continue
                    err_text = await response.text()
                    logger.error(f"API Error {warehouse_code} (Status: {response.status}): {err_text}")
                    raise ProcessError(f"API returned status {response.status}")
                current_retries = 0
                data = await response.json(content_type=None)
                current_batch_items = []
                if is_erp:
                    if data.get("state") == "success" and "bizContent" in data:
                        try:
                            biz_content = json.loads(data["bizContent"])
                            current_batch_items = biz_content.get("result", [])
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse bizContent JSON for {warehouse_code}")
                    else:
                        logger.error(f"ERP Error Response: {data.get('errorMsg')}")
                else:
                    response_list_key = config_params.get("response_list_key", "itemList")
                    current_batch_items = data.get(response_list_key, [])
                if current_batch_items:
                    for item in current_batch_items:
                        item["owner_code"] = payload_row.get("merchant_owner_code")
                        item["merchant_id"] = merchant_id
                        item["bq_warehouse_code"] = warehouse_code
                    all_items.extend(current_batch_items)
                    last_success_time = datetime.now()
                    total_elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Warehouse {warehouse_code}: Fetched {len(current_batch_items)} items from Page {current_page} (Run time: {int(total_elapsed)}s)")
                    if len(current_batch_items) < page_size:
                        break
                    current_page += 1
                else:
                    if current_page == 1:
                        key_name = response_list_key if response_list_key else "ERP_Result"
                        logger.warning(f"No data found in key '{key_name}' (ERP={is_erp}). API Response keys: {list(data.keys())}")
                    break
        except asyncio.TimeoutError:
            real_elapsed = (datetime.now() - start_time).total_seconds()
            logger.warning(f"TIMEOUT: Page {current_page} for {warehouse_code} took >{single_request_timeout}s. Retrying... (Total elapsed: {int(real_elapsed)}s)")
            await asyncio.sleep(5)
            continue
        except (ClientResponseError, ServerDisconnectedError, ClientOSError) as error:
            logger.warning(f"PROTOCOL ERROR: Connection broke for {warehouse_code} on Page {current_page}. Retrying... Error: {error}")
            await asyncio.sleep(5)
            continue
        except Exception as error:
            logger.error(f"Error processing API call for {warehouse_code} on page {current_page}: {error}")
            raise error
    return all_items

async def main_async_logic(payload_data, logger, ctx, config_params, api_endpoint):
    all_results = []
    timeout_seconds = config_params.get("api_timeout", 1800)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_one(session, row, logger, ctx, config_params, api_endpoint) for row in payload_data]
        results = await asyncio.gather(*tasks)
        for res in results:
            if res: all_results.extend(res) if isinstance(res, list) else all_results.append(res)
    return all_results

# --- Pipeline Definition ---
@pipeline_extract_external_api(retry=2, retry_wait_time=1.5)
def pipeline_search(): ...

@pipeline_search.extract()
def extract_search(**ctx: Dict) -> List:
    logger = ctx.get("log")
    payload_data = get_payload_data_from_bq(ctx=ctx, logger=logger)
    if not payload_data: return []
    
    base_url = ctx.get("host")
    api_endpoint = f"{base_url}{ctx.get('endpoint')}"
    config_params = ctx.get("params", {})
    
    return asyncio.run(main_async_logic(payload_data, logger, ctx, config_params, api_endpoint))

@pipeline_search.transform()
def transform_search(extract_result: List, **ctx: Dict) -> pd.DataFrame:
    if not extract_result: return pd.DataFrame()
    df = pd.json_normalize(extract_result, sep="_")
    df.columns = [camel_to_snake(col) for col in df.columns]
    return df

@pipeline_search.load()
def load_search(transform_result: pd.DataFrame, **ctx: Dict):
    logger = ctx.get("log")
    # Retrieve keywords from config
    search_cfg = ctx.get("params", {}).get("search_settings", {})
    search_and_report(transform_result, logger, ctx.get("task_name"), search_cfg)

# --- CLI Command ---
@click.command()
@click.option('--task-name', required=True)
@click.option('--task-type', required=True)
@click.option('--execution-date', required=True)
@click.option('--log-level', default="INFO")
@click.option('--owner-code', required=False)
@click.option('--id', required=False)
@click.option('--config-dir', default='etc/conf')
@click.option('--config-timeout', default=10)
def main(task_name, task_type, execution_date, log_level, owner_code, id, config_dir, config_timeout):
    # Standard setup
    temp_logger = Logger(task_name, level=log_level)
    all_configs = parse_config(temp_logger, config_timeout, config_dir=config_dir)
    task_cfg = all_configs["tasks"][task_name]
    
    # Get target list from BigQuery
    bq_client = get_bq_client(task_cfg["gcp_project_id"], temp_logger)
    targets = fetch_qianyi_targets(bq_client, task_cfg["gcp_project_id"], task_cfg["merchant_table_id"], temp_logger, owner_code, id)

    for target in targets:
        current_ctx = {
            **task_cfg,
            "owner_code": target["owner_code"],
            "id": target["id"],
            "task_type": task_type,
            "execution_date": execution_date,
            "log": Logger(task_name, level=log_level)
        }
        pipeline_search.add_ctx(current_ctx)
        pipeline_search()

if __name__ == "__main__":
    main()