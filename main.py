import asyncio
import click
import json
import os
import pandas as pd
import aiohttp
import boto3
from datetime import datetime, timedelta
from typing import Dict, List
from aiohttp import ClientOSError, ClientResponseError, ServerDisconnectedError

# SDK and Utils imports
from sdk.extract_external_api import pipeline_extract_external_api
from sdk.utils.config import parse_config
from sdk.utils import ArgumentError, Logger, ProcessError

from utils.bigquery import get_bq_client, fetch_qianyi_targets, get_payload_data_from_bq
from utils.helper import (
    _load_time, camel_to_snake, decrypt_qianyi_auth,
    generate_qianyi_wms_sign, generate_qianyi_erp_sign,
    enforce_string_on_null_columns, fillna_column, 
    uppercase_specific_columns
)

# --- Search Utility ---
def search_and_report(df: pd.DataFrame, logger, task_name, search_cfg):
    if df.empty or not search_cfg:
        return

    keywords = search_cfg.get("keywords", [])
    target_cols = search_cfg.get("target_columns", ["sku"])
    valid_cols = [col for col in target_cols if col in df.columns]
    
    for kw in keywords:
        mask = df[valid_cols].apply(
            lambda x: x.astype(str).str.contains(kw, case=False, na=False)
        ).any(axis=1)
        
        matches = df[mask]
        if not matches.empty:
            logger.info(f"🎯 MATCH FOUND [{kw}] in {task_name}: {len(matches)} rows.")
            print(f"\n--- Search Results for '{kw}' ---")
            print(matches.to_markdown(index=False))
        else:
            logger.info(f"No matches found for keyword: {kw}")

# --- API Logic (Exact Replica of ETL) ---
async def fetch_one(session, payload_row, logger, ctx, config_params, api_endpoint):
    auth_info_str = payload_row.get("authentication_info") or ""
    wms_type = payload_row.get("wms_type")
    merchant_id = payload_row.get("merchant_id")
    warehouse_code = payload_row.get("warehouse_code")
    
    try:
        decrypted_json_str = decrypt_qianyi_auth(auth_info_str, wms_type, merchant_id)
        auth_info = json.loads(decrypted_json_str)
        is_erp = config_params.get("is_erp", False)
        
        if is_erp:
            signing_key = auth_info.get("appSecret") or auth_info.get("app_secret") or auth_info.get("partner_key")
            app_id = auth_info.get("appId") or auth_info.get("app_id")
            partner_id = None
        else:
            partner_id = auth_info.get("partner_id")
            signing_key = auth_info.get("partner_key")
            app_id = None
    except Exception as error:
        logger.error(f"Failed to decrypt auth: {error}")
        return None

    service_type = config_params.get("serviceType")
    page_size = config_params.get("page_size", 200)
    exec_date_dt = datetime.fromisoformat(ctx.get("execution_date"))
    
    # Time Window Calculation
    task_type = ctx.get("task_type", "incremental")
    hours = config_params.get("fullload_hours", 720) if task_type == "fullload" else config_params.get("incremental_hours", 48)
    from_time_str = (exec_date_dt - timedelta(hours=int(hours))).strftime(config_params.get("time_format", "%Y-%m-%dT%H:%M:%S"))
    to_time_str = exec_date_dt.strftime(config_params.get("time_format", "%Y-%m-%dT%H:%M:%S"))

    current_page = 1
    all_items = []
    
    while True:
        biz_data_dict = {"page": current_page, "pageSize": page_size}
        if is_erp:
            biz_data_dict["updateTimeFrom"] = from_time_str
            biz_data_dict["updateTimeTo"] = to_time_str
        else:
            biz_data_dict["updatedTimeFrom"] = from_time_str
            biz_data_dict["updatedTimeTo"] = to_time_str
            biz_data_dict["customerCode"] = payload_row.get("warehouse_owner_code")

        biz_data_str = json.dumps(biz_data_dict, separators=(",", ":"))
        sign = generate_qianyi_erp_sign(biz_data_str, signing_key) if is_erp else generate_qianyi_wms_sign(biz_data_str, signing_key)
        
        params = {"serviceType": service_type, "sign": sign}
        if is_erp: params.update({"appId": app_id, "bizParam": biz_data_str})
        else: params.update({"partnerId": partner_id, "bizData": biz_data_str})

        async with session.post(api_endpoint, params=params, json=biz_data_dict) as response:
            if response.status != 200: break
            data = await response.json(content_type=None)
            
            items = []
            if is_erp and data.get("state") == "success":
                items = json.loads(data.get("bizContent", "{}")).get("result", [])
            else:
                items = data.get(config_params.get("response_list_key", "itemList"), [])
            
            if not items: break
            all_items.extend(items)
            if len(items) < page_size: break
            current_page += 1
            
    return all_items

async def main_async_logic(payload_data, logger, ctx, config_params, api_endpoint):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, row, logger, ctx, config_params, api_endpoint) for row in payload_data]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results if sublist for item in sublist]

# --- Pipeline Wrappers ---
@pipeline_extract_external_api(retry=2, retry_wait_time=1.5)
def pipeline_search(): ...

@pipeline_search.extract()
def extract_search(**ctx: Dict) -> List:
    logger = ctx.get("log")
    payload_data = get_payload_data_from_bq(ctx=ctx, logger=logger)
    if not payload_data: return []
    
    config_params = ctx.get("params", {})
    api_endpoint = f"{ctx.get('host')}{ctx.get('endpoint')}"
    return asyncio.run(main_async_logic(payload_data, logger, ctx, config_params, api_endpoint))

@pipeline_search.transform()
def transform_search(extract_result: List, **ctx: Dict) -> pd.DataFrame:
    if not extract_result: return pd.DataFrame()
    df = pd.json_normalize(extract_result, sep="_")
    df.columns = [camel_to_snake(col) for col in df.columns]
    return df

@pipeline_search.load()
def load_search(transform_result: pd.DataFrame, **ctx: Dict):
    search_cfg = ctx.get("params", {}).get("search_settings", {})
    search_and_report(transform_result, ctx.get("log"), ctx.get("task_name"), search_cfg)

# --- CLI Implementation ---
@click.command()
@click.option('--task-name', required=True)
@click.option('--task-type', type=click.Choice(['fullload', 'incremental']), required=True)
@click.option('--execution-date', required=True)
@click.option('--log-level', default="INFO")
@click.option('--aws-profile', help="AWS profile name.")
@click.option('--owner-code')
@click.option('--id')
@click.option('--config-dir', default='conf')
@click.option('--config-timeout', default=10)
def main(task_name, task_type, execution_date, log_level, aws_profile, owner_code, id, config_dir, config_timeout):
    if aws_profile:
        boto3.setup_default_session(profile_name=aws_profile)
        print(f"INFO: Using AWS profile: '{aws_profile}'")

    temp_logger = Logger(task_name, level=log_level)
    all_configs = parse_config(temp_logger, config_timeout, config_dir=config_dir)
    task_cfg = all_configs["tasks"][task_name]
    
    bq_client = get_bq_client(task_cfg["gcp_project_id"], temp_logger)
    targets = fetch_qianyi_targets(bq_client, task_cfg["gcp_project_id"], task_cfg["merchant_table_id"], temp_logger, owner_code, id)

    for target in targets:
        full_ctx = {
            **task_cfg,
            "task_name": task_name,
            "task_type": task_type,
            "execution_date": execution_date,
            "owner_code": target["owner_code"],
            "id": target["id"],
            "log": Logger(task_name, level=log_level)
        }
        pipeline_search.add_ctx(full_ctx)
        pipeline_search()

if __name__ == "__main__":
    main()