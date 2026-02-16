# Import Libraries
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Any, Dict, List
import base64
import binascii
import json
import logging
import os

def get_bq_client(project_id: str, logger: logging.Logger) -> bigquery.Client:
    """
    Creates a BigQuery client using the GCP_SERVICE_ACCOUNT env var if available,
    otherwise falls back to Application Default Credentials (ADC).
    """
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT")
        if creds_str:
            logger.info("Found GCP_SERVICE_ACCOUNT env var. Attempting authentication.")
            service_account_info = None
            try:
                decoded_bytes = base64.b64decode(creds_str)
                decoded_str = decoded_bytes.decode('utf-8')
                service_account_info = json.loads(decoded_str)
                logger.info("Successfully decoded Base64 GCP credentials.")
            except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, ValueError):
                pass
            if service_account_info is None:
                try:
                    service_account_info = json.loads(creds_str)
                    logger.info("Successfully parsed raw JSON GCP credentials.")
                except json.JSONDecodeError:
                    logger.error("Failed to decode GCP_SERVICE_ACCOUNT. It is neither valid Base64 nor valid raw JSON.")
                    raise ValueError("Invalid GCP_SERVICE_ACCOUNT JSON format.")
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            return bigquery.Client(project=project_id, credentials=credentials)
        else:
            logger.warning("No GCP_SERVICE_ACCOUNT env var found. Attempting default ADC.")
            return bigquery.Client(project=project_id)
    except Exception as error:
        logger.error(f"Failed to initialize BigQuery client: {error}")
        raise

def fetch_qianyi_targets(client: bigquery.Client, project_id: str, table_id: str, logger: logging.Logger, owner_code: str = None, merchant_id: str = None) -> List[Dict[str, Any]]:
    """
    Fetches Qianyi targets.
    - If no filters: Fetches ALL (Batch Mode).
    - If owner_code provided: Fetches all IDs for that code.
    - If merchant_id provided: Fetches the specific code for that ID.
    """
    query = f"""
        SELECT DISTINCT honeywell_code as owner_code, CAST(merchant_id AS STRING) as id
        FROM `{project_id}.{table_id}`
        WHERE wms_type = "Qianyi"
          AND honeywell_code IS NOT NULL
          AND merchant_id IS NOT NULL
    """
    query_params = []
    if owner_code:
        query += " AND honeywell_code = @owner_code"
        query_params.append(bigquery.ScalarQueryParameter("owner_code", "STRING", owner_code))
        logger.info(f"Filtering targets by Owner Code: {owner_code}")
    if merchant_id:
        query += " AND merchant_id = @merchant_id"
        query_params.append(bigquery.ScalarQueryParameter("merchant_id", "INT64", int(merchant_id)))
        logger.info(f"Filtering targets by Merchant ID: {merchant_id}")
    if not owner_code and not merchant_id:
        limit_val = os.environ.get("DEBUG_CODE_LIMIT")
        if limit_val:
            query += f" LIMIT {limit_val}"
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    try:
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        if not results:
            logger.warning(f"No targets found for provided filters (Code={owner_code}, ID={merchant_id}).")
            return []
        targets = [dict(row.items()) for row in results]
        logger.info(f"Found {len(targets)} target(s) to process.")
        return targets
    except Exception as error:
        logger.error(f"Failed to fetch targets: {error}")
        raise

def _fetch_merchant_info(client: bigquery.Client, project_id: str, table_id: str, owner_code: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Helper function to fetch merchant info based on owner_code.
    """
    query = f"""
        SELECT merchant_id, honeywell_code as owner_code, wms_type, authentication_info
        FROM `{project_id}.{table_id}`
        WHERE wms_type = "Qianyi" AND honeywell_code = @owner_code
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("owner_code", "STRING", owner_code),
        ]
    )
    logger.info(f"Executing merchant query for owner_code: {owner_code}")
    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())
    if not results:
        logger.warning(f"No merchant found in '{table_id}' for owner_code: {owner_code}")
        return {}
    if len(results) > 1:
        logger.warning(f"Found {len(results)} merchants for {owner_code}. Using the first result.")
    return dict(results[0].items())

def _fetch_warehouse_info(client: bigquery.Client, project_id: str, table_id: str, merchant_id: int, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Helper function to fetch warehouse info based on merchant_id.
    """
    query = f"""
        SELECT merchant_id, owner_code, code
        FROM `{project_id}.{table_id}`
        WHERE merchant_id = @merchant_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("merchant_id", "INT64", merchant_id),
        ]
    )
    logger.info(f"Executing warehouse query for merchant_id: {merchant_id}")
    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())
    if results:
        logger.info(f"DEBUG: Found {len(results)} warehouses for merchant_id {merchant_id}:")
        for row in results:
            logger.info(f" - Warehouse Code: {row.get('code')} | Owner: {row.get('owner_code')}")
    else:
        logger.warning(f"DEBUG: No warehouses found in '{table_id}' for merchant_id: {merchant_id}")
    if not results:
        logger.warning(f"No warehouses found in '{table_id}' for merchant_id: {merchant_id}")
        return []
    return [dict(row.items()) for row in results]

def _join_merchant_and_warehouse(merchant_data: Dict[str, Any], warehouse_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Joins the merchant data with the list of warehouses to create the final API payloads.
    """
    joined_payloads = []
    for warehouse in warehouse_list:
        payload = {
            "merchant_id": merchant_data.get("merchant_id"),
            "merchant_owner_code": merchant_data.get("owner_code"),
            "wms_type": merchant_data.get("wms_type"),
            "authentication_info": merchant_data.get("authentication_info"),
            "warehouse_owner_code": warehouse.get("owner_code"),
            "warehouse_code": warehouse.get("code")
        }
        joined_payloads.append(payload)
    return joined_payloads

def get_payload_data_from_bq(ctx: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Main orchestrator to fetch and join data from BigQuery.
    """
    owner_code = ctx.get("owner_code")
    gcp_project = ctx.get("gcp_project_id")
    merchant_table_id = ctx.get("merchant_table_id")
    warehouse_table_id = ctx.get("warehouse_table_id")
    if not all([gcp_project, merchant_table_id, warehouse_table_id, owner_code]):
        logger.error("Missing required BQ config/parameters.")
        raise ValueError("Missing required BQ configuration.")
    logger.info(f"Connecting to BigQuery in project '{gcp_project}' for owner_code '{owner_code}'")
    try:
        client = get_bq_client(gcp_project, logger)
        merchant_data = _fetch_merchant_info(client, gcp_project, merchant_table_id, owner_code, logger)
        if not merchant_data:
            return []
        merchant_id = merchant_data.get("merchant_id")
        if not merchant_id:
            logger.error("Merchant ID is missing from merchant data.")
            return []
        warehouse_list = _fetch_warehouse_info(client, gcp_project, warehouse_table_id, int(merchant_id), logger)
        if not warehouse_list:
            return []
        wh_limit = os.environ.get("DEBUG_WAREHOUSE_LIMIT")
        if wh_limit and warehouse_list:
            limit_int = int(wh_limit)
            logger.warning(f"DEBUG MODE: Limiting warehouses for {owner_code} to {limit_int} (Total available: {len(warehouse_list)})")
            warehouse_list = warehouse_list[:limit_int]
        final_payload_list = _join_merchant_and_warehouse(merchant_data, warehouse_list)
        logger.info(f"Successfully generated {len(final_payload_list)} payloads for merchant_id {merchant_id}.")
        return final_payload_list
    except GoogleAPICallError as error:
        logger.error(f"BigQuery API error: {error}")
        raise
    except Exception as error:
        logger.error(f"An unexpected error occurred while querying BigQuery: {error}")
        raise