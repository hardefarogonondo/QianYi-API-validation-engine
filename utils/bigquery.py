from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Any, Dict, List
import json
import os
import logging

def get_bq_client(project_id: str, logger: logging.Logger) -> bigquery.Client:
    """Creates a BQ client using GCP_SERVICE_ACCOUNT (JSON string) or ADC."""
    creds_str = os.environ.get("GCP_SERVICE_ACCOUNT")
    if creds_str:
        try:
            service_account_info = json.loads(creds_str)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            return bigquery.Client(project=project_id, credentials=credentials)
        except Exception as e:
            logger.error(f"Failed parsing GCP_SERVICE_ACCOUNT: {e}")
    return bigquery.Client(project=project_id)

def fetch_qianyi_targets(client: bigquery.Client, project_id: str, table_id: str, logger: logging.Logger, owner_code: str = None, merchant_id: str = None) -> List[Dict[str, Any]]:
    query = f"""
        SELECT DISTINCT honeywell_code as owner_code, CAST(merchant_id AS STRING) as id
        FROM `{project_id}.{table_id}`
        WHERE wms_type = "Qianyi" AND honeywell_code IS NOT NULL AND merchant_id IS NOT NULL
    """
    params = []
    if owner_code:
        query += " AND honeywell_code = @owner_code"
        params.append(bigquery.ScalarQueryParameter("owner_code", "STRING", owner_code))
    if merchant_id:
        query += " AND merchant_id = @merchant_id"
        params.append(bigquery.ScalarQueryParameter("merchant_id", "INT64", int(merchant_id)))
        
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    results = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in results]

def get_payload_data_from_bq(ctx: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
    owner_code = ctx.get("owner_code")
    gcp_project = ctx.get("gcp_project_id")
    m_table = ctx.get("merchant_table_id")
    w_table = ctx.get("warehouse_table_id")

    client = get_bq_client(gcp_project, logger)
    
    # Fetch Merchant Info
    m_query = f"SELECT merchant_id, honeywell_code, authentication_info, wms_type FROM `{gcp_project}.{m_table}` WHERE honeywell_code = @owner_code"
    m_res = list(client.query(m_query, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("owner_code", "STRING", owner_code)])).result())
    
    if not m_res: return []
    merchant = m_res[0]

    # Fetch Warehouses
    wh_query = f"SELECT owner_code, code FROM `{gcp_project}.{w_table}` WHERE merchant_id = @mid"
    wh_res = client.query(wh_query, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("mid", "INT64", merchant.merchant_id)])).result()
    
    return [{
        "merchant_id": merchant.merchant_id,
        "wms_type": merchant.wms_type,
        "authentication_info": merchant.authentication_info,
        "warehouse_owner_code": wh.owner_code,
        "warehouse_code": wh.code
    } for wh in wh_res]