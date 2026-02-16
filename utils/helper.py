# Import Libraries
from botocore.exceptions import BotoCoreError, ClientError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from datetime import datetime, timezone
from sdk.utils.awswrangler_dataaccess import save_pandas_to_parquet
import binascii
import hashlib
import logging
import pandas as pd
import re

# Initialization
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
RANDOM_HASH = 43258201

def _load_time(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a UTC timestamp column '_load_time' to the DataFrame."""
    df["_load_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return df

def auto_convert_string_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts specified STRING columns to datetime objects.

    Logic:
    1. TARGET_COLS: Only processes hardcoded columns.
    2. CLEANING: Removes " UTC" substring to allow Pandas to parse the "+7" offset correctly.
    3. TIMEZONE: Converts everything to UTC and makes it timezone-naive (required for BigQuery).
    """
    TARGET_COLS = {
        "order_time",
    }
    for col in df.columns:
        if col not in TARGET_COLS:
            continue
        try:
            mask_is_string = df[col].apply(lambda x: isinstance(x, str))
            if not mask_is_string.any():
                continue
            raw_strings = df.loc[mask_is_string, col].astype(str)
            cleaned_series = raw_strings.str.replace(" UTC", "", regex=False).str.strip()
            converted_values = pd.to_datetime(cleaned_series, errors='coerce')
            if pd.api.types.is_datetime64tz_dtype(converted_values):
                converted_values = converted_values.dt.tz_convert("UTC").dt.tz_localize(None)
            df.loc[mask_is_string, col] = converted_values
        except Exception as error:
            print(f"ERROR converting string date {col}: {error}")
            pass
    return df

def auto_convert_unix_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts specified columns from Unix timestamps (milliseconds) to datetime objects.

    This function enforces a strict schema by:
    1. Processing only the columns explicitly listed in TARGET_COLS.
    2. Filtering out overflow values (e.g., > Year 11,000 AD) to prevent crash errors.
    3. Standardizing all conversions to Milliseconds (unit='ms').
    """
    TARGET_COLS = {
        "audit_time",
        "create_time",
        "latest_ship_date",
        "order_time",
        "pay_time",
        "platform_shipping_time",
        "shipping_time",
        "system_order_shipped_time",
        "update_time",
    }
    SAFE_MAX_MS = 3e14
    for col in df.columns:
        if col not in TARGET_COLS:
            continue
        try:
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            is_number = numeric_series.notna()
            if not is_number.any():
                continue
            mask_too_large = numeric_series > SAFE_MAX_MS
            if mask_too_large.any():
                numeric_series.loc[mask_too_large] = pd.NA
                is_number = numeric_series.notna()
            if not pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype('object')
            df.loc[is_number, col] = pd.to_datetime(numeric_series[is_number], unit='ms', errors='coerce')
        except Exception as error:
            print(f"ERROR converting unix {col}: {error}")
            pass
    return df

def camel_to_snake(name: str) -> str:
    """
    Converts camelCase or PascalCase to snake_case, handling acronyms correctly.

    Examples:
    - 'packagingList' -> 'packaging_list'
    - 'retailSKU'     -> 'retail_sku'
    - 'VolumeEA'      -> 'volume_ea'
    """
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def decrypt_qianyi_auth(ciphertext: str, wms_type: str, merchant_id: int) -> str:
    """
    Decrypts the authentication_info string using AES-GCM and PBKDF2.
    """
    try:
        passphrase_base = f"{wms_type}-{merchant_id}"
        final_passphrase = f"{passphrase_base}{RANDOM_HASH}"
        parts = ciphertext.split(".")
        if len(parts) < 3:
            raise ValueError("Invalid ciphertext format. Expected 'salt.iv.data'")
        salt = binascii.unhexlify(parts[0])
        iv = binascii.unhexlify(parts[1])
        encrypted_data = binascii.unhexlify(parts[2])
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=1000,
            backend=default_backend()
        )
        key = kdf.derive(final_passphrase.encode('utf-8'))
        aesgcm = AESGCM(key)
        plaintext_bytes = aesgcm.decrypt(iv, encrypted_data, None)
        return plaintext_bytes.decode('utf-8')
    except Exception as error:
        raise ValueError(f"Decryption failed: {str(error)}")

def enforce_string_on_null_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifies columns that are entirely null and forces them to the explicit
    string dtype. This ensures Parquet writers treat them as UTF-8 strings
    instead of inferring float64 from np.nan.
    """
    for col in df.columns:
        if df[col].isna().all():
            df[col] = df[col].astype("string")
    return df

def fillna_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills NaN values in a DataFrame with appropriate defaults based on dtype.
    - 0 for numeric types (int, float)
    - False for boolean types
    """
    for col in df.columns:
        val = df[col].dtypes
        if val == 'bool' or pd.api.types.is_bool_dtype(val):
            df[col] = df[col].fillna(False)
        elif val == 'float' or val == 'int' or val == 'double' or pd.api.types.is_numeric_dtype(val):
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)
    return df

def generate_qianyi_erp_sign(biz_param_str: str, partner_key: str) -> str:
    """Generates the MD5 signature for QianYi ERP API."""
    raw_sign = biz_param_str + partner_key
    return hashlib.md5(raw_sign.encode('utf-8')).hexdigest()

def generate_qianyi_wms_sign(biz_data_str: str, partner_key: str) -> str:
    """Generates the MD5 signature for QianYi WMS API."""
    raw_sign = biz_data_str + partner_key
    return hashlib.md5(raw_sign.encode('utf-8')).hexdigest()

def save_to_s3_with_partition(
        df: pd.DataFrame,
        bucket: str,
        app_name: str,
        service_name: str,
        partition_datetime: datetime,
        filename: str = "",
    ) -> str:
    """
    Saves a Pandas DataFrame to a partitioned S3 path in Parquet format.

    This function constructs a standard S3 path using date and time partitioning
    and handles the save operation, including validation and error handling.

    Args:
        df: The Pandas DataFrame to save.
        bucket: The target S3 bucket name.
        app_name: The application or domain name for the S3 path (e.g., 'qianyi/api/account_name').
        service_name: The specific service or data source (e.g., 'sku', 'inbound').
        partition_datetime: The datetime object used for partitioning the data.
        filename: An optional specific filename. If not provided, it's generated
                  from the partition_datetime.

    Returns:
        The full S3 path where the file was saved.

    Raises:
        TypeError: If the input 'df' is not a DataFrame or 'partition_datetime'
                   is not a datetime object.
        ValueError: If the input DataFrame is empty or required path components are missing.
        IOError: If the file fails to save to S3 due to AWS or network errors.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input 'df' must be a pandas DataFrame.")
    if not isinstance(partition_datetime, datetime):
        raise TypeError("Input 'partition_datetime' must be a datetime.datetime object.")
    if not all([bucket, app_name, service_name]):
        raise ValueError("'bucket', 'app_name', and 'service_name' must be non-empty strings.")
    if df.empty:
        logger.warning(f"The input DataFrame for {service_name} is empty. No file will be saved to S3.")
        return f"No file saved, DataFrame was empty for {service_name}."
    if not filename:
        output_file = partition_datetime.strftime('%Y%m%d-%H%M%S')
    else:
        output_file = filename.split('.')[0]
    partition_path = partition_datetime.strftime('%Y/%m/%d/%H')
    s3_path = f"s3://{bucket}/{app_name}/{service_name}/{partition_path}/{output_file}.parquet"
    logger.info(f"Attempting to save DataFrame to S3 path: {s3_path}")
    try:
        save_pandas_to_parquet(df, s3_path)
        logger.info(f"Successfully saved file to {s3_path}")
        return s3_path
    except (BotoCoreError, ClientError) as error:
        logger.error(f"AWS Error: Failed to save file to {s3_path}. Reason: {error}")
        raise IOError(f"Could not write to S3 path: {s3_path}") from error
    except Exception as error:
        logger.error(f"An unexpected error occurred while saving to {s3_path}. Reason: {error}")
        raise IOError(f"An unexpected error occurred while writing to S3: {s3_path}") from error

def uppercase_specific_columns(df: pd.DataFrame, target_columns: list = None) -> pd.DataFrame:
    """
    Forces specified columns to uppercase if they exist in the DataFrame.
    Default targets: ['sku']
    """
    if target_columns is None:
        target_columns = ["sku"]
    for col in target_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).upper() if pd.notna(x) else x)
    return df