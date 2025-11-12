# colab_error_logger/airtable_sync.py
import os
import pandas as pd
from pyairtable import Table

def write_airtable(
    df,
    airtable_token: str = None,
    base_id: str = None,
    table_name: str = "Errors"
):
    """
    Upload a pandas DataFrame of errors to Airtable.
    Args:
        df (pd.DataFrame): DataFrame containing 'session_name', 'error_type', 'date'
        airtable_token (str, optional): Airtable API token (uses env if None)
        base_id (str, optional): Airtable base ID (uses env if None)
        table_name (str, optional): Airtable table name
    """
    airtable_token = airtable_token or os.getenv("AIRTABLE_TOKEN")
    base_id = base_id or os.getenv("AIRTABLE_BASE_ID")

    if not (airtable_token and base_id):
        raise ValueError("Missing Airtable credentials. Set AIRTABLE_TOKEN and AIRTABLE_BASE_ID.")

    # Connecting to airtable dataframe.
    table = Table(airtable_token, base_id, table_name)
   
    # Iterate and write each record
    for _, row in df.iterrows():
        record_data = {
            "session_type": row.get("session_type"),
            "session_name": row.get("session_name"),
            "error_type": row.get("error_type"),
            "date": row.get("date"),
        }
        try:
            table.create(record_data)
        except Exception as e:
            print(f"⚠️ Failed to upload record for session '{record_data.get('session_name')}' "
                  f"({record_data.get('error_type')}, {record_data.get('date')}): {e}")

    print("✅ Upload to Airtable completed.")

def read_airtable(
    airtable_token: str = None,
    base_id: str = None,
    table_name: str = "Errors"
) -> pd.DataFrame:
    """
    Read an Airtable table into a pandas DataFrame.

    Args:
        airtable_token (str, optional): Airtable API token (uses env if None)
        base_id (str, optional): Airtable base ID (uses env if None)
        table_name (str, optional): Airtable table name

    Returns:
        pd.DataFrame: A DataFrame containing all records from Airtable.
    """
    airtable_token = airtable_token or os.getenv("AIRTABLE_TOKEN")
    base_id = base_id or os.getenv("AIRTABLE_BASE_ID")

    if not (airtable_token and base_id):
        raise ValueError("Missing Airtable credentials. Set AIRTABLE_TOKEN and AIRTABLE_BASE_ID.")
    
    # Connecting to airtable dataframe.
    table = Table(airtable_token, base_id, table_name)
    records = table.all()

    if not records:
        print("⚠️ No records found in Airtable table.")
        return pd.DataFrame()

    # Flatten Airtable record fields into a DataFrame
    df = pd.DataFrame([r["fields"] for r in records])

    print(f"✅ Retrieved {len(df)} records from Airtable.")
    return df
