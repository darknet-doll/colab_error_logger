# colab_error_logger/airtable_sync.py
import os
import pandas as pd
from pyairtable import Table

def upload_to_airtable(
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

    # Write to airtable_error_df by iterating through each row from session_log_df
    # (excluding id column).
    for _, row in df.iloc[:, 1:].iterrows():
        try:
            table.create({
                "session_name": row["session_name"],
                "error_type": row["error_type"],
                "date": row["date"]
            })
        except Exception as e:
            print(f"⚠️ Failed to upload record: {e}")

    print("✅ Upload to Airtable completed.")
