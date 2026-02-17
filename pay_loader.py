import argparse
from datetime import datetime
from functools import singledispatch
from typing import List, Dict, Any

import pandas as pd


# ==========================================================
# FUNCTION OVERLOADING (Simulated using singledispatch)
# ==========================================================

@singledispatch
def prepare_dataframe(input_data):
    """
    Default handler if unsupported type is passed.
    """
    raise TypeError("Unsupported input type for prepare_dataframe")


@prepare_dataframe.register
def _(input_data: str) -> pd.DataFrame:
    """
    If input is a string, treat it as a file path
    and read CSV into DataFrame.
    """
    print(f"Reading data from file: {input_data}")
    return pd.read_csv(input_data)


@prepare_dataframe.register
def _(input_data: list) -> pd.DataFrame:
    """
    If input is a list (manual data),
    convert it into a DataFrame.
    """
    print("Reading data from manual input list")
    return pd.DataFrame(input_data)


# ==========================================================
# DATA PROCESSING
# ==========================================================

def transform_data(df: pd.DataFrame, payer: str) -> pd.DataFrame:
    """
    Apply transformations:
    - Add ingestion_timestamp
    - Apply payer-specific logic
    """
    df = df.copy()

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now()

    # Payer-specific logic
    if payer.lower() == "anthem":
        # Example: Add 10% processing fee
        df["claim_amount"] = df["claim_amount"] * 1.10

    elif payer.lower() == "cigna":
        # Example: Apply 5% discount
        df["claim_amount"] = df["claim_amount"] * 0.95

    return df


# ==========================================================
# OOP SECTION
# ==========================================================

class BaseLoader:
    """
    Base class for loading data.
    """

    def load(self, df: pd.DataFrame):
        print("Loading data...")


class PayerLoader(BaseLoader):
    """
    Child class that overrides load method.
    """

    def __init__(self, payer: str):
        self.payer = payer.lower()

    def load(self, df: pd.DataFrame):
        """
        Override load method to print
        specific Snowflake table.
        """

        if self.payer == "anthem":
            table_name = "SNOWFLAKE.RAW.ANTHEM_TABLE"
        elif self.payer == "cigna":
            table_name = "SNOWFLAKE.RAW.CIGNA_TABLE"
        else:
            table_name = "SNOWFLAKE.RAW.GENERIC_CLAIMS"

        print(f"Loading data into {table_name}")
        print(f"Total rows: {len(df)}")


# ==========================================================
# MAIN FUNCTION
# ==========================================================

def main():
    parser = argparse.ArgumentParser(
        description="Multi-Source Payer Loader Utility"
    )

    parser.add_argument(
        "--source",
        type=str,
        required=False,
        help="Path to CSV file"
    )

    parser.add_argument(
        "--payer",
        type=str,
        required=True,
        choices=["anthem", "cigna", "manual"],
        help="Name of the payer"
    )

    args = parser.parse_args()

    payer = args.payer

    # ===============================
    # Decide input source
    # ===============================

    if payer == "manual":
        # Simulated manual data
        manual_data: List[Dict[str, Any]] = [
            {
                "member_id": 1,
                "claim_id": 1001,
                "claim_amount": 500,
                "service_date": "2024-01-10",
                "payer_name": "manual"
            },
            {
                "member_id": 2,
                "claim_id": 1002,
                "claim_amount": 800,
                "service_date": "2024-02-15",
                "payer_name": "manual"
            }
        ]

        df = prepare_dataframe(manual_data)

    else:
        if not args.source:
            raise ValueError("Source file path is required for file-based loading")

        df = prepare_dataframe(args.source)

    # ===============================
    # Transform data
    # ===============================

    df = transform_data(df, payer)

    # ===============================
    # Load data using OOP
    # ===============================

    loader = PayerLoader(payer)
    loader.load(df)


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    main()
