import pandas as pd 
import os 
import logging 

logger = logging 

def generate_brm_branches_csv(
    master_sheet_path: str,
    save_path: str,
):
    logger.debug(
        "generating brm branches csv"
    )

    raw_data = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory = False 
    )

    unique_branches = list(raw_data.dropna(subset=["Branch"])["Branch"].unique())

    brm_branch_csv_columns = [
        "ID", "Name"
    ]

    brm_branch_csv = pd.DataFrame(
        columns = brm_branch_csv_columns
    )

    count = 0 
    for branch in unique_branches:
        row_id = "-".join(
            [i.lower() for i in branch.split(" ")]
        )

        brm_branch_csv.loc[count] = [
            row_id,
            branch
        ]

        count += 1 
    
    brm_branch_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-brm-branches-csv.csv"
        ),
        encoding = "utf-8",
        index=False
    )

    logger.debug(
        "brm branches generated"
    )
