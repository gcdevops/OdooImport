import pandas as pd
import os
import logging 

logger = logging 



def generate_regions_csv(
    master_sheet_path: str,
    save_path: str
):
    logger.debug("genrating regions csv")
    raw_data = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory = False 
    )

    # get unique regions 

    unique_regions = list(raw_data.dropna(subset=["Region"])["Region"].unique())

    region_csv = pd.DataFrame(columns=[
        "ID", "Name"
    ])

    count = 0

    for i in unique_regions:
        try:
            int(i)
        except:
            if i.strip() != "":
                row_id = "-".join([i.lower() for i in i.split(" ")])
                region_csv.loc[count] = [
                    row_id,
                    i
                ]
                count += 1


    region_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-regions-csv.csv"
        ),
        encoding = "utf-8",
        index=False
    )

    logger.debug("regions csv generated")
