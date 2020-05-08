import pandas as pd
import logging 
import os

logger = logging 


def generate_jobs_csv(
    master_sheet_path: str,
    save_path: str
):
    logger.debug("generating jobs csv")
    raw_data = pd.read_csv(
        master_sheet_path,
        low_memory=False,
        encoding="utf-8"
    )

    def createId(data):
        filtered_chars = ".?&/\\,"
        name_array = data.split(" ")
        name_array = [ 
            "".join([j for j in i if j not in filtered_chars]).lower() for i in name_array
        ]
        return "-".join(name_array)
    
    # drop nan's
    title_series = raw_data.dropna(subset=["Title"])
    title_series = title_series.drop_duplicates(subset = ["Title"])

    # generate csv
    job_csv = pd.DataFrame(
        columns = ["ID", "Job Position"]
    ) 

    count = 0
    for i in list(title_series["Title"]):
        job_id = createId(i)
        job_csv.loc[count] = [
            job_id,
            i
        ]
        count += 1

    job_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-jobs-csv.csv"
        ),
        encoding="utf-8",
        index = False
    )

    logger.debug("jobs csv generated")






