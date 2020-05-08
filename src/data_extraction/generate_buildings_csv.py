import pandas as pd
import os 
import logging 

logger = logging 


def generate_buildings_csv(
    master_sheet_path: str,
    save_path: str
):
    logger.debug(
        "generating buildings csv"
    )
    raw_data = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory=False
    )

    # extracting columns needed 
    raw_office_data = raw_data.loc[:, [
        "Office", "Province", "Province.1", 
        "City", "Address", "Postal Code"
    ]]

        # get unique buildings 
    unique_offices = raw_office_data[ 
        ~ raw_office_data["Postal Code"].isna() 
    ]
    unique_offices = unique_offices[ 
        unique_offices["Office"] != "NA - ND" 
    ].drop_duplicates(subset=["Office"])


    province_map = {
        "Alberta": "base.state_ca_ab",
        "British Columbia": "base.state_ca_bc",
        "Manitoba": "base.state_ca_mb",
        "New Brunswick": "base.state_ca_nb",
        "Newfoundland and Labrador": "base.state_ca_nl",
        "Northwest Territories": "base.state_ca_nt",
        "Nova Scotia": "base.state_ca_ns",
        "Nunavut": "base.state_ca_nu",
        "Ontario": "base.state_ca_on",
        "Prince Edward Island": "base.state_ca_pe",
        "Quebec": "base.state_ca_qc",
        "Saskatchewan": "base.state_ca_sk",
        "Yukon": "base.state_ca_yt",
        "AB": "Alberta",
        "BC": "British Columbia",
        "MB": "Manitoba",
        "NB": "New Brunswick",
        "NL": "Newfoundland and Labrador",
        "NT": "Northwest Territories",
        "NS": "Nova Scotia",
        "NU": "Nunavut",
        "ON": "Ontario",
        "PE": "Prince Edward Island",
        "QC": "Quebec",
        "SK": "Saskatchewan",
        "YT": "Yukon" 
    }

    building_csv_columns = [
        "ID", "Name", "Company Type", "Address Type", "Street", "City", "Zip", "State/External ID", "Country/External ID"
    ]

    building_csv = pd.DataFrame(
        columns = building_csv_columns
    )

    count = 0
    for index, row in unique_offices.iterrows():
        name = row["Office"]
        street = row["Address"]
        city = row["City"]
        province = row["Province"]
        postal_code = row["Postal Code"]

        # make id from name by lowering 
        row_id = name.lower()

        province_id = province_map.get(province)

        if province_id is None:
            name_array = name.split("-")
            province = province_map.get(name_array[0])
            if province is not None:
                province_id = province_map.get(province)
            else:
                print(name)
                province_id = ""
        
        if street != street:
            street = ""
        

        building_csv.loc[count] = [
            row_id,
            name,
            "company",
            "contact",
            street,
            city,
            postal_code,
            province_id,
            "base.ca"
        ]

        count += 1

    building_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-buildings-csv.csv"
        ),
        encoding="utf-8",
        index=False
    )
    logger.debug(
        "buildings csv generated"
    )