import pandas as pd 
import logging
import requests 
import os
import json 

logger = logging 

def generate_classifications_csv(
    save_path: str
):
    logger.debug("generating classifications csv")

    query = """query{
        groups{
            identifier
            payscales{
                level
            }
        }
    }
    """

    url = "http://gc-payscales.herokuapp.com/graphql"

    res = requests.post(
        url, 
        json={
            'query': query
        }
    )
    if res.status_code == 200:
        res_data = json.loads(res.text)

        if res_data.get("data") is not None and res_data['data'].get("groups") is not None:
            classification_csv = pd.DataFrame(
                columns = ["ID", "Classification Name", "Level"]
            )
            res_data_array = res_data["data"]["groups"]
            count = 0
            for i in res_data_array:
                name = i["identifier"]
                levels = i["payscales"]
                for l in levels:
                    level = l['level']
                    row_id = name + "-" + str(level)
                    classification_csv.loc[count] = [
                        row_id,
                        name,
                        level
                    ]

                    count += 1
            
            classification_csv.to_csv(
                os.path.join(
                    save_path,
                    "odoo-classifications-csv.csv"
                ),
                encoding = "utf-8",
                index= False
            )
        
        logger.debug("Classifications Generated")
    
    else:
        logger.critical("Classifications API unreachable")




