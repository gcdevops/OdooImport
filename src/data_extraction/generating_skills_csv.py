import pandas as pd 
import os 
import logging
import string 

logger = logging 


def generate_skills_csv(
    master_sheet_path: str,
    save_path: str
):
    logger.debug("generating skills csvs")
    raw_data = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory=False
    )

    skills = raw_data[["Skills", "Sub Skills"]]

    sorted_skills_dataframe = skills.sort_values("Skills")

    raw_skills_dataframe = sorted_skills_dataframe[
        ~sorted_skills_dataframe["Skills"].isna() & 
        ~sorted_skills_dataframe["Sub Skills"].isna()
    ]

    raw_skills_dataframe = raw_skills_dataframe.drop_duplicates(
        subset=["Skills","Sub Skills"]
    )

    subskills_csv = pd.DataFrame(
        columns=["ID", "Name", "Parent Skill"]
    )

    count = 0
    row_id_map = {}
    current_skill = raw_skills_dataframe.iloc[0]["Skills"]
    other_created = False
    for index, row in raw_skills_dataframe.iterrows():
        skill = row["Skills"]

        if skill != current_skill or count == raw_skills_dataframe.shape[0]:
            if other_created is False:
                skill_id = "-".join([j[0] for j in current_skill.lower().split(" ") if len(j) > 0 and j[0] in string.ascii_letters])
                row_id = skill_id + "-o"
                subskills_csv.loc[count] = [
                    row_id,
                    "Other",
                    current_skill
                ]
                count += 1
            else:
                other_created = False

        sub_skill = row["Sub Skills"]

        if sub_skill == "Other":
            other_created = True
        
        name_array = sub_skill.lower().split(" ")
        skill_id = "-".join([ j[0]  for j in skill.lower().split(" ") if len(j) > 0 and j[0] in string.ascii_letters])
        row_id = skill_id + "-" + name_array[0]
    
        if len(name_array) > 0:
            row_id += "-".join([ j[0] for j in name_array[1:]  if len(j) > 0 and j[0] in string.ascii_letters])
        
        row_id = "-".join(row_id.split("."))

        if row_id_map.get(row_id) is not None:
            not_unique = True
            counter = 1
            while not_unique:
                row_id_unique = row_id + "-" + str(counter)
                not_unique = row_id_map.get(row_id_unique) is not None
                counter += 1
            row_id = row_id_unique
            row_id_map[row_id] = 1
        else:
            row_id_map[row_id] = 1

        
        subskills_csv.loc[count] = [
            row_id,
            sub_skill,
            skill
        ]
        count += 1

        current_skill = skill
    
    skills_csv = pd.DataFrame(
        columns = [
            "ID",
            "name",
            "skill_ids/id",
            "skill_level_ids/name"
        ]
    )

    skill_level_csv = pd.DataFrame(
        columns = [
            "ID",
            "Name",
            "level_progress"
        ]
    )


    current_skill = ""
    count = 0
    unique_skills = 0 
    subskills_csv["Parent ID"] = ""
    subskills_csv["Level ID"] = ""
    for index, row in subskills_csv.iterrows():
        new_skill = row["Parent Skill"]
        sub_skill_id = row["ID"]
        if ( new_skill != current_skill or count == subskills_csv.shape[0]):
            row_id = "-".join(new_skill.lower().split(" ")).strip("\"")
            skills_csv.loc[count] = [
                row_id,
                new_skill,
                sub_skill_id,
                row_id[0:23] + "-level-1"
            ]
            skill_level_csv.loc[unique_skills] = [
                row_id[0:23] + "-level-1",
                1,
                100
            ]
            current_skill = new_skill
            subskills_csv.at[count, "Parent ID"] = row_id 
            subskills_csv.at[count, "Level ID"] = row_id[0:23] + "-level-1"
            count += 1
            unique_skills += 1
            
        else:
            skills_csv.loc[count] = [
                "",
                "",
                sub_skill_id,
                ""
            ]
            subskills_csv.at[count, "Parent ID"] = row_id
            subskills_csv.at[count, "Level ID"] = row_id[0:23] + "-level-1" 
            count += 1
    
    skill_level_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-skill-levels-csv.csv"
        ),
        encoding="utf-8",
        index=False
    )

    subskills_csv_saved = subskills_csv[["ID", "Name"]]
    subskills_csv_saved.to_csv(
        os.path.join(
            save_path,
            "odoo-sub-skills-csv.csv"
        ),
        encoding = "utf-8",
        index = False
    )

    subskills_csv.to_csv(
        os.path.join(
            save_path,
            "skills.csv"
        ),
        encoding="utf-8",
        index=False
    )

    skills_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-skills-csv.csv"
        ),
        encoding = "utf-8",
        index = False
    )

    logger.debug("generated skills csvs")