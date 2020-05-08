import pandas as pd
import os
import logging

logger = logging

class Organization:
    def __init__(self, code, english_name, french_name = None, parent = None):
        self.code_str = code
        self.code_array = code.split(".")
        self.english_name = english_name
        self.french_name = french_name 
        self.parent = parent
        self.children = []
        self.childrenMapCode = {} 
        self.childrenMapName = {}
    
    def generateId(self):
        return "-".join(self.getCodeArray()) 

    def getCodeStr(self):
        return self.code_str
    
    def getCodeArray(self):
        return self.code_array
    
    def getEnglishName(self):
        return self.english_name
    
    def getFrenchName(self):
        return self.french_name 
    
    def getParent(self):
        return self.parent
    
    def getChildren(self):
        return self.children
    
    def getChildByCode(self, code):
        return self.childrenMapCode.get(code)
    
    def getChildrenByName(self, name):
        return self.childrenMapName.get(name)
    
    def getParent(self):
        return self.parent

    def setParent(self, parent):
        self.parent = parent 

    def addChild(self, child):
        child_code = child.getCodeStr()
        child_name = child.getEnglishName()
        if self.getChildByCode(child_code) is not None:
            raise ValueError(
                "child " + str(child) + " has already been added to " + str(self)
            )
        self.children.append(child)
        self.childrenMapCode[child_code] = child
        
        if self.getChildrenByName(child_name) is None:
            self.childrenMapName[child_name] = [child]
        else:
            self.childrenMapName[child_name].append(child)
        
        child.setParent(self)
        

    def __str__(self):
        return "Organization= code:" + self.getCodeStr() + ", name:" + self.getEnglishName()


class OrganizationTree:
    def __init__(self, base_organization, data):
        self.data = data
        self.root = base_organization
        
        self.organizationMapName = {}
        self.organizationMapName[self.root.getEnglishName()] = [self.root]
        self.organizationMapCode = {}
        self.organizationMapCode[self.root.getCodeStr()] = self.root

        for index, row in self.data.iterrows():
            org_code = row["Division"]
            org_name = row["Division Name"]

            if self.getOrginzationByCode(org_code) is None:
                org = Organization(
                    org_code,
                    org_name
                )
                self.__addOrganizationToMap(org)
                self.__checkIfParentExists(org_code, org)
        
    def getOrginzationByCode(self, code):
        return self.organizationMapCode.get(code)
    
    def getOrginzationsByName(self, name):
        return self.organizationMapName.get(name)

    def __addOrganizationToMap(self, org):
        org_code = org.getCodeStr()
        org_name = org.getEnglishName()
        if self.getOrginzationByCode(org_code) is not None:
            raise ValueError(
                str(org) + " already exists in tree"
            )
        
        self.organizationMapCode[org_code] = org 
        
        if self.getOrginzationsByName(org_name) is None:
            self.organizationMapName[org_name] = [org]
        else:
            self.organizationMapName[org_name].append(
                org
            )


    def __checkIfParentExists(self, code, org):
        parent_code = code.rsplit(".", 1)
    
        # base case, we have reached the root org 
        if len(parent_code) == 1:
            parent_code = parent_code[0]
            if parent_code == self.root.getCodeStr():
                self.root.addChild(org)
            return
        
        elif len(parent_code) > 0:
            parent_code = parent_code[0]
            parent_org = self.getOrginzationByCode(parent_code)
            
            # case where parent org already exists in tree 
            if parent_org is not None:
                parent_org.addChild(org)
            
            else:
                parent_org_df = self.data[self.data["Division"] == parent_code]

                # case where parent exists in data frame 
                if(parent_org_df.shape[0] == 1):
                    parent_org_row = parent_org_df.iloc[0]
                    parent_org = Organization(
                        parent_code,
                        parent_org_row["Division Name"]
                    )
                    parent_org.addChild(
                        org
                    )
                    self.__addOrganizationToMap(parent_org)
                    self.__checkIfParentExists(parent_code, parent_org)
                
                # case where parent org does not exist in data frame 
                # we skip a level  
                else:
                    self.__checkIfParentExists(parent_code.rsplit(".", 1)[0], org)
    
    
    def __generateDataFrame(self, dataFrame, parent_id, children):
        for i in children:
            index = dataFrame.shape[0]
            child_id = i.generateId()
            dataFrame.loc[index] = [
                child_id,
                i.getEnglishName(),
                parent_id
            ]
            self.__generateDataFrame(dataFrame, child_id, i.getChildren())
    
    def generateDataFrame(self):
        columns = ["ID", "Department Name", "Parent Department/External ID"]
        departmentDataFrame = pd.DataFrame(columns=columns)
        departmentDataFrame.loc[0] = [
            self.root.generateId(),
            self.root.getEnglishName(),
            ""
        ]

        self.__generateDataFrame(
            departmentDataFrame, 
            self.root.generateId(), 
            self.root.getChildren()
        )

        return departmentDataFrame


def generate_org_csv(
    org_sheet_path:str,
    save_path: str,
    orgs_to_ignore:list
):
    logger.debug("generating org csv")
    raw_data = pd.read_csv(
        org_sheet_path,
        encoding="utf-8",
        low_memory=False
    )

    # see if there is any non standard structures for Division 
    def division_validator(value):
        if value != value:
            return False

        division_array = value.split(".")

        if len(division_array) == 0:
            return False
        
        for i in division_array:
            try:
                int(i)
            except:
                return False
        
        return True
    
    raw_data = raw_data[raw_data["Division"].apply(division_validator)]

    # drop NaN from list 
    division_no_nan = raw_data.dropna(subset=["Division"])
    raw_data_no_nan = division_no_nan.dropna(subset=["Division Name"])
    unique_orgs = raw_data_no_nan.drop_duplicates(subset=["Division"])

    esdc_organization = Organization(
        "100000",
        "Employment and Social Development Canada"
    )

    esdc_org_tree = OrganizationTree(esdc_organization, unique_orgs)
    department_csv = esdc_org_tree.generateDataFrame()

    if len(orgs_to_ignore) >= 1:
        for i in orgs_to_ignore:
            department_csv = department_csv[~department_csv["ID"].str.startswith("-".join(i.split(".")))]
    
    department_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-org-csv.csv"
        ),
        encoding="utf-8",
        index=False
    )

    logging.debug("org csv generated")
