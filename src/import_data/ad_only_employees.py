import logging 
from utils.connect_to_rpc import connect_to_rpc

logger = logging 


def find_first_available_org(
    models,
    db,
    uid,
    password,
    org_code
): 
    logger.debug('attempting to find org ' + org_code)
    org_code_array = org_code.split(".")
    if len(org_code_array) == 0 or (len(org_code_array) == 1 and org_code_array[0] == ""):
        return None
    
    external_id = "-".join(org_code_array)

    org_row = models.execute_kw(
        db, uid, password,
        'ir.model.data',
        'search_read',
        [[['name', '=', external_id]]],
        {
            'fields': ['res_id']
        }
    )


    if org_row is None or len(org_row) == 0:
        logger.debug('org not found attempting to find parent')
        return find_first_available_org(
            models, db, uid, password, 
            ".".join(org_code_array[:-1])
        )

    return org_row[0]['res_id']

def find_branch(
    models,
    db,
    uid,
    password,
    branch
):
    logger.debug("attempting to find branch " + branch)
    branch_row = models.execute_kw(
        db, uid, password,
        'hr.branch',
        'search_read',
        [[['name', '=', branch]]],
        {
            'fields': ['id']
        }
    )

    if branch_row is not None and len(branch_row) > 0:
        return branch_row[0]['id']
    
    logger.debug("could not find branch " + branch)

def find_region(
    models,
    db,
    uid,
    password,
    region
):
    logger.debug("attempting to find region " + region)
    region_row = models.execute_kw(
        db, uid, password,
        'hr.region',
        'search_read',
        [[['name', '=', region]]],
        {
            'fields': ['id']
        }
    )

    if region_row is not None and len(region_row) > 0:
        return region_row[0]['id']
    
    logger.debug("could not find region " + region)

def find_or_create_job_position(
    models,
    db,
    uid,
    password,
    job_title
):
    logger.debug("attempting to find job " + job_title)
    job_row = models.execute_kw(
        db, uid, password,
        'hr.job',
        'search_read',
        [[['name', '=', job_title]]],
        {
            'fields': ['id']
        }
    )

    if job_row is not None and len(job_row) > 0:
        return job_row[0]['id']
    else:
        logger.debug("could not find job " + job_title + " attempting to create")
        filtered_chars = ".?&/\\,"
        name_array = job_title.split(" ")
        name_array = [ 
            "".join([j for j in i if j not in filtered_chars]).lower() for i in name_array
        ]
        job_external_id = "-".join(name_array)

        record_id = models.execute(
            db, uid, password,
            "hr.job", 'create',
            {
                'name': job_title
            }
        )

        return record_id


def find_address(
    models,
    db,
    uid,
    password,
    office,
    address,
    province,
    city,
    postal_code
):
    logger.debug("attempting to find building " + office)
    office_row = models.execute_kw(
        db, uid, password,
        'res.partner',
        'search_read',
        [[['name', '=', office]]],
        {
            'fields': ['id']
        }
    )

    if office_row is not None and len(office_row) > 0:
        return office_row[0]['id']
    else:
        logger.debug("could not find building " + office + " attempting to create")
        province_map = {
            "Alberta": "state_ca_ab",
            "British Columbia": "state_ca_bc",
            "Manitoba": "state_ca_mb",
            "New Brunswick": "state_ca_nb",
            "Newfoundland and Labrador": "state_ca_nl",
            "Northwest Territories": "state_ca_nt",
            "Nova Scotia": "state_ca_ns",
            "Nunavut": "state_ca_nu",
            "Ontario": "state_ca_on",
            "Prince Edward Island": "state_ca_pe",
            "Quebec": "state_ca_qc",
            "Saskatchewan": "state_ca_sk",
            "Yukon": "state_ca_yt",
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


        province_id = province_map.get(province)

        if province_id is None:
            name_array = office.split("-")
            province = province_map.get(name_array[0])
            if province is not None:
                province_id = province_map.get(province)
                province_row = models.execute_kw(
                    db, uid, password,
                    'ir.model.data', 'search_read',
                    [
                        [
                            '&',('model', '=', 'res.country.state'),
                            ('name', '=', province_id)
                        ]
                    ],
                        {'fields': ['res_id']}
                )
                province_id = province_row[0]['res_id']
            else:
                province_id = ""
        else:
            print(province_id)
            province_row = models.execute_kw(
                    db, uid, password,
                    'ir.model.data', 'search_read',
                    [
                        [
                            '&',('model', '=', 'res.country.state'),
                            ('name', '=', province_id)
                        ]
                    ],
                        {'fields': ['res_id']}
                )
            province_id = province_row[0]['res_id']
            print(province_id)
        
        
        if address != address:
            address = ""
        
        if city != city:
            city = ""
        
        if postal_code != postal_code:
            postal_code = "" 

        country_id = models.execute_kw(
            db, uid, password,
            'ir.model.data',
            'search_read',
            [
                [
                    '&',('model', '=', 'res.country'),
                    ('name', '=', 'ca')]
            ],
            {'fields': ['res_id']}
        )

        country_id = country_id[0]["res_id"]
        print(country_id)
        record_id = models.execute(
            db, uid, password,
            'res.partner',
            'create',
            {
                'name': office,
                'is_company': "true",
                'street': address,
                'city': city,
                'zip': postal_code,
                'country_id': country_id,
                'state_id': province_id
            }
        )

        return record_id


def find_skill_profile(
    models, db, uid, password,
    skill, sub_skill
):
    if sub_skill != sub_skill or sub_skill == "":
        sub_skill = "Other"
    logger.debug(
        'attempting to find skill ' + skill + ' and sub skill ' + sub_skill
    )
    skill_row = models.execute_kw(
        db, uid, password,
        'hr.skill.type',
        'search_read',
        [[['name', '=', skill]]],
        {
            'fields': ['id']
        }
    )

    if skill_row is not None and len(skill_row) > 0:
        skill_id = skill_row[0]['id']
        sub_skill_row = models.execute_kw(
            db, uid, password,
            'hr.skill',
            'search_read',
            [[  '&',
                ['name', '=', sub_skill],
                ['skill_type_id', '=', skill_id]
            ]],
            {
                'fields': ['id']
            }
        )
        
        if sub_skill_row is not None and len(sub_skill_row) > 0:
            sub_skill_id = sub_skill_row[0]['id']
            skill_level_row = models.execute_kw(
                db, uid, password,
                'hr.skill.level',
                'search_read',
                [[  '&',
                    ['name', '=', '1'],
                    ['skill_type_id', '=', skill_id]
                ]],
                {
                    'fields': ['id']
                }
            )

            if skill_level_row is not None and len(skill_level_row) > 0:
                skill_level_id =  skill_level_row[0]['id']
                return {
                    'skill_type_id': skill_id,
                    'skill_level_id': skill_level_id,
                    'skill_id': sub_skill_id 
                }
            else:
                record_id = models.execute_kw(
                    db, uid, password,
                    'hr.skill.level',
                    'create',
                    {
                        'name': '1',
                        'skill_type_id': skill_id
                    }
                )

                return {
                    'skill_type_id': skill_id,
                    'skill_level_id': skill_level_id,
                    'skill_id': sub_skill_id
                }
        else:
            logger.debug('could not find sub skill ' + sub_skill + ' attempting to create')
            sub_skill_record_id = models.execute(
                db, uid, password,
                'hr.skill',
                'create',
                {
                    'name': sub_skill,
                    'skill_type_id': skill_id
                }
            )

            skill_level_record_id = models.execute(
                db, uid, password,
                'hr.skill.level',
                'create',
                {
                    'name': '1',
                    'skill_type_id': skill_id
                }
            )

            return {
                'skill_type_id': skill_id,
                'skill_level_id': skill_level_record_id,
                'skill_id': sub_skill_record_id
            }

    logger.debug('could not find skill ' + skill + ' attempting to create skill and sub skill ' + sub_skill)
    skill_record_id = models.execute(
        db, uid, password,
        'hr.skill.type',
        'create',
        {
            'name': skill
        }
    )

    sub_skill_record_id = models.execute(
        db, uid, password,
        'hr.skill',
        'create',
        {
            'name': sub_skill,
            'skill_type_id': skill_record_id
        }
    )

    skill_level_record_id = models.execute(
        db, uid, password,
        'hr.skill.level',
        'create',
        {
            'name': '1',
            'skill_type_id': skill_record_id
        }
    )

    return {
        'skill_type_id': skill_record_id,
        'skill_level_id': skill_level_record_id,
        'skill_id': sub_skill_record_id
    }


def ad_only_employees(
    employee_list,
    ad_employee_data,
    username,
    password,
    database,
    url
):
    logger.debug(
        "Creating AD Only Employees in Odoo"
    )

    try:
        emails_handled = {}
        models, uid = connect_to_rpc(
            username,
            password,
            database,
            url
        )

        org_message_sent = False 
        for email in employee_list:
            if emails_handled.get(email) is None:
                matched_employee_row = ad_employee_data[
                    ad_employee_data["E-mail Address"] == email
                ]

                branch = matched_employee_row.iloc[0]["Branch"]
                region = matched_employee_row.iloc[0]["Region"]
                province = matched_employee_row.iloc[0]['Province']
                city = matched_employee_row.iloc[0]['City']
                phone_number = matched_employee_row.iloc[0]["Phone Number"]
                if phone_number != phone_number:
                    phone_number = ""
                
                office = matched_employee_row.iloc[0]["Office"] 
                address = matched_employee_row.iloc[0]["Address"] 
                postal_code = matched_employee_row.iloc[0]["Postal Code"] 
                desk_location = matched_employee_row.iloc[0]["Desk Location"]
                floor = matched_employee_row.iloc[0]["Floor"]
                if floor != floor:
                    floor = ""
                division_name = matched_employee_row.iloc[0]["Division Name"]
                division = matched_employee_row.iloc[0]["Division"]
                name = matched_employee_row.iloc[0]["Name"]
                title = matched_employee_row.iloc[0]["Title"]
                skill = matched_employee_row.iloc[0]["Skills"]
                sub_skill = matched_employee_row.iloc[0]["Sub Skills"]
                device_type = matched_employee_row.iloc[0]["Device Type"] 
                asset_number = matched_employee_row.iloc[0]["Asset Number"] 
                app_gate = matched_employee_row.iloc[0]["AppGate"] 
                vpn = matched_employee_row.iloc[0]["VPN"]
                critical = matched_employee_row.iloc[0]["Critical"]

                branch_id = ""
                if branch == branch:
                    branch_id = find_branch(
                        models,
                        database,
                        uid,
                        password,
                        branch
                    ) or ""
                
                region_id = ""
                if region == region:
                    region_id = find_region(
                        models,
                        database,
                        uid,
                        password,
                        region
                    ) or ""
                
                division_id = ""
                if division == division:
                    division_id = find_first_available_org(
                        models,
                        database,
                        uid,
                        password,
                        division
                    )
                    if division_id is None:
                        logger.debug(
                            "Could not find any orgs with code " + division + " trying employee with root org"
                        )

                        division_row = models.execute_kw(
                            database, uid, password,
                            'ir.model.data',
                            'search_read',
                            [[['name', '=', '100000']]],
                            {
                                'fields': ['res_id']
                            }
                        )
                        
                        if division_row is not None and len(division_row) > 0:
                            division_id = division_row[0]['res_id']
                        elif org_message_sent is False:
                            logger.critical(
                                "Could not find the root organization. " +
                                " All employees in which their org tree cannot be found " +
                                " will not have an org set."
                            )
                            logger.debug("No org found or can be set: " + email)
                            org_message_sent = True
                        else:
                            logger.debug("No org found or can be set: " + email)
                

                building_id = ""
                if  office == office:
                    building_id = find_address(
                        models, database, uid, password,
                        office,
                        address,
                        province,
                        city,
                        postal_code
                    )
                
                job_id = ""
                if title == title:
                    job_id = find_or_create_job_position(
                        models, database, uid, password,
                        title
                    )
                

                skill_map = {}
                if skill == skill and sub_skill == sub_skill:
                    skill_map = find_skill_profile(
                        models, database, uid, password,
                        skill, sub_skill
                    )
                
                update_dict = {
                    'name': name,
                    'work_email': email,
                    'work_phone': phone_number or "",
                    'x_employee_office_floor': floor or "",
                    'x_employee_work_criticality': bool(critical),
                    'x_employee_in_ad': True,
                    'department_id': division_id,
                    'branch_id': branch_id,
                    'job_id': job_id,
                    'address_id': building_id,
                    'region_id': region_id,
                }

                if asset_number == asset_number and asset_number != "":
                    update_dict['x_employee_asset_number'] = asset_number
                
                if device_type == device_type and device_type.strip() == "Desktop":
                    update_dict['x_employee_device_type'] = "desktop"
                elif device_type == device_type and device_type.strip() == "Laptop":
                    update_dict['x_employee_device_type'] = "laptop"
                elif device_type == device_type and device_type.strip() == "Tablet":
                    update_dict['x_employee_device_type'] = "tablet"
                
                if app_gate != app_gate:
                    app_gate = False
                
                if vpn != vpn:
                    vpn = False
                
                if app_gate and vpn:
                    update_dict['x_employee_remote_access_tool'] = "both"
                    update_dict['x_employee_remote_access_network'] = True
                elif vpn:
                    update_dict['x_employee_remote_access_tool'] = "vpn"
                    update_dict['x_employee_remote_access_network'] = True
                elif app_gate:
                    update_dict['x_employee_remote_access_tool'] = "appgate"
                    update_dict['x_employee_remote_access_network'] = True
                else:
                    update_dict['x_employee_remote_access_tool'] = ""
                    update_dict['x_employee_remote_access_network'] = False

                print(update_dict)

                record_id = models.execute(
                    database, uid, password,
                    'hr.employee',
                    'create', 
                    update_dict
                )

                if bool(skill_map):
                    skill_map['employee_id'] = record_id
                    print(skill_map)
                    models.execute(
                        database, uid, password,
                        'hr.employee.skill',
                        'create',
                        skill_map
                    )
                
                emails_handled[email] = 1
    except Exception as e:
        logger.critical(
            "Failed to add AD Only Employees in Odoo",
            exc_info = True
        )
        raise e