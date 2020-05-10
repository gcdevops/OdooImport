import xmlrpc.client
import logging 

logger = logging

def connect_to_rpc(
    username: str,
    password: str,
    db: str,  
    url: str
):

    try:
        common = xmlrpc.client.ServerProxy(
            url + "/xmlrpc/2/common"
        )
        common.version()
        uid = common.authenticate(
            db, username, password, {}
        )
        models = xmlrpc.client.ServerProxy(
            url + "/xmlrpc/2/object"
        )

        return models, uid
    except Exception as e:
        logger.critical(
            "Failed to authenticate against Odoo endpoint"
        )
        raise e