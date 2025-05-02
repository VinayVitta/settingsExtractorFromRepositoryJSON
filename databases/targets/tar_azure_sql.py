# Handling azure_sql settings
import helpers.utils
import json
import re
from helpers.utils import get_non_info_settings


def extract_azure_sql_settings(json_data, target_ep_name):
    tasks = json_data['cmd.replication_definition']['tasks']
    replicate_json_hostname = json_data['description']
    databases = json_data['cmd.replication_definition']['databases']

    for database in databases:
        # print(database['name'])
        if database['name'] == target_ep_name:
            target_db_type = database['type_id']
            if target_db_type == 'AZURE_SQL_DB_COMPONENT_TYPE':
                target_endpoint_name = database['name']
                target_additional_properties = database['db_settings'].get(
                    'additionalConnectionProperties') if 'additionalConnectionProperties' in database['db_settings'] else None
                target_username = database['db_settings'].get('username')
                target_server = database['db_settings'].get('server')
                target_database = database['db_settings'].get('database')
                target_executeTimeout = database['db_settings'].get('executeTimeout') if 'executeTimeout' in database[
                    'db_settings'] else None
                target_loadTimeout = database['db_settings'].get('loadTimeout') if 'loadTimeout' in database['db_settings'] else None
                target_safeguardPolicy = database['db_settings'].get('target_safeguardPolicy') if 'target_safeguardPolicy' in database[
                    'db_settings'] else None
