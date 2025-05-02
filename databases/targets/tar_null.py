# Handling Null settings
import helpers.utils
import json
import re
from helpers.utils import get_non_info_settings


def extract_null_settings(json_data, target_ep_name):
    tasks = json_data['cmd.replication_definition']['tasks']
    replicate_json_hostname = json_data['description']
    databases = json_data['cmd.replication_definition']['databases']

    for database in databases:
        # print(database['name'])
        if database['name'] == target_ep_name:
            target_db_type = database['type_id']
            if target_db_type == 'NULL_TARGET_COMPONENT_TYPE':
                target_endpoint_name = database['name']
