import re


# Handling SAP HANA and APP DB
def extract_hana_settings(json_data, source_ep_name):
    tasks = json_data['cmd.replication_definition']['tasks']
    replicate_json_hostname = json_data['description']
    databases = json_data['cmd.replication_definition']['databases']
    # Find the source and target EP name for tasks

    for database in databases:
        # print(database['name'])
        if database['name'] == source_ep_name:
            db_type = database['type_id']
            # If DB type is 'SAP_APPLICATION_COMPONENT_TYPE' or 'SAP_HANA_SRC_COMPONENT_TYPE' collect these settings
            if db_type == 'SAP_APPLICATION_COMPONENT_TYPE' or db_type == 'SAP_HANA_SRC_COMPONENT_TYPE':
                source_endpoint_name = database['name']
                source_db_type = database['type_id']
                source_db_role = database['role']
                source_db_user = database['db_settings'].get('username')
                source_server = database['db_settings'].get('server')
                source_client = database['db_settings'].get('client')
                source_backend_db = database['db_settings'].get('backend_db')
                source_instance_number = database['db_settings'].get('instance_number')

                # Advance Settings
                source_cleanup_interval = database['db_settings'].get('cleanup_interval')
                source_log_retention_period = database['db_settings'].get('log_retention_period')
                source_logTableTriggerBasedMode = database['db_settings'].get('logTableTriggerBasedMode')
                source_logstreamstagingtask = database['db_settings'].get('logstreamstagingtask')
                if source_backend_db is not None:
                    for database in databases:
                        if database['name'] == source_backend_db:
                            backend_db_host = database['db_settings'].get('server')
                            backend_db_user = database['db_settings'].get('username')
                            backend_db_instance_number = database['db_settings'].get('instance_number')
                            backend_db_cleanup_interval = database['db_settings'].get('cleanup_interval')
                            backend_db_log_retention_period = database['db_settings'].get('log_retention_period')
                            backend_db_logTableTriggerBasedMode = database['db_settings'].get('logTableTriggerBasedMode')
                            backend_db_logstreamstagingtask = database['db_settings'].get('logstreamstagingtask')
                        else:
                            None
                source_rfc_call_batch = database['db_settings'].get('rfc_call_batch')
                source_connection_type = database['db_settings'].get('connection_type')
                source_server_group = database['db_settings'].get('server_group')
                source_message_server_service = database['db_settings'].get('message_server_service')
                source_r3_system = database['db_settings'].get('r3_system')
                source_store_only_tdline_in_stxtl_clustd = database['db_settings'].get('store_only_tdline_in_stxtl_clustd')
