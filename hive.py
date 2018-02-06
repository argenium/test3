#!/usr/bin/python

# Copyright (c) 2017 [Guavus]
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from ansible.module_utils.basic import AnsibleModule
try:
    from pyhive import hive, exc
    HAS_LIB_HIVE = True
except ImportError:
    HAS_LIB_HIVE = False

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: hive
short_description: Run a sql file or sql queries in Hive
description:
    - Run a sql file in Hive
author:
    - Vanessa Vuibert (@vvuibert)
version_added: "2.2"
options:
    host:
        description:
            - Hive host
        required: true
    port:
        description:
            - Hive port
        required: false
    database:
        description:
            - Hive database
        required: false
        default: default
    extra_settings:
        description:
            - Hive settings need to run before executing queries
        required: false
    files_reference:
        description:
            - List of files containing ';' separated sql queries
            - This option is mutually exclusive with C('inline_query').
        required: false
    inline_query:
        description:
            - Inline sql query
            - This option is mutually exclusive with C('files_reference').
        required: false
    app_name:
        description:
            - Application name for the YARN UI
        required: false
        default: Ansible Hive Module
requirements:
  - "python >= 2.7"
  - "pyhive[hive] >= 0.2.1"
'''

EXAMPLES = '''
---
# Test a sql file
- name: Test sql file
  hive:
    host: "data012-vip-01.devops.guavus.mtl"
    port: "10000"
    database: "carereflex"
    files_reference: ["/opt/guavus/carereflex/srx-data/schemas/hive/test.sql"]

# Test a sql query
- name: Test sql query
  hive:
    host: "data012-vip-01.devops.guavus.mtl"
    port: 10000
    database: "carereflex"
    inline_query: "SHOW TABLES"
'''

RETURN = '''
---
sql_queries:
    description: List of queries that were ran
    returned: success
    type: list
sql_result:
    description: Result for C('inline_query')
    returned: success
    type: string
sql_settings:
    description: Hive settings
    returned: success
    type: dict
'''


def run_module():
    module_args = dict(
        host=dict(type='str', required=True),
        port=dict(type='int', required=False, default=10000),
        database=dict(type='str', required=False, default='default'),
        extra_settings=dict(type='dict', required=False),
        files_reference=dict(type='list', required=False),
        inline_query=dict(type='str', required=False),
        app_name=dict(type='str', required=False, default='Ansible Hive Module')
    )

    result = dict(
        changed=False,
        sql_queries=[],
        sql_settings={},
        sql_result=None
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        mutually_exclusive=[('files_reference', 'inline_query')],
        required_one_of=[('files_reference', 'inline_query')],
        supports_check_mode=True
    )

    if not HAS_LIB_HIVE:
        ansible_module.fail_json(msg="missing python library: pyhive[hive]")

    if ansible_module.check_mode:
        return result

    cursor = None
    try:
        cursor = hive.connect(
            host=ansible_module.params['host'],
            port=ansible_module.params['port'],
            database=ansible_module.params['database']).cursor()
        conf_query = "SET {}={}"
        cursor.execute(conf_query.format("spark.app.name", ansible_module.params['app_name']))
        if ansible_module.params['extra_settings']:
            for key, value in ansible_module.params['extra_settings'].iteritems():
                cursor.execute(conf_query.format(key, value))
                result['sql_settings'][key] = value
        if ansible_module.params['inline_query']:
            clean_query = ansible_module.params['inline_query'].strip()
            result['sql_queries'].append(clean_query)
            cursor.execute(clean_query)
            try:
                result['sql_result'] = cursor.fetchall()
            except exc.ProgrammingError:
                pass
        else:
            for file_name in ansible_module.params['files_reference']:
                with open(file_name, 'r') as file_handle:
                    queries = file_handle.read()
                    for query in queries.split(";"):
                        clean_query = query.strip()
                        if clean_query:
                            result['sql_queries'].append(clean_query)
                            cursor.execute(clean_query)

        result['changed'] = True
    except Exception as e:
        ansible_module.fail_json(msg=str(e))
    finally:
        if cursor:
            cursor.close()
    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
