#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
try:
    from pyhive import hive, exc
    from TCLIService.ttypes import TOperationState
    HAS_LIB_HIVE = True
except ImportError:
    HAS_LIB_HIVE = False

DOCUMENTATION = '''
---
module: hive

short_description: Run a sql file or sql queries in Hive

description:
    - "Run a sql file in Hive"

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
    extra_settings:
        description:
            - Hive settings need to run before executing queries
        required: false
    files_reference:
        description:
            - List of files containing ';' separated sql queries
            This option is mutually exclusive with C('inline_query').
        required: false
    inline_query:
        description:
            - Inline sql query
              This option is mutually exclusive with C('files_reference').
        required: false

requirements:
  - "python >= 2.7"
  - "pyhive[hive] >= 0.2.1"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
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
sql_queries:
    description: List of queries that were ran
    type: list
sql_result:
    description: Result for C('inline_query')
    type: str
sql_settings:
    description: List of settings that were ran
    type: list
'''


def run_module():
    module_args = dict(
        host=dict(type='str', required=True),
        port=dict(type='int', required=False, default=10000),
        database=dict(type='str', required=False, default='default'),
        extra_settings=dict(type='list', required=False),
        files_reference=dict(type='list', required=False),
        inline_query=dict(type='str', required=False)
    )

    result = dict(
        changed=False,
        sql_queries=[],
        sql_settings=[],
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
        if ansible_module.params['extra_settings']:
            for setting in ansible_module.params['extra_settings']:
                cursor.execute(setting)
                result['sql_settings'].append(setting)
        if ansible_module.params['inline_query']:
            clean_query = ansible_module.params['inline_query'].strip()
            result['sql_queries'].append(clean_query)
            rows = cursor.execute(clean_query)
            if rows > 0:
                result['sql_result'] = cursor.fetchall()
        else:
            for file in ansible_module.params['files_reference']:
                with open(file, 'r') as file_handle:
                    queries = file_handle.read()
                    for query in queries.split(";"):
                        clean_query = query.strip()
                        if clean_query:
                            result['sql_queries'].append(clean_query)
                            cursor.execute(clean_query)

        while cursor.poll().operationState in (
                TOperationState.INITIALIZED_STATE,
                TOperationState.RUNNING_STATE):
            time.sleep(1)

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
