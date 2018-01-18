#!/usr/bin/python
import time

from ansible.module_utils.basic import AnsibleModule

try:
    from impala.dbapi import connect
    from impala import error
    HAS_LIB_IMPALA = True
except ImportError:
    HAS_LIB_IMPALA = False

DOCUMENTATION = '''
---
module: impala

short_description: Run sql queries in Impala

description:
    - "Run sql queries in Impala"

options:
    host:
        description:
            - Impala host
        required: true
    port:
        description:
            - Impala port
        required: false
    database:
        description:
            - Impala database
        required: false
    user:
        description:
            - Impala user
        required: false
    files_reference:
        description:
            - List of files containing ';' separated sql queries to be ran asynchronously
              This option is mutually exclusive with C('inline_query').
    inline_query:
        description:
            - Inline sql query
              This option is mutually exclusive with C('files_reference').

requirements:
  - "python >= 2.7"
  - "impyla >= 0.14.0"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
# Test a sql file
- name: Test sql file
  impala:
    host: "data018-vip-01.devops.guavus.mtl"
    port: 21050
    database: "carereflex"
    user: "impala"
    files_reference: ["/opt/guavus/carereflex/srx-data/schemas/impala/test.sql"]

# Test a sql query
- name: Test sql file
  impala:
    host: "data018-vip-01.devops.guavus.mtl"
    port: 21050
    database: "carereflex"
    user: "impala"
    inline_query: "SHOW TABLES"
'''

RETURN = '''
sql_queries:
    description: List of queries that were ran
    type: list
sql_result:
    description: Result for C('inline_query')
    type: str
'''


def run_module():
    module_args = dict(
        host=dict(type='str', required=True),
        port=dict(type='int', required=False, default=21050),
        database=dict(type='str', required=False, default='default'),
        user=dict(type='str', required=False, default='impala'),
        files_reference=dict(type='list'),
        inline_query=dict(type='str')
    )

    result = dict(
        changed=False,
        sql_queries=[],
        sql_result=None
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        mutually_exclusive=[('files_reference', 'inline_query')],
        required_one_of=[('files_reference', 'inline_query')],
        supports_check_mode=True
    )

    if not HAS_LIB_IMPALA:
        ansible_module.fail_json(msg="missing python library: impyla")

    if ansible_module.check_mode:
        return result

    connection = None
    cursor = None
    try:
        connection = connect(
            host=ansible_module.params['host'],
            port=ansible_module.params['port'],
            database=ansible_module.params['database'],
            user=ansible_module.params['user'],
            auth_mechanism="NOSASL"
        )
        cursor = connection.cursor(user=ansible_module.params['user'])
        if ansible_module.params['inline_query']:
            clean_query = ansible_module.params['inline_query'].strip()
            result['sql_queries'].append(clean_query)
            try:
                cursor.execute(clean_query)
                if cursor.has_result_set:
                    result['sql_result'] = cursor.fetchall()
            except error.HiveServer2Error as e:
                ansible_module.fail_json(msg=str(e), sql_query=clean_query, changed=True)
        else:
            for file_name in ansible_module.params['files_reference']:
                with open(file_name, 'r') as file_handle:
                    queries = file_handle.read()
                    for query in queries.split(";"):
                        clean_query = query.strip()
                        if clean_query:
                            result['sql_queries'].append(clean_query)
                            try:
                                cursor.execute_async(clean_query)
                            except error.HiveServer2Error as e:
                                ansible_module.fail_json(msg=str(e), sql_query=clean_query,
                                                         file=file_name, changed=True)

            while cursor.is_executing():
                time.sleep(1)

        result['changed'] = True
    except Exception as e:
        ansible_module.fail_json(msg=str(e), changed=True)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
