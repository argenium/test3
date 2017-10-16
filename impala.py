#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
from impala.dbapi import connect

DOCUMENTATION = '''
---
module: impala

short_description: Run a sql file in Impala

description:
    - "Run a sql files in Impala asynchronously"

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
    files:
        description:
            - List of files containing ';' separated sql queries
        required: true

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
    host: "data012-vip-01.devops.guavus.mtl"
    port: 21050
    database: "carereflex"
    user: "impala"
    files: ["/opt/guavus/carereflex/srx-data/schemas/impala/test.sql"]
'''

RETURN = '''
sql_queries:
    description: List of queries that were ran
    type: list
'''


def run_module():
    module_args = dict(
        host=dict(type='str', required=True),
        port=dict(type='int', required=False, default=21050),
        database=dict(type='str', required=False, default='default'),
        user=dict(type='str', required=False, default='impala'),
        files=dict(type='list', required=True)
    )

    result = dict(
        changed=False,
        sql_queries=[]
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

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
        for file in ansible_module.params['files']:
            with open(file, 'r') as file_handle:
                queries = file_handle.read()
                for query in queries.split(";"):
                    clean_query = query.strip()
                    if clean_query:
                        result['sql_queries'].append(clean_query)
                        cursor.execute_async(clean_query)

        while cursor.is_executing():
            time.sleep(1)

        result['changed'] = True
    except Exception as e:
        ansible_module.fail_json(msg=str(e))
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
