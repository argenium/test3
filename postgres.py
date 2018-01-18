#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
try:
    from psycopg2 import connect, Error
    from psycopg2.extras import wait_select
    HAS_LIB_POSTGRES = True
except ImportError:
    HAS_LIB_POSTGRES = False

DOCUMENTATION = '''
---
module: postgres

short_description: Run a sql file in Postgres

description:
    - "Run a sql files in Postgres asynchronously"

options:
    host:
        description:
            - Postgres host
        required: true
    port:
        description:
            - Postgres port
        required: false
    database:
        description:
            - Postgres database
        required: false
    user:
        description:
            - Postgres user
        required: false
    password:
        description:
            - Postgres password
        required: false
    files_reference:
        description:
            - List of files containing ';' separated sql queries
        required: true

requirements:
  - "python >= 2.7"
  - "psycopg2 >= 2.5.1"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
# Test a sql file
- name: Test sql file
  postgres:
    host: "data012-vip-01.devops.guavus.mtl"
    port: 5432
    database: "carereflex"
    user: "postgres"
    password: "postgres"
    files_reference: ["/opt/guavus/carereflex/srx-data/schemas/postgres/test.sql"]
'''

RETURN = '''
sql_queries:
    description: List of queries that were ran
    type: list
'''


def run_module():
    module_args = dict(
        host=dict(type='str', required=True),
        port=dict(type='int', required=False, default=5432),
        database=dict(type='str', required=False, default=None),
        user=dict(type='str', required=False, default='postgres'),
        password=dict(type='str', required=False, default='postgres'),
        files_reference=dict(type='list', required=True)
    )

    result = dict(
        changed=False,
        sql_queries=[]
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if not HAS_LIB_POSTGRES:
        ansible_module.fail_json(msg="missing python library: psycopg2")

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
            password=ansible_module.params['password'],
            async=True
        )
        wait_select(connection)
        cursor = connection.cursor()
        for file in ansible_module.params['files_reference']:
            with open(file, 'r') as file_handle:
                queries = file_handle.read()
                for query in queries.split(";"):
                    clean_query = query.strip()
                    if clean_query:
                        result['sql_queries'].append(clean_query)
                        cursor.execute(clean_query)
                        wait_select(cursor.connection)

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
