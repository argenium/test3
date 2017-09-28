#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
from pyhive import hive

DOCUMENTATION = '''
---
module: hive

short_description: Run a sql file in Hive

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
    file:
        description:
            - File containing ; separated sql queries
        required: true

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
    file: "/opt/guavus/carereflex/srx-data/schemas/hive/test.sql"
'''

RETURN = '''
sql_queries:
    description: List of queries that were ran
    type: list
'''


def run_module():
    module_args = dict(
        host=dict(required=True),
        port=dict(required=False, default=10000),
        database=dict(required=False, default='default'),
        file=dict(required=True)
    )

    result = dict(
        changed=False,
        sql_queries=[]
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if module.check_mode:
        return result

    cursor = None
    try:
        cursor = hive.connect(host=module.params['host'],
                              port=module.params['port'],
                              database=module.params['database']).cursor()
        with open(module.params['file'], 'r') as file_handle:
            queries = file_handle.read()
            for query in queries.split(";"):
                clean_query = query.strip()
                if clean_query:
                    result['sql_queries'].append(clean_query)
                    cursor.execute(clean_query)

        result['changed'] = True
    except Exception as e:
        module.fail_json(msg=str(e))
    finally:
        if cursor:
            cursor.close()
    module.exit_json(**result)


def main():
    run_module()

if __name__ == '__main__':
    main()
