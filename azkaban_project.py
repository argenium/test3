#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
from os.path import basename

try:
    import pendulum
    HAS_LIB_PENDULUM = True
except ImportError:
    HAS_LIB_PENDULUM = False

try:
    import requests
    HAS_LIB_REQUEST = True
except ImportError:
    HAS_LIB_REQUEST = False

try:
    import json
    HAS_LIB_JSON = True
except ImportError:
    HAS_LIB_JSON = False

DOCUMENTATION = '''
---
module: azkaban_project

short_description: Azkaban project API calls

description:
    - "Azkaban project API calls"

options:
    url:
        description:
            - Azkaban URL.
        required: true
    user:
        description:
            - Azkaban user.
        required: true
    password:
        description:
            - Azkaban password.
        required: true
    project_name:
        description:
            - Azkaban project name.
        required: true
    project_description:
        description:
            - Azkaban project description.
        required: false
    project_file:
        description:
            - Azkaban project zip file.
        required: false
    flow_name:
        description:
            - Execute this Azkaban flow.
            This option is mutually exclusive with C('execution_id').
        required: false
    execution_id:
        description:
            - Get the status of this Azkaban execution id.
            This option is mutually exclusive with C('flow_name').
        required: false
    op:
        description:
            - Operation.
        default: get
        required: false
        choices: ['get', 'create', 'delete', 'upload']
    module_fail:
        description:
            - If True, this module will fail if the status of an execution is FAILED or KILLED.
            This option is mutually exclusive with C('flow_name') and C('project').
        required: false

requirements:
  - "python >= 2.7"
  - "requests >= 2.11.1"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
- name: List the flows in project srx-data
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data

- name: Execute exec_subscriber_history
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    flow_name: exec_subscriber_history

- name: Check the status of an execution
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    execution_id: 481
    
- name: Create srx-data
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    project_description: srx-data jobs
    op: create
    
- name: Delete srx-data
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    op: delete
    
- name: Upload project file to srx-data
  azkaban_project:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    project_file: /tmp/test.zip
    op: upload
'''

RETURN = '''
json:
    description: Azkaban API call result
    type: dict
'''


def authenticate(session, url, user, password):
    try:
        return session.post(url + "/?action=login",
                            data="username={user}&password={password}".format(
                                user=user,
                                password=password)).json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to authenticate {}. Error: {}'.format(url, str(e)))


def fetch_project_flows(session, url, project):
    try:
        return session.get(url + "/manager?ajax=fetchprojectflows",
                           params={"project": project}).json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to fetch project {} flows. Error: {}'.format(project, str(e)))


def _fetch_flow_update(session, url, execution_id, module_fail):
    try:
        flow_update = session.get(url + "/executor?ajax=fetchexecflowupdate",
                                  params={"execid": execution_id,
                                          "lastUpdateTime": -1}).json()
        if (flow_update['status'] == "FAILED" or flow_update['status'] == "KILLED") and module_fail:
            raise Exception('At least one job failed or was killed {}'.format(flow_update))
        return flow_update
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to fetch update for execution_id {}. Error: {}'.format(execution_id, str(e)))


def _execute_flow(session, url, project, flow_name):
    try:
        return session.get(url + "/executor?ajax=executeFlow",
                           params={"project": project,
                                   "flow": flow_name}).json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to execute {}. Error: {}'.format(flow_name, str(e)))


def _create_project(session, url, name, description):
    try:
        create_project = session.post(url + "/manager?action=create",
                                      params={"name": name,
                                              "description": description}).json()
        if create_project['status'] != 'success':
            raise Exception('Failed to create project {}. Error: {}'.format(name, json.dumps(create_project)))
        return create_project
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to create project {}. Error: {}'.format(name, str(e)))


# Does not return anything
def _delete_project(session, url, name):
    try:
        session.get(url + "/manager?delete=true", params={"project": name})
    except requests.exceptions.RequestException as e:
        raise Exception('Failed to delete project {}. Error: {}'.format(name, str(e)))


def _upload_project(session, url, project, file_name):
    session.headers.clear()
    files = {'file': (basename(file_name), open(file_name, 'rb'), "application/zip")}
    try:
        upload_project = session.post(url + "/manager", params={
            'session.id': session.cookies.get('azkaban.browser.session.id'),
            'ajax': 'upload',
            'project': project},
                                      files=files).json()
        if 'error' in upload_project:
            Exception('Failed to upload file {}. Error: {}'.format(basename(file_name), upload_project['error']))
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to upload file {}. Error: {}'.format(basename(file_name), str(e)))


def run_module():
    module_args = dict(
        url=dict(type='str', required=True),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True),
        project_name=dict(type='str', required=True),
        project_description=dict(type='str', required=False, default=None),
        project_file=dict(type='str', required=False, default=None),
        flow_name=dict(type='str', required=False, default=None),
        execution_id=dict(type='int', required=False, default=None),
        op=dict(type='str', required=False, default='get', choices=['get', 'create', 'delete', 'upload']),
        module_fail=dict(type='bool', required=False, default=True)
    )

    result = dict(
        changed=False,
        json={}
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        mutually_exclusive=[('execution_id', 'flow_name')],
        required_if=[
            ['op', 'create', ['project_description']],
            ['op', 'upload', ['project_file']]
        ],
        supports_check_mode=True
    )

    if not HAS_LIB_PENDULUM:
        ansible_module.fail_json(msg="missing python library: pendulum")

    if not HAS_LIB_REQUEST:
        ansible_module.fail_json(msg="missing python library: request")

    if not HAS_LIB_JSON:
        ansible_module.fail_json(msg="missing python library: json")

    if ansible_module.check_mode:
        return result

    session = requests.Session()
    try:
        session = requests.Session()
        session.headers.update(
            {"Content-Type": "application/x-www-form-urlencoded",
             "X-Requested-With": "XMLHttpRequest"})
        authenticate(session=session, url=ansible_module.params['url'],
                     user=ansible_module.params['user'], password=ansible_module.params['password'])
        if ansible_module.params['execution_id']:
            result['json'] = _fetch_flow_update(session=session,
                                                url=ansible_module.params['url'],
                                                execution_id=ansible_module.params['execution_id'],
                                                module_fail=ansible_module.params['module_fail'])
        elif ansible_module.params['flow_name']:
            result['json'] = _execute_flow(session=session,
                                           url=ansible_module.params['url'],
                                           project=ansible_module.params['project_name'],
                                           flow_name=ansible_module.params['flow_name'])
        else:
            if ansible_module.params['op'] == 'create':
                result['json'] = _create_project(session=session,
                                                 url=ansible_module.params['url'],
                                                 name=ansible_module.params['project_name'],
                                                 description=ansible_module.params['project_description'])
            elif ansible_module.params['op'] == 'delete':
                _delete_project(session=session,
                                url=ansible_module.params['url'],
                                name=ansible_module.params['project_name'])
            elif ansible_module.params['op'] == 'upload':
                result['json'] = _upload_project(session=session,
                                                 url=ansible_module.params['url'],
                                                 project=ansible_module.params['project_name'],
                                                 file_name=ansible_module.params['project_file'])
            else:
                project_flows = fetch_project_flows(session=session,
                                                    url=ansible_module.params['url'],
                                                    project=ansible_module.params['project_name'])
                flows = []
                for flow in project_flows['flows']:
                    flows.append(flow['flowId'])

                result['json'] = flows

        result['changed'] = True
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        ansible_module.fail_json(msg=str(e), changed=True)
    finally:
        if session:
            session.close()

    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
