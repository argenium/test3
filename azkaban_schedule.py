#!/usr/bin/python

# Copyright (c) 2017 [Guavus]
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from ansible.module_utils.basic import AnsibleModule

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

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: azkaban_schedule

short_description: Azkaban schedule API calls

description:
    - "Azkaban schedule API calls"

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
    flow_name:
        description:
            - Azkaban flow name.
        required: true
    schedule_name:
        description:
            - Azkaban schedule name.
        required: true
    schedule_period:
        description:
            - Specifies the recursion period.
        required: false
    schedule_time:
        description:
            - The time to schedule the flow.
        required: false
    op:
        description:
            - Operation.
        default: get
        required: false
        choices: ['create', 'delete', 'get']

requirements:
  - "python >= 2.7"
  - "requests >= 2.11.1"
  - "pendulum > 1.0.0"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
---
- name: Get schedule
  azkaban_schedule:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    flow_name: exec_subscriber_history
    schedule_name: exec_subscriber_history
    
- name: Create a schedule
  azkaban_schedule:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    flow_name: exec_subscriber_history
    schedule_name: exec_subscriber_history
    schedule_period: 1d
    schedule_time: '00,00,am'
    op: create

- name: Delete a schedule
  azkaban_schedule:
    url: http://data018-vip-01.devops.guavus.mtl:8507
    user: azkaban
    password: azkaban
    project_name: srx-data
    flow_name: exec_subscriber_history
    schedule_name: exec_subscriber_history
    op: delete
    
'''

RETURN = '''
---
json:
    description: Azkaban schedule API call result
    type: dict
params:
    description: Parameters passed to this module
    returned: failure
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


def _fetch_schedule(session, url, project_id, flow_name, schedule_name):
    try:
        return session.get(url + "/schedule?ajax=fetchSchedule",
                           params={"projectId": project_id,
                                   "flowId": flow_name,
                                   "scheduleName": schedule_name}).json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to fetch schedule {}. Error: {}'.format(schedule_name, str(e)))


def _delete_schedule(session, url, schedule_id):
    try:
        return session.post(url + "/schedule?action=removeSched",
                            params={"scheduleId": schedule_id}).json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to delete schedule {}. Error: {}'.format(schedule_id, str(e)))


def _create_schedule(session, url, project_name, project_id, flow_name,
                     schedule_name, schedule_time, schedule_period):
    try:
        create_schedule = session.post(url + "/schedule?ajax=scheduleFlow",
                                       params={"is_recurring": "on",
                                               "period": schedule_period,
                                               "projectName": project_name,
                                               "flow": flow_name,
                                               "projectId": project_id,
                                               "scheduleTime": schedule_time + ',UTC',
                                               "scheduleDate": pendulum.now().format('%m/%d/%Y'),
                                               "scheduleName": schedule_name}).json()
        if create_schedule['status'] != 'success':
            raise Exception('Failed to create schedule {}. Error: {}'.format(schedule_name,
                                                                             json.dumps(create_schedule)))
        return create_schedule
    except (requests.exceptions.RequestException, ValueError) as e:
        raise Exception('Failed to create schedule {}. Error: {}'.format(schedule_name, str(e)))


def run_module():
    module_args = dict(
        url=dict(type='str', required=True),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True),
        project_name=dict(type='str', required=True),
        flow_name=dict(type='str', required=True),
        schedule_name=dict(type='str', required=True),
        schedule_period=dict(type='str', required=False, default=None),
        schedule_time=dict(type='str', required=False, default=None),
        op=dict(type='str', required=False, default='get', choices=['create', 'delete', 'get']),
    )

    result = dict(
        changed=False,
        json={}
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        required_if=[
            ['op', 'create', ['schedule_period', 'schedule_time']],
        ],
        required_together=[['schedule_period', 'schedule_time']],
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

        project_flows = fetch_project_flows(session=session,
                                            url=ansible_module.params['url'],
                                            project=ansible_module.params['project_name'])

        if ansible_module.params['op'] == 'create':
            result['json'] = _create_schedule(session=session,
                                              url=ansible_module.params['url'],
                                              project_name=ansible_module.params['project_name'],
                                              project_id=project_flows['projectId'],
                                              flow_name=ansible_module.params['flow_name'],
                                              schedule_name=ansible_module.params['schedule_name'],
                                              schedule_time=ansible_module.params['schedule_time'],
                                              schedule_period=ansible_module.params['schedule_period'])
        elif ansible_module.params['op'] == 'delete':
            schedule_info = _fetch_schedule(session=session,
                                            url=ansible_module.params['url'],
                                            project_id=project_flows['projectId'],
                                            flow_name=ansible_module.params['flow_name'],
                                            schedule_name=ansible_module.params['schedule_name'])
            result['json'] = _delete_schedule(session=session,
                                              url=ansible_module.params['url'],
                                              schedule_id=schedule_info['schedule']['scheduleId'])
        else:
            result['json'] = _fetch_schedule(session=session,
                                             url=ansible_module.params['url'],
                                             project_id=project_flows['projectId'],
                                             flow_name=ansible_module.params['flow_name'],
                                             schedule_name=ansible_module.params['schedule_name'])

        result['changed'] = True
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        ansible_module.fail_json(msg=str(e), changed=True, params=ansible_module.params)
    finally:
        if session:
            session.close()

    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
