#!/usr/bin/python
#
# Copyright 2017 Guavus - A Thales company
#
# This file is part of Guavus Infrastructure using Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'guavus-devops'}

DOCUMENTATION = '''
---
module: rdeck_job_import

short_description: Manage rundeck job creation and deletion.

version_added: "2.2.0.0"

description:
     - Create, delete , update rundeck jobs using api

options:
  state:
    description:
      - when state is present -> create/import rundeck job
      - when state is absent -> delete rundeck job
      - when state is latest -> delete and recreate/import rundeck job
    default: present
    required: false
    choices:
      - present
      - absent
      - latest
  url:
    description:
      - rundeck url in the form http://<rundeck_host>:<port>
    required: true
  project:
    description:
      - project  in wich job will be created
    required: true
  project_name:
    description:
      - project name to create
    required: true
  api_version:
    description:
      - rundeck api version
    default: 21
    required: false
  job_definition:
    description:
      - job definition in yaml/xml format
    required: True
  job_format:
    description:
      - yaml or xml support
    required: True
    choices:
      - yaml
      - xml
  token:
    description
      - api access token provided by rundeck server
    required: True

requirements:
  - "python >= 2.7"
  - "requests >= 2.13.0"

author:
  - onkar.kadam@guavus.com
'''

EXAMPLES = '''

- name: Create job
  rdeck_job_import:
    state: present
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project: "TEST"
    job_name: "TEST"
    api_version: 21
    job_definition: "{{ lookup('template', 'test.yaml.j2') }}"
    job_format: 'yaml'
    token: "tZe4sOnz4hk9ZEQBs4OmtJO3g5ahW4eR"

- name: Delete job
  rdeck_job_import:
    state: absent
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project: "TEST"
    job_name: "TEST"
    api_version: 21
    job_definition: "{{ lookup('template', 'test.yaml.j2') }}"
    job_format: 'yaml'
    token: "tZe4sOnz4hk9ZEQBs4OmtJO3g5ahW4eR"

- name: Update job
  rdeck_job_import:
    state: latest
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project: "TEST"
    job_name: "TEST"
    api_version: 21
    job_definition: "{{ lookup('file', 'test.yaml') }}"
    job_format: 'yaml'
    token: "tZe4sOnz4hk9ZEQBs4OmtJO3g5ahW4eR"
'''

from ansible.module_utils.basic import *
import json

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

def rundeck_user_validation(url,api_version,headers,module):
    api_system_info_url = '{0}/api/{1}/system/info/'.format(url,api_version)
    try:
        r = requests.get(api_system_info_url, headers=headers)
        if r.status_code == requests.codes.ok: 
            pass
        else:
            err_msg = 'return status code {0}'.format(r.status_code)
            response = r.json()
            module.fail_json(msg=err_msg, meta=response)
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_project_validation(url,api_version,headers,project,module):
    api_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project)
    try:
        r = requests.get(api_project_url, headers=headers)
        if r.status_code == requests.codes.ok:
            pass
        else:
            err_msg = 'return status code {0}'.format(r.status_code)
            response = r.json()
            module.fail_json(msg=err_msg, meta=response)
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_create_job(url,api_version,headers,project,job_name,job_definition,job_format,module):
    # api urls 
    api_job_create_url = '{0}/api/{1}/project/{2}/jobs/import?fileformat={3}&dupeOption=update&uuidOption=remove'.format(url,api_version,project,job_format)
    api_job_check_url = '{0}/api/{1}/project/{2}/jobs?jobExactFilter={3}'.format(url,api_version,project,job_name)

    try:
        jb_chk = requests.get(api_job_check_url, headers=headers)
        jb_response =  jb_chk.json()
        if not jb_response == [] and jb_response[0]['name'] == job_name:
             module.exit_json(changed=False, meta=jb_response[0])
        else:
            jb_create = requests.post(api_job_create_url, headers=headers, data=job_definition)
            jb_create_resp = jb_create.json()
            if jb_create.status_code == requests.codes.ok and not jb_create_resp['succeeded'] == []:
                module.exit_json(changed=True, meta=jb_create_resp['succeeded'])
            else:
                err_msg = 'return status code {0}'.format(jb_create.status_code)
                module.fail_json(msg=err_msg, meta=jb_create_resp['failed'])
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_delete_job(url,api_version,headers,project,job_name,module):
    # api url
    api_job_check_url = '{0}/api/{1}/project/{2}/jobs?jobExactFilter={3}'.format(url,api_version,project,job_name)

    try:
        jb_chk = requests.get(api_job_check_url, headers=headers)
        jb_response =  jb_chk.json()
        if not jb_response == [] and jb_response[0]['name'] == job_name:
            jb_id =  jb_chk.json()[0]['id']
            api_job_delete_url = '{0}/api/{1}/job/{2}'.format(url,api_version,jb_id)
            jb_del = requests.delete(api_job_delete_url, headers=headers)
            if jb_del.status_code == requests.codes.no_content:
               module.exit_json(changed=True, job_id=jb_id, job_name=job_name, state="absent")
            else:
                err_msg = 'return status code {0}'.format(jb_del.status_code)
                module.fail_json(msg=err_msg)
        else:
            module.exit_json(changed=False, job_id="null", job_name=job_name, state="absent")
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_update_job(url,api_version,headers,project,job_name,job_definition,job_format,module):
    api_job_create_url = '{0}/api/{1}/project/{2}/jobs/import?fileformat={3}&dupeOption=update&uuidOption=remove'.format(url,api_version,project,job_format)
    api_job_check_url = '{0}/api/{1}/project/{2}/jobs?jobExactFilter={3}'.format(url,api_version,project,job_name)

    try:
        jb_chk = requests.get(api_job_check_url, headers=headers)
        jb_response =  jb_chk.json()
        if not jb_response == [] and jb_response[0]['name'] == job_name:
            jb_id =  jb_chk.json()[0]['id']
            api_job_delete_url = '{0}/api/{1}/job/{2}'.format(url,api_version,jb_id)
            jb_del = requests.delete(api_job_delete_url, headers=headers)
            if jb_del.status_code == requests.codes.no_content:
                jb_create = requests.post(api_job_create_url, headers=headers, data=job_definition)
                jb_create_resp = jb_create.json()
                if jb_create.status_code == requests.codes.ok and not jb_create_resp['succeeded'] == []:
                    module.exit_json(changed=True, meta=jb_create_resp['succeeded'])
                else:
                    err_msg = 'return status code {0}'.format(jb_create.status_code)
                    jb_create_resp = jb_create.json()
                    module.fail_json(msg=err_msg, meta=jb_create_resp['failed'])
            else:
                err_msg = 'return status code {0}'.format(jb_del.status_code)
                module.fail_json(msg=err_msg)
        else:
            jb_create = requests.post(api_job_create_url, headers=headers, data=job_definition)
            jb_create_resp = jb_create.json()
            if jb_create.status_code == requests.codes.ok and not jb_create_resp['succeeded'] == []:
                module.exit_json(changed=True, meta=jb_create_resp['succeeded'])
            else:
                err_msg = 'return status code {0}'.format(jb_create.status_code)
                module.fail_json(msg=err_msg, meta=jb_create_resp['failed'])
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    argument_spec = {
        "state": {"default": "present", "choices": ['present', 'absent', 'latest']},
        "url": {"required": True, "type": "str"},
        "project": {"required": True, "type": "str"},
        "job_name": {"required": True, "type": "str"},
        "api_version": {"default": "20", "type": "int"},
        "job_definition": {"required": True, "type": "raw"},
        "job_format": {"required": True, "type": "str" , "choices": ['yaml', 'xml']},
        "token": {"required": True, "type": "str", "no_log": True}
        }

    module = AnsibleModule(argument_spec,supports_check_mode=True)
    changed = False
    # Get all module params
    state = module.params['state']
    url = module.params['url']
    api_version = module.params['api_version']
    project = module.params['project']
    job_name = module.params['job_name']
    job_definition = module.params['job_definition']
    job_format = module.params['job_format']
    token = module.params['token']
    job_content_type = 'application/{type}'.format(type=job_format)

    headers = { "Content-Type": job_content_type,
                "Accept": "application/json",
                "X-Rundeck-Auth-Token": token }

    if module.check_mode:
        # psuedo check_mode
        module.exit_json(changed=False)

    if not HAS_REQUESTS:
        module.fail_json(msg="requests module is not installed , use pip install requests")

    if module.params["api_version"] < 20:
        module.fail_json(msg="API version should be at least 20")

    # Validate user token
    rundeck_user_validation(url,api_version,headers,module)
    # Validate Project existence
    rundeck_project_validation(url,api_version,headers,project,module)

    if state == 'present':
        rundeck_create_job(url,api_version,headers,project,job_name,job_definition,job_format,module)

    elif state == 'absent':
        rundeck_delete_job(url,api_version,headers,project,job_name,module)

    elif state == 'latest':
        rundeck_update_job(url,api_version,headers,project,job_name,job_definition,job_format,module)

# main
if __name__ == '__main__':
    main()
