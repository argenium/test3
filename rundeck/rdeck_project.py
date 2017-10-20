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
module: rdeck_project

short_description: Manage rundeck project creation and deletion.

version_added: "2.2.0.0"

description:
     - Create, delete , update rundeck projects using api

options:
  state: 
    description:
      - when state is present -> create rundeck project
      - when state is absent -> delete rundeck project
      - when state is latest -> delete and recreate rundeck project 
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
  project_name: 
    description:
      - project name to create
    required: true
  api_version:
    description: 
       - rundeck api version 
    default: 21
    required: false
  project_definition: 
    description:
       - project definition in json format 
    required: True     
  project_format:
    description: 
     - currently this module only support json format
    required: True
    default: json
  token:
    description
      - api access token provided by rundeck server
    required: True
requirements:
  - "python >= 2.7"
  - "requests == 2.18.4"

author:
  - onkar.kadam@guavus.com
'''

EXAMPLES = '''

- name: Create Project
  rdeck_project:
    state: present
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project_name: "TEST"
    api_version: 21
    project_definition: '{{ project_definition_hash | to_nice_json(indent=2) }}'
    project_format: json
    token: "tZe4sOnzasasasaewfewffdSDFas"


- name: Delete Project
  rdeck_project:
    state: absent
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project_name: "TEST"
    api_version: 21
    project_definition: '{{ project_definition_hash | to_nice_json(indent=2) }}'
    project_format: json
    token: "tZe4sOnzasasasaewfewffdSDFas"

- name: Update Project
  rdeck_project:
    state: latest
    url: "http://devops007-mgt-01.devops.guavus.mtl:5550"
    project_name: "TEST"
    api_version: 21
    project_definition: '{{ project_definition_hash | to_nice_json(indent=2) }}'
    project_format: json
    token: "tZe4sOnzasasasaewfewffdSDFas"
'''


from ansible.module_utils.basic import *
import json

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

def rundeck_user_validation(url,api_version,headers,module):
    api_user_info_url = '{0}/api/{1}/user/info/'.format(url,api_version)
    try:
        r = requests.get(api_user_info_url, headers=headers)
        if r.status_code == requests.codes.ok:
            pass
        else:
            err_msg = 'return status code {0}'.format(r.status_code)
            response = r.json()
            module.fail_json(msg=err_msg, meta=response)
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_create_project(url,api_version,headers,project_name,project_definition,project_format,module):
    # api urls 
    api_check_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project_name)
    api_create_project_url = '{0}/api/{1}/projects'.format(url,api_version)
    api_project_resource_url = '{0}/api/{1}/projects/resources'.format(url,api_version)

    try:
        p_chk = requests.get(api_check_project_url, headers=headers)
        if p_chk.status_code == requests.codes.not_found:
            p_create = requests.post(api_create_project_url, headers=headers, data=project_definition)
            if p_create.status_code == requests.codes.ok or p_create.status_code == requests.codes.created:
                p_create_resp = p_create.json()
                requests.get(api_project_resource_url, headers=headers)
                module.exit_json(changed=True, meta=p_create_resp)
            else:
                err_msg = 'return status code {0}'.format(p_create.status_code)
                p_create_resp = p_create.json()
                module.fail_json(msg=err_msg, meta=p_create_resp)
        elif p_chk.status_code == requests.codes.ok:
            p_chk_resp = p_chk.json()
            module.exit_json(changed=False, meta=p_chk_resp)
        else:
            err_msg = 'return status code {0}'.format(p_chk.status_code)
            p_chk_resp = p_chk.json()
            module.fail_json(msg=err_msg, meta=p_chk_resp)
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_delete_project(url,api_version,headers,project_name,module):
    # api_urls
    api_check_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project_name)
    api_delete_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project_name)
    
    try:
        p_chk = requests.get(api_check_project_url, headers=headers)
        if p_chk.status_code == requests.codes.not_found:
            module.exit_json(changed=False, project_name=project_name, state="absent")
        if p_chk.status_code == requests.codes.ok and isinstance(p_chk.json(),dict):
            p_delete = requests.delete(api_delete_project_url, headers=headers)
            if p_delete.status_code == requests.codes.no_content:
                module.exit_json(changed=True, project_name=project_name, state="absent")
            else:
                err_msg = 'return status code {0}'.format(p_delete.status_code)
                module.fail_json(msg=err_msg)
        else:
            err_msg = 'return status code {0}'.format(p_chk.status_code)
            p_chk_resp = p_chk.json()
            module.fail_json(msg=err_msg, meta=p_chk_resp)
    except Exception as e:
        module.fail_json(msg=str(e))

def rundeck_update_project(url,api_version,headers,project_name,project_definition,project_format,module):
    #simply delete and re-create
    api_check_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project_name)
    api_create_project_url = '{0}/api/{1}/projects'.format(url,api_version)
    api_delete_project_url = '{0}/api/{1}/project/{2}'.format(url,api_version,project_name)
    api_project_resource_url = '{0}/api/{1}/projects/resources'.format(url,api_version)
    
    try:
        p_chk = requests.get(api_check_project_url, headers=headers)
        if (p_chk.status_code == requests.codes.ok) and (isinstance(p_chk.json(),dict)):
            p_delete = requests.delete(api_delete_project_url, headers=headers)
            if p_delete.status_code == requests.codes.no_content:
                p_create = requests.post(api_create_project_url, headers=headers, data=project_definition)
                if p_create.status_code == requests.codes.ok or p_create.status_code == requests.codes.created:
                    p_create_resp = p_create.json()
                    requests.get(api_project_resource_url, headers=headers)
                    module.exit_json(changed=True, meta=p_create_resp)
            else:
                err_msg = 'return status code {0}'.format(p_create.status_code)
                p_create_resp = p_create.json()
                module.fail_json(msg=err_msg, meta=p_create_resp)
        elif p_chk.status_code == requests.codes.not_found:
            p_create = requests.post(api_create_project_url, headers=headers, data=project_definition)
            if p_create.status_code == requests.codes.ok:
                p_create_resp = p_create.json()
                requests.get(api_project_resource_url, headers=headers)
                module.exit_json(changed=True, meta=p_create_resp)
            else:
                err_msg = 'return status code {0}'.format(p_create.status_code)
                p_create_resp = p_create.json()
                module.fail_json(msg=err_msg, meta=p_create_resp)
        else:
            err_msg = 'return status code {0}'.format(p_chk.status_code)
            p_chk_resp = p_chk.json()
            module.fail_json(msg=err_msg, meta=p_chk_resp)
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    argument_spec = {
        "state": {"default": "present", "choices": ['present', 'absent', 'latest']},
        "url": {"required": True, "type": "str"},
        "project_name": {"required": True, "type": "str"},
        "api_version": {"default": "21", "type": "int"},
        "project_definition": {"required": True, "type": "json"},
        "project_format": {"required": True, "type": "str" , "choices": ['json']},
        "token": {"required": True, "type": "str", "no_log": True},
        }

    module = AnsibleModule(argument_spec,supports_check_mode=True)
    changed = False
    # Get all module params
    state = module.params['state']
    url = module.params['url']
    api_version = module.params['api_version']
    project_name = module.params['project_name']
    project_definition = module.params['project_definition']
    project_format = module.params['project_format']
    token = module.params['token']
    project_content_type = 'application/{type}'.format(type=project_format)

    headers = { "Content-Type": project_content_type,
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

    if state == 'present':
        rundeck_create_project(url,api_version,headers,project_name,project_definition,project_format,module)

    elif state == 'absent':
        rundeck_delete_project(url,api_version,headers,project_name,module)

    elif state == 'latest':
        rundeck_update_project(url,api_version,headers,project_name,project_definition,project_format,module)

# main
if __name__ == '__main__':
    main()
