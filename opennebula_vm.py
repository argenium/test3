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
module: opennebula_vm

short_description: Manage opennebula virtual machines.

version_added: "2.2.0.0"

description:
     - Create, delete , start , stop or restart  opennebula virtual machine using xmlrpc opennebula api

options:
  archive_path:
    description:
      - Use with state C(present) to archive an image to a .tar file.
    default: false
    required: false 
    version_added: "2.1"
  state:
    description:
      - when state is present -> create and launch vm. 
      - when state is absent -> delete vm. 
      - when state is start or started -> launch a stopped vm. 
      - when state is stop , stopped or shutdown -> shutdown a running vm.
      - when start is restart , restarted or reboot -> reboot a vm
    default: present
    required: false   
    choices:
      - present
      - absent 
      - start
      - started 
      - stop 
      - stopped
      - shutdown 
      - restart
      - restarted
      - reboot
  server_url:
    description: 
      - opennebula frontend RPC url in the form of http://onhost.fqdn/RPC2    
    required: true
  username:
    description:
      - valid opennebula username
    required: true
  password:
    description:
      - valid opennebula username's password
    required: true
  timeout:
    description: 
      - Timeout in sec  to wait to check the status of VM.
    default: 20 
    required: false
  vm_base:
    description: 
      - A valid prefix of centos vm template , usually in the form centos7-v2
    default: centos7-v2  
    required: false
  vm_name:
    description:
      - A unique name for vm.
    required: true
  vm_type:
    description:
      - size of vm from below choices
    default: small
    choices:
      - small
      - app
      - platform
      - platform_huge
  vm_storage:
    description:
      - if storage local ==> will use hosts local storage.
      - if storage san ==> will use openebula shared storage.
    default: local
    choices:
      - san
      - local

requirements:
  - "python >= 2.7"
  - "xmltodict == 0.11.0"

author:
  - onkar.kadam@guavus.com

'''

EXAMPLES = '''


- name: Create vm
  opennebula_vm:
    state: 'present'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    vm_base: 'centos7-v2'
    vm_type: 'small'
    vm_storage: 'local'
    username: "oneadmin"
    password: "oneadmin"

- name: Delete vm
  opennebula_vm:
    state: 'absent'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    username: "oneadmin"
    password: "oneadmin"

- name: Stop vm
  opennebula_vm:
    state: 'stopped'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    username: "oneadmin"
    password: "oneadmin"

- name: Start vm
  opennebula_vm:
    state: 'started'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    username: "oneadmin"
    password: "oneadmin"

- name: Restart vm
  opennebula_vm:
    state: 'restart'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    username: "oneadmin"
    password: "oneadmin"

- name: Create Multiple VMs
  opennebula_vm:
    state: "present"
    server_url: 'http://opennebula-host:2633/RPC2'
    vm_name: '{{ item.name }}'
    vm_base: 'centos7-v2'
        vm_type: '{{ item.type }}'
        vm_storage: '{{ item.storage }}'
        username: "oneuser"
        password: "oneuser"
      with_items:
        - name: devops005-mgt-01
          type: small
          storage: local
        - name: devops005-mst-01
          type: small
          storage: san
'''

# for pretty debugging
#import json
import time 

try:
    from xmlrpclib import ServerProxy
    HAS_XMLRPCLIB = True
except ImportError:
    HAS_XMLRPCLIB = False

try:
    import xmltodict
    HAS_XMLTODICT = True
except ImportError:
    HAS_XMLTODICT = False

# Function to get VM id by name of the vm
def vm_pool_get_id_by_name(url,auth,vm_name):
    one_server = ServerProxy(url)
    try:
        vm_pool_info = one_server.one.vmpool.info(auth,-3,-1,-1,-1)
    except Exception as e:
        return str(e)
    vm_pool_info_dict = xmltodict.parse(vm_pool_info[1])
    # pretty json below
    #vm_pool_info_json = json.dumps(vm_pool_info_dict, indent=2)
    #vm_jsonObj = json.loads(vm_pool_info_json)

    if isinstance(vm_pool_info_dict['VM_POOL']['VM'], dict):
        if 'ID' not in vm_pool_info_dict['VM_POOL']['VM']:
            raise ValueError("ID not present in xml-rpc response")
        else:
            id = vm_pool_info_dict['VM_POOL']['VM']['ID']
            return int(id)

    elif isinstance(vm_pool_info_dict['VM_POOL']['VM'], list):
        for index,item in enumerate(vm_pool_info_dict['VM_POOL']['VM']):
            if 'NAME' not in item:
                raise ValueError("no NAME key in xml-rpc template_pool response")
            elif item["NAME"] == vm_name:
                 id = vm_pool_info_dict['VM_POOL']['VM'][index]['ID']
                 return int(id)
            elif item["NAME"] != vm_name:
                pass

# To get template_id from opennebula
def template_pool_get_id_by_name(url,auth,template_name):
    one_server = ServerProxy(url)
    try:
        template_pool_info = one_server.one.templatepool.info(auth,-2,-1,-1)
    except Exception as e:
        return str(e)
    template_pool_info_dict = xmltodict.parse(template_pool_info[1])

    if isinstance(template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE'], dict):
        if 'ID' not in template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE']:
            raise ValueError("ID key not present in xml-rpc response")
        else:
            id = template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE']['ID']
            return int(id)

    elif isinstance(template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE'], list):
        for index,item in enumerate(template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE']):
            if 'NAME' not in item:
                raise ValueError("no NAME key in xml-rpc template_pool response")
            elif item["NAME"] == template_name:
                id = template_pool_info_dict['VMTEMPLATE_POOL']['VMTEMPLATE'][index]['ID']
                return int(id)
            elif item["NAME"] != template_name:
                pass

def vm_get_state_by_name(url,auth,vm_name):
    one_server = ServerProxy(url)
    vm_id = vm_pool_get_id_by_name(url,auth,vm_name)
    try:
        vm_info = one_server.one.vm.info(auth,vm_id)
    except Exception as e:
        return str(e)
    vm_info_dict = xmltodict.parse(vm_info[1])
    state = vm_info_dict['VM']['STATE']
    return int(state)

def vm_create(url,auth,template_name,vm_name):
    one_server = ServerProxy(url)
    template_id = template_pool_get_id_by_name(url,auth,template_name)
    try:
        vm_status = one_server.one.template.instantiate(auth,template_id,vm_name,False,'',False)
        return vm_status
    except Exception as e:
        return str(e)

def vm_delete(url,auth,vm_name):
    one_server = ServerProxy(url)
    vm_id = vm_pool_get_id_by_name(url,auth,vm_name)
    try:
        vm_status = one_server.one.vm.action(auth,'terminate-hard',vm_id)
        return vm_status
    except Exception as e:
        return str(e)

def vm_start(url,auth,vm_name):
    one_server = ServerProxy(url)
    vm_id = vm_pool_get_id_by_name(url,auth,vm_name)
    try:
        vm_status = one_server.one.vm.action(auth,'resume',vm_id)
        return vm_status
    except Exception as e:
        return str(e)

def vm_stop(url,auth,vm_name):
    one_server = ServerProxy(url)
    vm_id = vm_pool_get_id_by_name(url,auth,vm_name)
    try: 
        vm_status = one_server.one.vm.action(auth,'stop',vm_id)
        return vm_status
    except Exception as e:
        return str(e)

def vm_reboot(url,auth,vm_name):
    one_server = ServerProxy(url)
    vm_id = vm_pool_get_id_by_name(url,auth,vm_name)
    try:
        vm_status = one_server.one.vm.action(auth,'reboot-hard',vm_id)
        return vm_status
    except Exception as e:
        return str(e)


def main():
    argument_spec = {
        "state": {"default": "present", "choices": ['present', 'absent', 'stopped', 'stop', 'started', 'start', 'restarted', 'restart', 'reboot']},
        "server_url": {"required": True, "type": "str"},
        "vm_name": {"required": True, "type": "str"},
        "vm_type": {"default": "small", "choices": ['small','app','platform','platform_huge']},
        "vm_storage": {"default": "local", "choices": ['local','san']},
        "vm_base": {"default": "centos7-v2", "type": "str"},
        "username": {"required": True, "type": "str"},
        "password": {"required": True, "type": "str"},
        "timeout": {"default": "20", "type": "int"},
        }

    module = AnsibleModule(argument_spec,supports_check_mode=True)

    changed = False
    # Get all module params
    # State of vm
    state = module.params['state']
    # Vm spec
    vm_name =  module.params['vm_name']
    vm_type = module.params['vm_type']
    vm_storage = module.params['vm_storage']
    vm_base = module.params['vm_base']
    # Openneubla frontend
    server_url = module.params['server_url']
    username = module.params['username']
    password = module.params['password']
    timeout = module.params['timeout']
    # Create Variables based on module params
    one_auth = '{0}:{1}'.format(username, password)
    #TODO : Create better way to handle this maybe provide template module param
    template_name = '{0}-{1}-{2}'.format(vm_base, vm_storage, vm_type)

    if not HAS_XMLTODICT:
        module.fail_json(msg="xmltodict module is not installed , use pip install xmltodict")

    if not HAS_XMLRPCLIB:
        module.fail_json(msg="xmlrpclib module is not installed , use pip install xmlrpclib")

    if module.check_mode:
        # psuedo check_mode
        module.exit_json(changed=False)

    if state == "present":
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name):
            if vm_get_state_by_name(server_url,one_auth,vm_name) != 3:
                module.fail_json(msg="The VM is not in good state please check opennebula UI")
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='RUNNING')
        else:
            vm_status = vm_create(server_url,one_auth,template_name,vm_name)
            count = 0
            while count <= timeout:
              time.sleep(1)
              vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
              if vm_state == 3 and count < timeout :
                  module.exit_json(changed=True , vm_created=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='CREATED')
                  break
              elif vm_state != 3 and count == timeout:
                  module.fail_json(msg="The VM Launch timed out please check opennebula UI for consistency")
                  break 

    if state == "absent":
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:
            vm_status = vm_delete(server_url,one_auth,vm_name)
            if vm_status[0] == True:
                module.exit_json(changed=True, vm_deleted=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='DELETED')
            else: 
                module.fail_json(msg=vm_status[1])
        else:
            module.exit_json(changed=False, vm_name=vm_name, vm_state='DELETED')

    if state in ["stopped", "stop", "shutdown"]:
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:
            if vm_get_state_by_name(server_url,one_auth,vm_name) != 4:
                vm_status = vm_stop(server_url,one_auth,vm_name)
                count = 0
                while count <= timeout:
                  time.sleep(1) 
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
                  if vm_status[0] == True and vm_state == 4 and count < timeout:
                      module.exit_json(changed=True, vm_stopped=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='STOPPED')
                      break
                  elif vm_state != 4 and count == timeout:
                      module.fail_json(msg="The VM Stop action timed out please check opennebula UI for consistency")
                      break 
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='STOPPED')
        else:
            module.fail_json(msg="VM does not exist in opennebula")

    if state in ["started", "start"]:
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:
            if vm_get_state_by_name(server_url,one_auth,vm_name) != 3:
                vm_status = vm_start(server_url,one_auth,vm_name)
                count = 0
                while count <= timeout:
                  time.sleep(1)
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)  
                  if vm_status[0] == True and vm_state == 3 and count <= timeout:
                      module.exit_json(changed=True, vm_deleted=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='STARTED')
                      break
                  elif vm_state != 3 and count == timeout:
                      module.fail_json(msg="The VM Start action timed out please check opennebula UI for consistency")
                      break 
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='STARTED')
        else:
            module.fail_json(msg="VM does not exist in opennebula")
     
    if state in ["restarted", "restart", "reboot"]:
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:
            if vm_get_state_by_name(server_url,one_auth,vm_name) == 3:
                vm_status = vm_reboot(server_url,one_auth,vm_name)
                count = 0
                while count <= timeout:
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
                  if vm_status[0] == True and vm_state == 3 and count <= timeout:
                      module.exit_json(changed=True, vm_restarted=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='RESTARTED')
                  elif vm_state != 3 and count == timeout:
                      module.fail_json(msg="The VM Restart action timed out please check opennebula UI for consistency")
            else:
                module.fail_json(msg="VM not in valid state , it should be ACTIVE(3)")
        else:
            module.fail_json(msg="VM does not exist in opennebula")

# import module snippets
from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()
