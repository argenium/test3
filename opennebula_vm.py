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
  vm_template:
    description: 
      - A valid centos template name in opennebula
    default: centos72-S-lb-v1
    required: false
  vm_name:
    description:
      - A unique name for vm.
    required: true
  vm_start_on_hold:
    description:
      - if True or true vm will be launched on hold.
      - if False or false , vm will be launched on pending. 
    default: False
    choices:
      - True
      - true
      - False
      - false

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
    vm_template: 'centos72-S-mgt-v1'
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

- name: Create vm on hold | Create Virtualip
  opennebula_vm:
    state: 'present'
    server_url: 'http://10.70.14.100:2633/RPC2'
    vm_name: 'devops-test-1'
    vm_template: "centos72-S-lb-v1"
    vm_start_on_hold : 'True'
    username: "oneadmin"
    password: "oneadmin"

- name: Create Multiple VMs
  opennebula_vm:
    state: "present"
    server_url: 'http://opennebula-host:2633/RPC2'
    vm_name: '{{ item.name }}'
    vm_template: '{{ item.template }}'
    username: "oneuser"
    password: "oneuser"
  with_items:
    - name: devops005-mgt-01
      template: centos72-M-mgt-v1
    - name: devops005-mst-01
      template: centos72-M-mst-v1
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

def get_one_user_info(url,auth):
    one_server = ServerProxy(url)
    try:
        one_user_info =  one_server.one.user.info(auth,-1)
        return one_user_info
    except Exception as e:
        return str(e)

# Function to get VM id by name of the vm
def vm_pool_get_id_by_name(url,auth,vm_name):
    one_server = ServerProxy(url)
    try:
        vm_pool_info = one_server.one.vmpool.info(auth,-3,-1,-1,-1)
    except Exception as e:
        return str(e)
    if vm_pool_info[1] != '<VM_POOL></VM_POOL>': 
        vm_pool_info_dict = xmltodict.parse(vm_pool_info[1])
        # pretty json below
        #vm_pool_info_json = json.dumps(vm_pool_info_dict, indent=2)
        #vm_jsonObj = json.loads(vm_pool_info_json)

        if isinstance(vm_pool_info_dict['VM_POOL']['VM'], dict):
            if 'ID' not in vm_pool_info_dict['VM_POOL']['VM']:
                raise ValueError("ID not present in xml-rpc response")
            else:
                if vm_pool_info_dict['VM_POOL']['VM']['NAME'] == vm_name:
                    id = vm_pool_info_dict['VM_POOL']['VM']['ID']
                    return int(id)
                else:
                    pass 

        elif isinstance(vm_pool_info_dict['VM_POOL']['VM'], list):
            for index,item in enumerate(vm_pool_info_dict['VM_POOL']['VM']):
                if 'NAME' not in item:
                    raise ValueError("no NAME key in xml-rpc template_pool response")
                elif item["NAME"] == vm_name:
                    id = vm_pool_info_dict['VM_POOL']['VM'][index]['ID']
                    return int(id)
                elif item["NAME"] != vm_name:
                    pass
    else:
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

def vm_create_on_hold(url,auth,template_name,vm_name):
    one_server = ServerProxy(url)
    template_id = template_pool_get_id_by_name(url,auth,template_name)
    try:
        vm_status = one_server.one.template.instantiate(auth,template_id,vm_name,True,'',False)
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
        "vm_start_on_hold": {"required": False, "default": "False", "choices": ['True','False','true','false']},  
        "vm_template": {"required": False, "default": "centos72-S-lb-v1", "type": "str"},
        "username": {"required": True, "type": "str"},
        "password": {"required": True, "type": "str"},
        "timeout": {"required": False, "default": "20", "type": "int"},
        }

    module = AnsibleModule(argument_spec,supports_check_mode=True)

    changed = False
    # Get all module params
    # State of vm
    state = module.params['state']
    # Vm spec
    vm_name =  module.params['vm_name']
    vm_template = module.params['vm_template']
    vm_hold = module.params['vm_start_on_hold'].title()
    # Openneubla frontend
    server_url = module.params['server_url']
    username = module.params['username']
    password = module.params['password']
    timeout = module.params['timeout']
    # Create Variables based on module params
    one_auth = '{0}:{1}'.format(username, password)
    
    one_vm_states = {
        "INIT"           : 0,
        "PENDING"        : 1,
        "HOLD"           : 2,
        "ACTIVE"         : 3,
        "STOPPED"        : 4,
        "SUSPENDED"      : 5,
        "DONE"           : 6,
        "POWEROFF"       : 8,
        "UNDEPLOYED"     : 9,
        "CLONING"        : 10,
        "CLONING_FAILURE": 11
    }

    if not HAS_XMLTODICT:
        module.fail_json(msg="xmltodict module is not installed , use pip install xmltodict")

    if not HAS_XMLRPCLIB:
        module.fail_json(msg="xmlrpclib module is not installed , use pip install xmlrpclib")

    if module.check_mode:
        # psuedo check_mode
        module.exit_json(changed=False)
    
    # Validate user 
    user_info = get_one_user_info(server_url,one_auth) 
    if user_info[0] == False:
        err_msg = '{0}{1}'.format(user_info[1],'Username or password incorrect')
        module.fail_json(msg=err_msg)
    
    elif user_info[0] == True:
        pass
      
    if state == "present" and vm_hold == "False":
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name):
            if vm_get_state_by_name(server_url,one_auth,vm_name) != one_vm_states["ACTIVE"]:
                module.fail_json(msg="The VM is not in good state please check opennebula UI")
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='PRESENT')
        else:
            vm_status = vm_create(server_url,one_auth,vm_template,vm_name)
            count = 0
            while (count <= timeout):
              time.sleep(1)
              vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
              if vm_state == one_vm_states["ACTIVE"] and count < timeout :
                  module.exit_json(changed=True , vm_created=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='CREATED')
                  break
              elif vm_state != one_vm_states["ACTIVE"] and count == timeout:
                  vm_state_text =  one_vm_states.keys()[one_vm_states.values().index(vm_state)]
                  print_msg = '{0} {1}'.format('The VM action timed out please check opennebula UI,current VM state is',vm_state_text)
                  module.fail_json(msg=print_msg)
                  break
              count += 1

    if state == "present" and vm_hold == "True":
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name):
            if vm_get_state_by_name(server_url,one_auth,vm_name) != one_vm_states['HOLD']:
                module.fail_json(msg="The VM is not in good state please check opennebula UI")
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='PRESENT')
        else:
            vm_status = vm_create_on_hold(server_url,one_auth,vm_template,vm_name)
            count = 0
            while (count <= timeout):
              time.sleep(1)
              vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
              if vm_state == one_vm_states['HOLD'] and count < timeout :
                  module.exit_json(changed=True , vm_created=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='CREATED')
                  break
              elif vm_state != one_vm_states['HOLD'] and count == timeout:
                  vm_state_text =  one_vm_states.keys()[one_vm_states.values().index(vm_state)]
                  print_msg = '{0} {1}'.format('The VM action timed out please check opennebula UI,current VM state is',vm_state_text)
                  module.fail_json(msg=print_msg)
                  break
              count += 1
   

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

            if vm_get_state_by_name(server_url,one_auth,vm_name) == one_vm_states['HOLD']:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='HOLD')

            elif vm_get_state_by_name(server_url,one_auth,vm_name) != one_vm_states['STOPPED']:
                vm_status = vm_stop(server_url,one_auth,vm_name)
                count = 0
                while (count <= timeout):
                  time.sleep(1) 
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
                  if vm_status[0] == True and vm_state == one_vm_states['STOPPED'] and count < timeout:
                      module.exit_json(changed=True, vm_stopped=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='STOPPED')
                      break
                  elif vm_state != one_vm_states['STOPPED'] and count == timeout:
                      vm_state_text =  one_vm_states.keys()[one_vm_states.values().index(vm_state)]
                      print_msg = '{0} {1}'.format('The VM action timed out please check opennebula UI,current VM state is',vm_state_text)
                      module.fail_json(msg=print_msg)
                      break
                  count += 1

            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='STOPPED')
        else:
            module.fail_json(msg="VM does not exist in opennebula")

    if state in ["started", "start"]:
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:

            if vm_get_state_by_name(server_url,one_auth,vm_name) == one_vm_states['HOLD']:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='HOLD')

            elif vm_get_state_by_name(server_url,one_auth,vm_name) != one_vm_states['ACTIVE']:
                vm_status = vm_start(server_url,one_auth,vm_name)
                count = 0
                while (count <= timeout):
                  time.sleep(1)
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)  
                  if vm_status[0] == True and vm_state == one_vm_states['ACTIVE'] and count <= timeout:
                      module.exit_json(changed=True, vm_deleted=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='STARTED')
                      break
                  elif vm_state != one_vm_states['ACTIVE'] and count == timeout:
                      vm_state_text =  one_vm_states.keys()[one_vm_states.values().index(vm_state)]
                      print_msg = '{0} {1}'.format('The VM action timed out please check opennebula UI,current VM state is',vm_state_text)
                      module.fail_json(msg=print_msg)
                      break
                  count += 1
 
            else:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='STARTED')
        else:
            module.fail_json(msg="VM does not exist in opennebula")
     
    if state in ["restarted", "restart", "reboot"]:
        if vm_pool_get_id_by_name(server_url,one_auth,vm_name) != None:

            if vm_get_state_by_name(server_url,one_auth,vm_name) == one_vm_states['HOLD']:
                module.exit_json(changed=False, vm_name=vm_name, vm_state='HOLD')
     
            elif vm_get_state_by_name(server_url,one_auth,vm_name) == one_vm_states['ACTIVE']:
                vm_status = vm_reboot(server_url,one_auth,vm_name)
                count = 0
                while (count <= timeout):
                  vm_state = vm_get_state_by_name(server_url,one_auth,vm_name)
                  if vm_status[0] == True and vm_state == one_vm_states['ACTIVE'] and count <= timeout:
                      module.exit_json(changed=True, vm_restarted=vm_status[0], vm_name=vm_name, vm_id=vm_status[1], vm_state='RESTARTED')
                  elif vm_state != one_vm_states['ACTIVE'] and count == timeout:
                      vm_state_text =  one_vm_states.keys()[one_vm_states.values().index(vm_state)]
                      print_msg = '{0} {1}'.format('The VM action timed out please check opennebula UI,current VM state is',vm_state_text)
                      module.fail_json(msg=print_msg)
                  count += 1
 
            else:
                module.fail_json(msg="VM not in valid state , it should be ACTIVE(3)")
        else:
            module.fail_json(msg="VM does not exist in opennebula")

# import module snippets
from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()
