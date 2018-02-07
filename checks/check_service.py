#!/usr/bin/env python

# Copyright (c) 2017 [Guavus]
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from ansible.module_utils.basic import AnsibleModule
import subprocess

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: check_service

short_description: check service

description:
    - "check service"

options:
    name:
        description:
            - Service name.
        required: true

requirements:
  - "python >= 2.7"

author:
    - Onkar Kadam (@onkarkadam7)
'''

EXAMPLES = '''
---
- name: Check kafka-server status   
  check_service:
    name: 'kafka-server'
'''

RETURN = '''
---
msg:
    description: message
    type: string
process_id:
    description: process id
    returned: success
    type: string
'''


def main():
    # Parsing argument file
    ansible_module = AnsibleModule(
        argument_spec=dict(
            name=dict(required=True)
        )
    )
    name = ansible_module.params.get('name')

    try:
        """Return True if service is running"""
        cmd = '/bin/systemctl status %s.service' % name
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        stdout_list = proc.communicate()[0].split('\n')
        for line in stdout_list:
            if 'Active:' in line:
                if '(running)' in line:
                    pid = stdout_list[3].split(' ')[3]
                    ret_msg = 'Service {0} is Active and running with pid {1}'.format(name, pid)
                    ansible_module.exit_json(msg=ret_msg, process_id=pid)
                else: 
                    ret_msg = 'Service {0} is stopped, inactive, dead or failed'.format(name)
                    ansible_module.fail_json(msg=ret_msg)
    except Exception as e:
        ansible_module.fail_json(msg=str(e))


if __name__ == "__main__":
    main()
