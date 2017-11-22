#!/usr/bin/env python

from ansible.module_utils.basic import AnsibleModule
import sys
import subprocess

def main():
    # Parsing argument file
    module = AnsibleModule(
        argument_spec = dict(
            name = dict(required=True)
        )
    )
    name = module.params.get('name')

    try:
        """Return True if service is running"""
        cmd = '/bin/systemctl status %s.service' % name
        proc = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
        stdout_list = proc.communicate()[0].split('\n')
        for line in stdout_list:
            if 'Active:' in line:
                if '(running)' in line:
                    pid = stdout_list[3].split(' ')[3]
                    ret_msg = 'Service {0} is Active and running with pid {1}'.format(name, pid)
                    module.exit_json(msg=ret_msg, process_id=pid)
                else: 
                    ret_msg = 'Service {0} is stopped, inactive, dead or failed'.format(name)
                    module.fail_json(msg=ret_msg)
    except Exception as e:
        module.fail_json(msg=str(e))
    
if __name__ == "__main__":
    main()
