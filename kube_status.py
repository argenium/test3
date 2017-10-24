#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule

from kubernetes import client, config

DOCUMENTATION = '''
---
module: kube_status

short_description: Check the status of Kubernetes pods

description:
    - "Check the status of Kubernetes pods"

options:
    namespace:
        description:
            - Kubernetes namespace
        required: false
    label_selector:
        description:
            - Labels for the pods
        required: false

requirements:
  - "python >= 2.7"
  - "kubernetes >= 4.0.0a1"

author:
    - Vanessa Vuibert (@vvuibert)
'''

EXAMPLES = '''
# Check the status of the orchestrator pod
- name: Check orchestrator status
  kube_status:
    namespace: "default"
    label_selector: "app=orchestrator"
'''

RETURN = '''
container_statuses:
    description: Dictionary of the status of the pods
    type: dict
ready:
    description: Pods are ready
    type: boolean
'''

BAD_REASONS = ['CrashLoopBackOff', 'Error', 'ImagePullBackOff']


def run_module():
    module_args = dict(
        namespace=dict(type='str', required=False, default='default'),
        label_selector=dict(type='str', required=False, default=''),
    )

    result = dict(
        changed=False,
        container_statuses=dict(),
        ready=True
    )

    ansible_module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if ansible_module.check_mode:
        return result

    try:
        config.load_kube_config()
        for pod in client.CoreV1Api().list_namespaced_pod(
                namespace=ansible_module.params['namespace'],
                label_selector=ansible_module.params['label_selector']).items:

            container_statuses = []
            for status in pod.status.container_statuses:
                if status.state.running:
                    container_state = {'running': status.state.running.to_dict()}
                elif status.state.waiting:
                    container_state = {'waiting': status.state.waiting.to_dict()}
                else:
                    container_state = {'terminated': status.state.terminated.to_dict()}
                container_statuses.append({
                    "name": status.name,
                    "status": container_state}
                )
                if not status.ready:
                    result['ready'] = False
                    if status.state.terminated and (status.state.waiting.reason in BAD_REASONS):
                        ansible_module.fail_json(msg=str(status.state.terminated.message))
                    elif status.state.waiting and (status.state.waiting.reason in BAD_REASONS):
                        ansible_module.fail_json(msg=str(status.state.waiting.message))
            result['container_statuses'][pod.metadata.name] = container_statuses

        result['changed'] = True
    except Exception as e:
        ansible_module.fail_json(msg=str(e))

    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
