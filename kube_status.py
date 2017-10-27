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

    pods_fail_statuses = dict()

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
            container_statuses = dict()
            container_failed_statuses = dict()
            if pod.status.container_statuses:
                for status in pod.status.container_statuses:
                    if status.state.running:
                        container_state = {'running': status.state.running.to_dict()}
                    elif status.state.waiting:
                        container_state = {'waiting': status.state.waiting.to_dict()}
                    else:
                        container_state = {'terminated': status.state.terminated.to_dict()}
                    container_statuses[status.name] = container_state

                    if not status.ready:
                        result['ready'] = False
                        if (status.state.terminated and (status.state.terminated.reason in BAD_REASONS)) or \
                                (status.state.waiting and (status.state.waiting.reason in BAD_REASONS)):
                            container_failed_statuses[status.name] = container_state
            else:
                container_failed_statuses = pod.status.to_dict()

            result['container_statuses'][pod.metadata.name] = container_statuses
            if container_failed_statuses:
                pods_fail_statuses[pod.metadata.name] = container_failed_statuses

        result['changed'] = True
        if pods_fail_statuses:
            ansible_module.fail_json(msg="At least one pod is failing",
                                     container_statuses=pods_fail_statuses,
                                     ready=False, changed=True)
    except Exception as e:
        result['ready'] = False
        ansible_module.fail_json(msg=str(e), ready=False, changed=True)

    ansible_module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
