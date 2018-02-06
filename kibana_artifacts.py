#!/usr/bin/python
#
# Copyright 2017 Guavus - A Thales company
#
# This file is part of Guavus Infrastructure using Ansible

from ansible.module_utils.basic import AnsibleModule
import json

try:
    import requests
    HAS_LIB_REQUEST = True
except ImportError:
    HAS_LIB_REQUEST = False

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: kibana_artifacts

short_description: Upload Kibana artifacts

description:
    - Upload Kibana artifacts

author:
    - Olivier Toupin (@oliviertoupin)

version_added: "2.2"

options:
    kibana_url:
        description: The Kibana instance's url
        required: True
    artifacts_json:
        description: JSON content of the dashboards
        required: True
    overwrite_existing:
        description: Should the plugin overwrite/update existing dashboard (same uuids) or create only.
        default: False

requirements:
  - "python >= 2.7"
  - "requests >= 2.0"
'''

EXAMPLES = '''
---
- name: Upload Kibana artifacts
  kibana_artifacts:
    kibana_url: 'http://data013-mgt-01.devops.guavus.mtl:5601'
    artifacts_json: "{{ lookup('file', './dashboards/dashboards.json') }}"
    overwrite_existing: True
'''

RETURN = '''
---
meta:
    description: unique text results
    type: list
'''


def upload_artifact(artifact, kibana_url, overwrite_existing):
    create_url_template = "{url}/es_admin/.kibana/{type}/{uuid}/_create/"
    update_url_template = "{url}/es_admin/.kibana/{type}/{uuid}"

    artifact_type = artifact['_type']
    artifact_uuid = artifact['_id']
    artifact_core = artifact['_source']

    headers = {'kbn-version': '5.2.0'}

    create_url_for_artifact = create_url_template.format(url=kibana_url, type=artifact_type, uuid=artifact_uuid)
    update_url_for_artifact = update_url_template.format(url=kibana_url, type=artifact_type, uuid=artifact_uuid)

    r_create = requests.post(create_url_for_artifact, data=json.dumps(artifact_core), headers=headers)

    if r_create.status_code == 201:
        # Created
        return False, True, "changed"
    elif r_create.status_code == 409:
        if overwrite_existing:
            # Changed-Overwrite
            requests.post(update_url_for_artifact, data=json.dumps(artifact_core), headers=headers)
            return False, True, "changed-overwrite"
        else:
            # Not changed
            return False, False, "not-updated"
    else:
        # Error
        return True, False, r_create.text


def upload_artifacts(kibana_url, artifacts_json, overwrite_existing):

    results = (upload_artifact(artifact, kibana_url, overwrite_existing) for artifact in artifacts_json)

    return list(results)


def extract_results(all_results):
    is_error = any(map(lambda x: x[0], all_results))
    has_changed = any(map(lambda x: x[1], all_results))
    unique_text_results = list(set(map(lambda x: x[2], all_results)))

    return is_error, has_changed, unique_text_results


def main():
    argument_spec = {
        "kibana_url": {"required": True, "type": "str"},
        "artifacts_json": {"required": True, "type": "raw"},
        "overwrite_existing": {"required": False, "default": False, "type": "bool"}
    }

    ansible_module = AnsibleModule(argument_spec, supports_check_mode=True)

    if not HAS_LIB_REQUEST:
        ansible_module.fail_json(msg="equests module is not installed, please install.")

    kibana_url = ansible_module.params['kibana_url']
    artifacts_json = ansible_module.params['artifacts_json']
    overwrite_existing = ansible_module.params['overwrite_existing']

    # list[(is_error, has_changed, result)]
    all_results = upload_artifacts(kibana_url, artifacts_json, overwrite_existing)

    is_error, has_changed, unique_text_results = extract_results(all_results)

    if not is_error:
        ansible_module.exit_json(changed=has_changed, meta=unique_text_results)
    else:
        ansible_module.fail_json(msg="Error creating/updating dashboards", meta=unique_text_results)


if __name__ == '__main__':
    main()
