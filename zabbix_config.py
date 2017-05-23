#!/usr/bin/python
ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: zabbix_config
short_description: Configure zabbix through the Zabbix API
description:
 This module call the Zabbix API.
author: "Sebastien Nobert (sebastien.nobert@guavus.com"
requirements:
    - requests
'''

EXAMPLES = '''
- name: Create a Zabbix Template
  zabbix_config:
    zabbix_url: "http://my.zabbix.net:8080/"
    zabbix_user: Admin
    zabbix_password: zabbix
    api: template
    api_args: { "name": "myTemplate" }

- name: Update a Zabbix Template
  zabbix_config:
    zabbix_url: "http://my.zabbix.net:8080/"
    zabbix_user: Admin
    zabbix_password: zabbix
    api: template
    api_args:
      name: myTemplate
      host:
        - server1
        - server2
        - server3

- name: Delete a Zabbix Template
  zabbix_config:
    zabbix_url: "..."
    zabbix_user: "Admin"
    zabbix_password: "zabbix"
    api: "template"
    api_args: {}
    state: "absent"
'''

import json
HAS_REQUESTS = False
try:
    import requests
except ImportError:
    pass
else:
    HAS_REQUESTS = True


class ZabbixConfigException(Exception):
    """ generic zabbix config exception """
    pass


class ZabbixConfig(object):

    def __init__(self, module):
        self.url = module.params["zabbix_url"]
        self.user = module.params["zabbix_user"]
        self.password = module.params["zabbix_password"]
        self.api = module.params["api"]
        self.api_args = module.params["api_args"]
        self.api_uid = module.params["api_uid"]

        if self.api_uid not in module.params["api_args"]:
            raise(Exception("No {} provided in api_args".format(self.api_uid)))

        self.name = module.params["api_args"][self.api_uid]
        self.zapi = None
        self.id = None
        self.object = None
        self.check_mode = module.check_mode
        self.request_id = 0
        self.auth = None

        if self.api == "hostgroup":
            self.id_string = "groupid"
        else:
            self.id_string = "{}id".format(self.api)

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json-rpc',
                                     'Cache-Control': 'no-cache'})
        try:
            self.auth = self.do_request("user.login",
                                        {'user': self.user,
                                         'password': self.password})
        except Exception as e:
            raise(ZabbixConfigException("Cannot login to zabbix api at {} ({})".format(self.url, e)))

        try:
            self.get_object({})
        except Exception as e:
            raise(ZabbixConfigException("Exception while checking if {} \"{}\" exist [params {}]({})".format(self.api, self.name, self.api_args, e)))

    def do_request(self, method, params=None):
        post_data = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': self.request_id}
        if self.auth is not None:
            post_data['auth'] = self.auth

        response = self.session.post(self.url, data=json.dumps(post_data))
        self.request_id += 1
        response.raise_for_status()
        data = json.loads(response.text)
        if 'error' in data:
            raise ZabbixConfigException("Zabbix API Error {}: {}, {}".format(data['error']['code'], data['error']['message'], data['error']['data'] or "No data"))

        return data['result']

    def get_object(self, params):
        params["filter"] = {self.api_uid : self.name}
        self.check_api_args(params)
        result = self.do_request("{}.get".format(self.api), params)
        if len(result) > 0:
            self.id = result[0][self.id_string]
            self.object = result[0]
            self.sort_api_args()
            return True

        return False

    def check_api_args(self, params):
        if 'groups' in self.api_args:
            params['selectGroups'] = 'groupid'
        if 'templates' in self.api_args:
            params['selectParentTemplates'] = 'templateid'
        if 'interfaces' in self.api_args:
            params['selectInterfaces'] = 'extend'
        if 'hosts' in self.api_args:
            params['selectHosts'] = 'hostid'
        if 'hostid' in self.api_args:
            params['filter']['hostid'] = self.api_args['hostid']

    def sort_api_args(self):
        if 'templates' in self.api_args:
            self.object['templates'] = self.object['parentTemplates']
            self.object['templates'] = sorted(self.object['templates'], key=lambda k: k['templateid'])
            self.api_args['templates'] = sorted(self.api_args['templates'], key=lambda k: k['templateid'])
        if 'groups' in self.api_args:
            self.object['groups'] = sorted(self.object['groups'], key=lambda k: k['groupid'])
            self.api_args['groups'] = sorted(self.api_args['groups'], key=lambda k: k['groupid'])
        if 'interfaces' in self.api_args:
            self.object['interfaces'] = sorted(self.object['interfaces'], key=lambda k: k['ip'])
            self.api_args['interfaces'] = sorted(self.api_args['interfaces'], key=lambda k: k['ip'])
        if 'hosts' in self.api_args:
            self.object['hosts'] = sorted(self.object['hosts'], key=lambda k: k['hostid'])
            self.api_args['hosts'] = sorted(self.api_args['hosts'], key=lambda k: k['hostid'])

    def update_object(self):
        has_changed = False
        meta = {}

        if self.need_update(self.api_args, self.object) is True:
            self.api_args[self.id_string] = self.id
            result = self.do_request("{}.update".format(self.api), self.api_args)
            has_changed = True
        meta = {"name": self.name, self.id_string: self.id}
        return (has_changed, meta)

    def list_to_dict(self, l):
        return dict(zip(map(str, range(len(l))), l))

    def need_update(self, update, orig, is_recursive=False):
        need_update = False
        for k in update.keys():
            if k not in orig:
                need_update = True
                continue
            if type(update[k]) != list and \
                    type(update[k]) != dict and update[k] != orig[k]:
                need_update = True
                continue
            if type(update[k]) != type(orig[k]):
                if (type(update[k]) != str and type(update[k]) != unicode) \
                        or (type(orig[k]) != str and type(orig[k]) != unicode):
                    need_update = True
                    continue
            if type(orig[k]) == dict:
                if self.need_update(update[k], orig[k], True) is True:
                    need_update = True
                    continue
            elif type(orig[k]) == list:
                if self.need_update(self.list_to_dict(update[k]), self.list_to_dict(orig[k]), True) is True:
                    need_update = True
                    continue

            if is_recursive is False:
                update.pop(k)

        return need_update

    def state_present(self):
        has_changed = False
        meta = {}

        try:
            if self.id is None:
                result = self.do_request("{}.create".format(self.api), self.api_args)
                has_changed = True
                self.id = result["{}s".format(self.id_string)][0]
                meta = {"name": self.name, self.id_string: self.id}
            else:
                has_changed, meta = self.update_object()
        except Exception as e:
            raise(ZabbixConfigException("Exception for api: {}, name: {}, id: {} [params {}]({})".format(self.api, self.name, self.id, self.api_args, e)))

        return (has_changed, meta)

    def state_absent(self):
        has_changed = False
        meta = {}

        if self.id is None:
            return (False, {})

        try:
            result = self.do_request("{}.delete".format(self.api), [self.id])
            if len(result) > 0:
                has_changed = True
                meta = {"name": self.name, self.id_string: self.id}
        except Exception as e:
            raise(ZabbixConfigException("Exception while deleting {} {} ({})".format(self.api, self.name, e)))

        return (has_changed, meta)


def main():
    has_changed = False
    result = {}

    module = AnsibleModule(
        argument_spec = dict(
            zabbix_url = dict(required=True, type="str"),
            zabbix_user = dict(required=True, type="str"),
            zabbix_password = dict(required=True, type="str", no_log=True),
            api = dict(required=True, type="str"),
            api_uid = dict(required=True, type="str"),
            api_args = dict(required=True, type="dict"),
            state = dict(default="present",
                       choices=["present", "absent"], type="str")
        ),
        supports_check_mode=True
    )
    if (HAS_REQUESTS is False):
        module.fail_json(msg="'requests' package not found... \
                         you can try install using pip: pip install requests")
    try:
        config = ZabbixConfig(module)

        if module.params['state'] == "present":
            has_changed, result = config.state_present()
        else:
            has_changed, result = config.state_absent()
    except Exception as e:
        module.fail_json(msg="{}".format(e))

    module.exit_json(changed=has_changed, meta=result)


from ansible.module_utils.basic import AnsibleModule
if __name__ == '__main__':
    main()
