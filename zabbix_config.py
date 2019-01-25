#!/usr/bin/python

# Copyright (c) 2017 [Guavus]
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: zabbix_config
short_description: Configure zabbix through the Zabbix API
description:
 This module call the Zabbix API.
author:
  - "Sebastien Nobert (sebastien.nobert@guavus.com)"
  - "Felix Archambault (felix.archambault@guavus.com)"
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


HAS_REQUESTS = False
try:
    from requests import Session
except ImportError:
    pass
else:
    HAS_REQUESTS = True

from ansible.module_utils.basic import AnsibleModule

# Mapping to retreive the primary key of and object from the object type.
ZBX_API_UID = dict(
    hostgroup='name',
    template='host',
    item='name',
    trigger='description',
    host='host',
    user='alias'
)


class ZabbixConfig(object):

    def __init__(self, module):

        # api url
        self.zbx_url = module.params["zabbix_url"]

        # zbx request_id counter
        self.zbx_request_id = 0
        self.zbx_request = None

        # to avoid error on first query
        self.auth = None

        # passing the module fail_json fct allow error handling in do_request
        # method
        self.fail_json = module.fail_json

        self.session = Session()
        self.session.headers.update({'Content-Type': 'application/json-rpc',
                                     'Cache-Control': 'no-cache'})

        # # inject proxy for debugging
        # proxies = {'http': 'http://localhost:8080'}
        # self.session.proxies = proxies

        # zbx auth is done through it's api
        # authenticate, (every request will be parsed for errors)
        self.prepare_request('user.login',
                             {'user': module.params["zabbix_user"],
                              'password': module.params["zabbix_password"]}
                             )

        resp = self.do_request()

        # register the auth token to use with api
        self.auth = resp['result']

    def prepare_request(self, zbx_method, zbx_params=None, extra_params=None):
        """
        Prepare request's json payload.

        Purpose of this is to save to internal variable for ansible -vvv
        debugging output and allow functionning of check_mode.
        Extra params will be merged.
        """
        p = zbx_params.copy()
        if extra_params is not None:
            p.update(extra_params)

        self.zbx_request = dict(jsonrpc=2.0,
                                id=self.zbx_request_id,
                                method=zbx_method,
                                params=p,
                                auth=self.auth
                                )

    def do_request(self):
        """
        Perform post request to zabbix api.

        zbx_method: zabbix api method
        zbx_params: content of zabbix params, not to be confused with requests
        params.

        Returns: server json response
        """
        self.zbx_request_id += 1

        r = self.session.post(self.zbx_url, json=self.zbx_request)

        resp = r.json()
        # unsucessful login attemps returns a http 200 rc. we must examine
        # returned json for success or failure.
        if 'error' in resp:
            self.fail_json(
                msg="Zabbix API error: {}".format(
                    resp['error']['data']))
        else:
            return resp

    def get_objects(self, api, api_args, filter=None):
        """
        Retrieve one or multiple zabbix objects.

        api: zabbix api to use
        api_args: a dict or list of dict, each dict contain valid zabbix params
        filter: a dict containing filters that will be injected literally
            e.g: filter={"hostid": 10153} will result in:
                    "method": "item.get",
                    "params": [
                        {
                        "filter": {
                            "hostid": 10153
                        },
                        ...

        NOTE: Zabbix api get methods do not accept arrays. As such, it cannot
                perform bulk query.
        """

        # zbx_params = api_args
        zbx_params = {}
        # zbx_params[ZBX_API_UID[api]] = api_args[ZBX_API_UID[api]]

        # this is kind of hackish, but will plug a filter criteria by the name
        # of a key found in api_args. Unique key for each api is defined in
        # ZBX_API_UID hash.
        zbx_params["filter"] = {ZBX_API_UID[api]: api_args[ZBX_API_UID[api]]}

        # add extra filter from dict
        if filter is not None:
            zbx_params["filter"].update(filter)

        # see also:
        # https://www.zabbix.com/documentation/3.2/manual/api/reference_commentary#common_get_method_parameters

        if "groups" in api_args:
            zbx_params["selectGroups"] = "groupid"

        if "templates" in api_args:
            zbx_params["selectParentTemplates"] = "templateid"

        if "interfaces" in api_args:
            zbx_params["selectInterfaces"] = "extend"

        if "hosts" in api_args:
            zbx_params["selectHosts"] = "hostid"

        if "hostid" in api_args:
            zbx_params["filter"]["hostid"] = api_args["hostid"]

        if api == "trigger":
            zbx_params["expandExpression"] = True
            zbx_params["templated"] = True

        if api == "configuration.export":
            pass

        self.prepare_request("{}.get".format(api), zbx_params)
        return self.do_request()

    # def update_params(self, api_args, zbx_objects):
    #     """
    #     Compute required update for a list of zabbix objects.

    #     api_args: list of zabbix new objects
    #     zbx_objects: object reference retreived from zabbix. Will contain more
    #                     objects than api_args

    #     Return a modified api_args where each index only contains values to be
    #     updated.
    #     """

    #     # if isinstance(api_args, list):
    #     #     zbx_params = [self.json_diff(api, api_arg)
    #     #                   for api_arg in api_args]
    #     # elif isinstance(api_args, dict):
    #     #     zbx_params = {}
    #     #     zbx_params = self.json_diff(api, api_args)
    #     pass


def list_to_dict(l):
    """
    Make a dict from a list of dicts.

    Input:
        l = [     {
        ...         "groupid": "25"
        ...       },
        ...       {
        ...         "groupid": "9"
        ...       },
        ...       {
        ...         "groupid": "12"
        ...       }
        ...     ]

    Output:

        {
        '9': {'groupid': '9'},
        '12': {'groupid': '12'},
        '25': {'groupid': '25'}
        }

    Note: zip function in Python 3 returns an iterator instead of a list.
            Therefore we convert iterator to list to get Consistant behavior
            regardless of interpreter version.

            We could have used something like this:
                nd = {};
                for elems in l:
                    for k, v in elems.items():
                        nd[v] = {k:v}

            But it requires all values to be unique.
            This might not be correct in all cases.

    """
    return dict(
        list(
            zip(map(str, range(len(l))),
                sorted(l, key=valgetter)
                )
        )
    )


def json_diff(d1, d2, recursive=False):
    """
    Recursively compare 2 json objects.

    d1:  reference json object
    d2:  json object compared against d1

    Semantically equivalent to the diff command.
    All identical keys will be popped out of d2. Pass a copy of your dict
    if you do not want to alter your json blob.
    Returns: True if objects are different (d2 non-empty)
                False if objects are identical (d2 empty)
    """
    different = False
    # the .keys method returns a view in python 3 (returned a list in
    # python 2) This idiom makes it work on both interpreter
    for k in list(d2.keys()):

        if k not in d1:
            different = True
            continue

        if not isinstance(d2[k], list) and not isinstance(d2[k], dict):
            # cast to string for the test: zbx always return strings
            d2[k] = str(d2[k])
            d1[k] = str(d1[k])
            # if str(d2[k]) != str(d1[k]):
            if d2[k] != d1[k]:
                different = True
                continue

        # recursive invocation
        if isinstance(d1[k], dict) and isinstance(d2[k], dict):
            if json_diff(d1[k], d2[k], True):
                different = True
                continue

        # recursive invocation
        if isinstance(d1[k], list) and isinstance(d2[k], list):
            if json_diff(list_to_dict(d1[k]), list_to_dict(d2[k]), True):
                different = True
                continue

        if not recursive:
            d2.pop(k)

    return different


def valgetter(x):
    """
    Return value from a one item dict.

    input: {'groupid': '9'}
    output: 9

    Will try to cast value to int. This is required for proper sorting when
    string is a number.
    """
    val = list(x.values())[0]
    try:
        return int(val)
    except:
        return val


def update_zabbix_object(module, zbx, zbx_objectid=None):
    """
    Create, Read, Update, Delete a zabbix object

    See list of objects in zabbix api methode reference.
    """

    # extract args from module
    state = module.params['state']
    api = module.params['api']
    api_args = module.params['api_args']

    # default state and response
    changed = False
    obj_exist = False
    zbx_resp = None
    meta = ""

    # quirk for zabbix hostgroup inconsistencies
    # this value is used as key in ansible meta output.
    if api == "hostgroup":
        id_string = "groupid"
    else:
        id_string = "{}id".format(api)

    # Attempt to get templateid if template_name was passed. Will return
    templateid = get_object_id(zbx, 'template', module.params['template_name'])

    if templateid is not None:
        # Inject templateid to avoid returning multiple instance of an object.
        # It does so for instance if object is link to a host and a template.

        # objectid = zbx.get_objects('template', dict(host=template_name))
        zbx_objects = zbx.get_objects(api, module.params['api_args'],
                                      dict(hostid=templateid)
                                      )
    else:
        # assume an hostid is passed in the api_args content
        zbx_objects = zbx.get_objects(api, module.params['api_args'])

    # while one day we would like to support loading of several identical
    # objects belonging to the same template, it poses challenges as
    # we can only search for one object at at time or return all object of a
    # template hostid. In that scenario this test below would not be enough. We
    # would have to filter list to return only dicts of interest.
    if len(zbx_objects['result']) > 0:
        obj_exist = True

    if state == "present" and not obj_exist:
        zbx.prepare_request("{}.create".format(api), api_args, templateid)
        changed = True
        if not module.check_mode:
            zbx_resp = zbx.do_request()

    elif state == "absent" and obj_exist:
        zbx.prepare_request("{}.delete".format(api), api_args, templateid)
        changed = True
        if not module.check_mode:
            zbx_resp = zbx.do_request()

    elif state == "present" and obj_exist:
        # check if object needs update
        # this variable is to prevent altering ansible invocation args
        zbx_api_args = api_args.copy()

        # templates vs selectParentTemplates quirk
        current_zbx_object = zbx_objects['result'][0]
        if 'parentTemplates' in current_zbx_object:
            current_zbx_object['templates'] =  \
                current_zbx_object.pop("parentTemplates")

        if json_diff(current_zbx_object, zbx_api_args):
            # import pprint; pp = pprint.PrettyPrinter(indent=2); pp.pprint(api_args)
            # import sys; sys.exit()

            # Side effect of json_diff method is to pop out the uid key of a
            # method. Let's insert it back.
            zbx_api_args[ZBX_API_UID[api]] = api_args[ZBX_API_UID[api]]

            # object id must also be inserted back
            zbx_api_args[id_string] = current_zbx_object[id_string]

            zbx.prepare_request("{}.update".format(api), zbx_api_args,
                                templateid)

            changed = True
            if not module.check_mode:
                zbx_resp = zbx.do_request()

                # Update only returns the string id if sucessful so we fill
                # meta from the args
                meta = {"name": zbx_api_args[ZBX_API_UID[api]],
                        id_string: zbx_api_args[id_string]
                        }

    # object was just created, we must fill meta
    if not obj_exist and zbx_resp is not None:
        # Sample output when creating an item. Returned result is not a list.
        # id_string is also plural
        # {
        #     "id": 3,
        #     "jsonrpc": "2.0",
        #     "result": {
        #         "itemids": [
        #             "30060"
        #         ]
        #     }
        # }
        meta = {api_args[ZBX_API_UID[api]]:
                zbx_resp['result'][id_string + 's'][0]}

    # object not updated because identical or check mode
    if obj_exist and zbx_resp is None:
        zbx_resp = zbx_objects
        meta = {zbx_resp['result'][0][ZBX_API_UID[api]]:
                zbx_resp['result'][0][id_string]
                }

    module.exit_json(changed=changed,
                     meta=meta,
                     zabbix_request=zbx.zbx_request,
                     results=zbx_resp)


def zabbix_config(module, zbx):
    """
    Import and export Zabbix configuration data.

    This makes use of zabbix api import/export mechanism.

    Example params to export configuration:

        "params": {
        "options": {
            "hosts": [
                "10161"
            ]
        },
        "format": "xml"
        },

        where options object has the following parameters:
            * groups - (array) IDs of host groups to export;
            * hosts - (array) IDs of hosts to export;
            * images - (array) IDs of images to export;
            * maps - (array) IDs of maps to export.
            * screens - (array) IDs of screens to export;
            * templates - (array) IDs of templates to export;
            * valueMaps - (array) IDs of value maps to export;
    """
    template_name = module.params['template_name']
    name = module.params['zbx_name']
    kind = module.params['kind']
    changed = False
    api = module.params['api']
    zbx_resp = None

    if api == 'configuration.export':

        # template_name module arg prevails
        if (template_name is not None or (kind == 'template' and
                                          name is not None)):

            name = template_name
            kind = 'template'

        # find the objectid we want to export
        object_id = get_object_id(zbx, kind, name)
        zbx.prepare_request("configuration.export",
                            module.params['api_args'],
                            dict(options={"{}s".format(kind): [object_id]})
                            )

    else:
        # Import rules. Picked those as enabled in the UI template import
        # wizard.
        zbx.prepare_request("configuration.import",
                            module.params['api_args'],
                            {"rules": dict(
                                applications=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                items=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                triggers=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                templates=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                ),
                                graphs=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                groups=dict(
                                    createMissing='true'
                                ),
                                hosts=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                ),
                                httptests=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                templateLinkage=dict(
                                    createMissing='true'
                                ),
                                templateScreens=dict(
                                    createMissing='true',
                                    updateExisting='true',
                                    deleteMissing='true'
                                ),
                                images=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                ),
                                maps=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                ),
                                screens=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                ),
                                valueMaps=dict(
                                    createMissing='true',
                                    updateExisting='true'
                                )
                            )}
                            )

    if not module.check_mode:
        zbx_resp = zbx.do_request()

    if zbx_resp is not None:
        if api == 'configuration.import':
            changed = zbx_resp['result']

    module.exit_json(changed=changed,
                     zabbix_request=zbx.zbx_request,
                     results=zbx_resp)


def get_object_id(zbx, object_type, object_name=None):
    """
    Get a zabbix object id.

    zbx: zbx object to interact with zabbix api
    object_type: kind of the object (item, trigger, template,...)
    object_name: object name to get the id of.


    Returns: Object id or None if object_name is None.
    """
    if object_name is None:
        return None
    else:
        if object_type == 'group':
            res = zbx.get_objects('hostgroup',
                                  {ZBX_API_UID['hostgroup']: object_name})
        else:
            res = zbx.get_objects(object_type,
                                  {ZBX_API_UID[object_type]: object_name})

        if len(res['result']) > 0:
            if object_type == 'group':
                return res['result'][0]['groupid']
            else:
                return res['result'][0]["{}id".format(object_type)]
        else:
            return None


def main():
    module = AnsibleModule(
        argument_spec=dict(
            zabbix_url=dict(required=True, type="str"),
            zabbix_user=dict(required=True, type="str"),
            zabbix_password=dict(required=True, type="str", no_log=True),
            api=dict(required=True, type="str"),
            api_args=dict(required=True, type="raw"),
            template_name=dict(required=False, type="str"),
            zbx_name=dict(required=False, type="str"),
            kind=dict(required=False, type="str"),
            state=dict(default="present",
                       choices=["present", "absent"], type="str")
        ),
        supports_check_mode=True
    )
    if (HAS_REQUESTS is False):
        module.fail_json(msg="'requests' package not found... \
                         you can try install using pip: pip install requests")

    zbx = ZabbixConfig(module)

    if module.params['api'].startswith('configuration'):
        # Use zabbix configurations loading mechanism. This is handy to work
        # with complex templates
        zabbix_config(module, zbx)
    else:
        # Interact with zbx using other api methods.
        update_zabbix_object(module, zbx)


if __name__ == '__main__':
    main()
