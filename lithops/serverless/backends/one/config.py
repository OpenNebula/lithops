import os
import json


from lithops.serverless.backends.k8s.config import (
    DEFAULT_CONFIG_KEYS,
    DOCKERFILE_DEFAULT,
    load_config as original_load_config
)


class OneConfigError(Exception):
    pass


MANDATORY_CONFIG_KEYS = {
    "public_network_id",
    "private_network_id"
}


OPTIONAL_CONFIG_KEYS = {
    "ONEAPP_VROUTER_ETH0_VIP0": "",
    "ONEAPP_VROUTER_ETH1_VIP0": "",
    "ONEAPP_RKE2_SUPERVISOR_EP": "ep0.eth0.vr:9345",
    "ONEAPP_K8S_CONTROL_PLANE_EP": "ep0.eth0.vr:6443",
    "ONEAPP_K8S_EXTRA_SANS": "localhost,127.0.0.1,ep0.eth0.vr,${vnf.TEMPLATE.CONTEXT.ETH0_IP},k8s.yourdomain.it",
    "ONEAPP_K8S_MULTUS_ENABLED": "NO",
    "ONEAPP_K8S_MULTUS_CONFIG": "",
    "ONEAPP_K8S_CNI_PLUGIN": "cilium",
    "ONEAPP_K8S_CNI_CONFIG": "",
    "ONEAPP_K8S_CILIUM_RANGE": "",
    "ONEAPP_K8S_METALLB_ENABLED": "NO",
    "ONEAPP_K8S_METALLB_CONFIG": "",
    "ONEAPP_K8S_METALLB_RANGE": "",
    "ONEAPP_K8S_LONGHORN_ENABLED": "YES",
    "ONEAPP_STORAGE_DEVICE": "/dev/vdb",
    "ONEAPP_STORAGE_FILESYSTEM": "xfs",
    "ONEAPP_K8S_TRAEFIK_ENABLED": "YES",
    "ONEAPP_VNF_HAPROXY_INTERFACES": "eth0",
    "ONEAPP_VNF_HAPROXY_REFRESH_RATE": "30",
    "ONEAPP_VNF_HAPROXY_LB0_PORT": "9345",
    "ONEAPP_VNF_HAPROXY_LB1_PORT": "6443",
    "ONEAPP_VNF_HAPROXY_LB2_PORT": "443",
    "ONEAPP_VNF_HAPROXY_LB3_PORT": "80",
    "ONEAPP_VNF_DNS_ENABLED": "YES",
    "ONEAPP_VNF_DNS_INTERFACES": "eth1",
    "ONEAPP_VNF_DNS_NAMESERVERS": "1.1.1.1,8.8.8.8",
    "ONEAPP_VNF_NAT4_ENABLED": "YES",
    "ONEAPP_VNF_NAT4_INTERFACES_OUT": "eth0",
    "ONEAPP_VNF_ROUTER4_ENABLED": "YES",
    "ONEAPP_VNF_ROUTER4_INTERFACES": "eth0,eth1"
}


DEFAULT_PRIVATE_VNET = """
NAME    = "private-oneke"
VN_MAD  = "bridge"
AUTOMATIC_VLAN_ID = "YES"
AR = [TYPE = "IP4", IP = "192.168.150.0", SIZE = "51"]
"""


FH_ZIP_LOCATION = os.path.join(os.getcwd(), 'lithops_one.zip')


# Overwrite default Dockerfile
DOCKERFILE_DEFAULT = "\n".join(DOCKERFILE_DEFAULT.split('\n')[:-2]) + """
COPY lithops_one.zip .
RUN unzip lithops_one.zip && rm lithops_one.zip
"""


STATE = {
    0: "INIT",
    1: "PENDING",
    2: "HOLD",
    3: "ACTIVE",
    4: "STOPPED",
    5: "SUSPENDED",
    6: "DONE",
    8: "POWEROFF",
    9: "UNDEPLOYED",
    10: "CLONING",
    11: "CLONING_FAILURE"
}


LCM_STATE = {
    0: "LCM_INIT",
    1: "PROLOG",
    2: "BOOT",
    3: "RUNNING",
    4: "MIGRATE",
    5: "SAVE_STOP",
    6: "SAVE_SUSPEND",
    7: "SAVE_MIGRATE",
    8: "PROLOG_MIGRATE",
    9: "PROLOG_RESUME",
    10: "EPILOG_STOP",
    11: "EPILOG",
    12: "SHUTDOWN",
    15: "CLEANUP_RESUBMIT",
    16: "UNKNOWN",
    17: "HOTPLUG",
    18: "SHUTDOWN_POWEROFF",
    19: "BOOT_UNKNOWN",
    20: "BOOT_POWEROFF",
    21: "BOOT_SUSPENDED",
    22: "BOOT_STOPPED",
    23: "CLEANUP_DELETE",
    24: "HOTPLUG_SNAPSHOT",
    25: "HOTPLUG_NIC",
    26: "HOTPLUG_SAVEAS",
    27: "HOTPLUG_SAVEAS_POWEROFF",
    28: "HOTPLUG_SAVEAS_SUSPENDED",
    29: "SHUTDOWN_UNDEPLOY",
    30: "EPILOG_UNDEPLOY",
    31: "PROLOG_UNDEPLOY",
    32: "BOOT_UNDEPLOY",
    33: "HOTPLUG_PROLOG_POWEROFF",
    34: "HOTPLUG_EPILOG_POWEROFF",
    35: "BOOT_MIGRATE",
    36: "BOOT_FAILURE",
    37: "BOOT_MIGRATE_FAILURE",
    38: "PROLOG_MIGRATE_FAILURE",
    39: "PROLOG_FAILURE",
    40: "EPILOG_FAILURE",
    41: "EPILOG_STOP_FAILURE",
    42: "EPILOG_UNDEPLOY_FAILURE",
    43: "PROLOG_MIGRATE_POWEROFF",
    44: "PROLOG_MIGRATE_POWEROFF_FAILURE",
    45: "PROLOG_MIGRATE_SUSPEND",
    46: "PROLOG_MIGRATE_SUSPEND_FAILURE",
    47: "BOOT_UNDEPLOY_FAILURE",
    48: "BOOT_STOPPED_FAILURE",
    49: "PROLOG_RESUME_FAILURE",
    50: "PROLOG_UNDEPLOY_FAILURE",
    51: "DISK_SNAPSHOT_POWEROFF",
    52: "DISK_SNAPSHOT_REVERT_POWEROFF",
    53: "DISK_SNAPSHOT_DELETE_POWEROFF",
    54: "DISK_SNAPSHOT_SUSPENDED",
    55: "DISK_SNAPSHOT_REVERT_SUSPENDED",
    56: "DISK_SNAPSHOT_DELETE_SUSPENDED",
    57: "DISK_SNAPSHOT",
    59: "DISK_SNAPSHOT_DELETE",
    60: "PROLOG_MIGRATE_UNKNOWN",
    61: "PROLOG_MIGRATE_UNKNOWN_FAILURE",
    62: "DISK_RESIZE",
    63: "DISK_RESIZE_POWEROFF",
    64: "DISK_RESIZE_UNDEPLOYED",
    65: "HOTPLUG_NIC_POWEROFF",
    66: "HOTPLUG_RESIZE",
    67: "HOTPLUG_SAVEAS_UNDEPLOYED",
    68: "HOTPLUG_SAVEAS_STOPPED",
    69: "BACKUP",
    70: "BACKUP_POWEROFF"
}


# Add OpenNebula defaults
DEFAULT_CONFIG_KEYS.update({
    'timeout': 600,
    'kubecfg_path': '/tmp/kube_config',
    'oneke_config_path': None,
    'delete': False,
    'minimum_nodes': 0
})


def load_config(config_data):
    if 'oneke_config' in config_data['one']:
        oneke_config = config_data['one']['oneke_config']

        # Validate mandatory params
        for key in MANDATORY_CONFIG_KEYS:
            if key not in oneke_config:
                raise OneConfigError(f"'{key}' is missing in 'oneke_config'")
        public_network_id = oneke_config['public_network_id']
        private_network_id = oneke_config['private_network_id']

        # Optional params
        name = oneke_config.get('name', 'OneKE for lithops')
        custom_attrs_values = {key: oneke_config.get(key, default_value) 
                               for key, default_value in OPTIONAL_CONFIG_KEYS.items()}

        oneke_update = {
            "name": name,
            "networks_values": [
                {"Public": {"id": str(public_network_id)}},
                {"Private": {"id": str(private_network_id)}}
            ],
            "custom_attrs_values": custom_attrs_values
        }
        # Override oneke_config with a valid JSON to update the service
        config_data['one']['oneke_config'] = json.dumps(oneke_update)

    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data['one']:
            config_data['one'][key] = DEFAULT_CONFIG_KEYS[key]

    # Ensure 'k8s' key exists and is a dictionary
    if 'k8s' not in config_data or config_data['k8s'] is None:
        config_data['k8s'] = {}
        config_data['k8s']['docker_user'] = config_data['one']['docker_user']
        config_data['k8s']['docker_password'] = config_data['one']['docker_password']

    # Load k8s default config
    original_load_config(config_data)