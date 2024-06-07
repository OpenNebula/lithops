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


# Add OpenNebula defaults
DEFAULT_CONFIG_KEYS = {
    'timeout': 600,
    'kubcfg_path': '/tmp/kube_config'
}


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

    # TODO: change me
    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data['one']:
            config_data['one'][key] = DEFAULT_CONFIG_KEYS[key]

    # Load k8s default config
    original_load_config(config_data)