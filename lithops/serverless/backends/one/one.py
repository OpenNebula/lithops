from ..k8s.k8s import KubernetesBackend
from .config import STATE, LCM_STATE

import oneflow
import pyone

import os
import json
import time
import base64
import logging
import urllib3

logger = logging.getLogger(__name__)
urllib3.disable_warnings()


class OneError(Exception):
    pass

def _config_one():  
    env = os.environ

    # Reading the `one_auth` file or environment variable.
    # `ONE_AUTH` can be a file path or the credentials, otherwise
    # the default credentials in `$HOME/.one/one_auth` are used.
    one_auth = env.get('ONE_AUTH') or os.path.expanduser('~/.one/one_auth')
    if os.path.isfile(one_auth):
        with open(one_auth, mode='r') as auth_file:
            credentials = auth_file.readlines()[0].strip()
    else:
        credentials = one_auth.strip()

    # Reading environment variables.
    # Environment variable `ONESERVER_URL` superseeds the default URL.
    url = env.get('ONESERVER_URL', 'http://localhost:2633/RPC2')

    return pyone.OneServer(url, session=credentials)    


class OpenNebula(KubernetesBackend):
    """
    A wrap-up around OpenNebula backend.
    """
    def __init__(self, one_config, internal_storage):
        logger.info("Initializing OpenNebula backend")

        logger.debug("Initializing Oneflow python client")
        self.client = oneflow.OneFlowClient()

        logger.debug("Initializing OpenNebula python client")
        self.pyone = _config_one()

        # service_template_id: instantiate master node
        if 'service_template_id' in one_config:
            one_config['service_id'] = self._instantiate_oneke(
                one_config['service_template_id'], 
                one_config['oneke_config'], 
                one_config['oneke_config_path']
            )
            self._wait_for_oneke(one_config['service_id'], one_config['timeout'])
        elif 'service_id' not in one_config:
            raise OneError(
                "OpenNebula backend must contain 'service_template_id' or 'service_id'"
            )

        # Check OneKE status
        self._check_oneke(one_config['service_id'])

        # Get and Save kubeconfig from OneKE
        kubecfg = self._get_kube_config(one_config['service_id'])
        with open(one_config['kubecfg_path'], 'w') as file:
            file.write(kubecfg)

        # Overwrite config values
        self.name = 'one'
        self.kubecfg_path = one_config['kubecfg_path']

        super().__init__(one_config, internal_storage)
    

    def invoke(self, docker_image_name, runtime_memory, job_payload):
        # Get current nodes
        super()._get_nodes()
        logger.info(f"Found {len(self.nodes)} nodes")
        logger.info(f"Nodes: {self.nodes}")
        # TODO: add dynamic scaling logic
        super().invoke(docker_image_name, runtime_memory, job_payload)
    

    def clear(self, job_keys=None):
        super().clear(job_keys)
        # TODO: if all are deteleted -> suspend OneKE VMs (scale down) and
        #       delete them after X minutes
        pass
    

    def _check_oneke(self, service_id):
        logger.info("Checking OpenNebula OneKE service status")

        # Check service status
        _service_json = self.client.servicepool[service_id].info()
        logs = _service_json[str(service_id)]['TEMPLATE']['BODY'].get('log', [])
        state = self._get_latest_state(logs)
        if state is None:
            raise OneError("No state found in logs")
        if state != 'RUNNING':
            raise OneError(f"OpenNebula OneKE service is not 'RUNNING': {state}")

        # Check VMs status
        vm_ids = {
            node['vm_info']['VM']['ID']
            for role in _service_json[str(service_id)]['TEMPLATE']['BODY']['roles']
            for node in role['nodes']
        }
        self._check_vms_status(vm_ids)
    

    def _instantiate_oneke(self, service_template_id, oneke_config, oneke_config_path):
        # TODO: create private network if not passed

        # Instantiate OneKE (with JSON or oneke_config parameters)
        logger.info("Instantiating OpenNebula OneKE service")
        if oneke_config_path is not None: 
            _json = self.client.templatepool[service_template_id].instantiate(path=oneke_config_path)
        else:  
            oneke_json = json.loads(oneke_config)
            _json = self.client.templatepool[service_template_id].instantiate(json_str=oneke_json)

        # Get service_id from JSON
        service_id = list(_json.keys())[0]
        logger.info(f"OneKE service ID: {service_id}")
        return service_id


    def _wait_for_oneke(self, service_id, timeout):
        start_time = time.time()
        minutes_timeout = int(timeout/60)
        logger.info(
            f"Waiting for OneKE service to become 'RUNNING'. "
            f"Be patient, this process can take up to {minutes_timeout} minutes"
        )
        while True:
            _service_json = self.client.servicepool[service_id].info()
            logs = _service_json[str(service_id)]['TEMPLATE']['BODY'].get('log', [])
            if logs:
                state = self._get_latest_state(logs)
                # Check OneKE deployment status
                if state == 'FAILED_DEPLOYING':
                    raise OneError("OneKE deployment has failed")
                elif state == 'RUNNING':
                    break

            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise OneError(
                    f"Deployment timed out after {timeout} seconds. "
                    f"You can try again once OneKE is in RUNNING state with the service_id option"
                )
            time.sleep(10)
        logger.info(f"OneKE service is RUNNING after {int(elapsed_time)} seconds")


    def _get_kube_config(self, service_id):
        # Get master VM ID
        master_vm_id = None
        _service_json = self.client.servicepool[service_id].info()
        roles = _service_json[str(service_id)]['TEMPLATE']['BODY']['roles']
        for role in roles:
            if role['name'] == 'master':
                master_vm_id = role['nodes'][0]['vm_info']['VM']['ID']
                break
        if master_vm_id is None:
            raise OneError(
                "Master VM ID not found. "
                "Please change the name of the master node to 'master' and try again"
            )
        # Get kubeconfig
        vm = self.pyone.vm.info(int(master_vm_id))
        encoded_kubeconfig = vm.USER_TEMPLATE.get('ONEKE_KUBECONFIG')
        decoded_kubeconfig = base64.b64decode(encoded_kubeconfig).decode('utf-8')
        logger.debug(f"OpenNebula OneKE Kubeconfig: {decoded_kubeconfig}")
        return decoded_kubeconfig
    

    def _get_latest_state(self, logs):
        for log in reversed(logs):
            if 'New state:' in log['message']:
                return log['message'].split(':')[-1].strip()
        return None


    def _check_vms_status(self, vm_ids):
        if len(vm_ids) == 0:
            # TODO: scale up OneKE VMs to default size
            raise OneError("No VMs found in OneKE service")
        for vm_id in vm_ids:
            vm = self.pyone.vm.info(int(vm_id))
            state = vm.STATE
            lcm_state = vm.LCM_STATE
            if state != 3 or lcm_state != 3:
                state_desc = STATE.get(state, "UNKNOWN_STATE")
                lcm_state_desc = LCM_STATE.get(lcm_state, "UNKNOWN_LCM_STATE")
                raise OneError(
                    f"VM {vm_id} fails validation: "
                    f"STATE={state_desc} (code {state}), "
                    f"LCM_STATE={lcm_state_desc} (code {lcm_state})"
                )