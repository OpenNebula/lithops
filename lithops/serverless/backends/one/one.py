from ..k8s.k8s import KubernetesBackend

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

    # Reading the `one_auth` file.
    # The `one_auth` file path is given in the environment variable
    # `ONE_AUTH` if exists, otherwise it is in `$HOME/.one/one_auth`.
    auth_path = env.get('ONE_AUTH') or os.path.expanduser('~/.one/one_auth')
    with open(auth_path, mode='r') as auth_file:
        credentials = auth_file.readlines()[0].strip()

    # Reading environment variables.
    # Environment variable `ONESERVER_URL` superseeds the default URL.
    url = env.get('ONESERVER_URL', 'http://localhost:2633/RPC2')

    return pyone.OneServer(url, session=credentials)    


class OpenNebula(KubernetesBackend):
    """
    A wrap-up around OpenNebula backend.
    """
    def __init__(self, one_config, internal_storage):
        logger.debug("Initializing OpenNebula backend")

        logger.debug("Initializing Oneflow python client")
        self.client = oneflow.OneFlowClient()

        logger.debug("Initializing OpenNebula python client")
        self.one = _config_one()

        # template_id: instantiate master node
        if 'template_id' in one_config:
            one_config['service_id'] = self._instantiate_oneke(one_config['template_id'], one_config['oneke_config'])
            self._wait_for_oneke(one_config['service_id'],  one_config['timeout'])
        # service_id: check deployed OneKE is available
        elif 'service_id' in one_config:
            self._check_oneke(one_config['service_id'])
        else:
            raise OneError(f"OpenNebula backend must contain 'template_id' or 'service_id'")
        
        # Get and Save kubeconfig from OneKE
        kubecfg = self._get_kube_config(one_config['service_id'])
        with open(one_config['kubecfg_path'], 'w') as file:
            file.write(kubecfg)

        # Overwrite config values
        self.name = 'one'
        self.kubecfg_path = one_config['kubecfg_path']

        super().__init__(one_config, internal_storage)
    

    def invoke(self, docker_image_name, runtime_memory, job_payload):
        super().invoke(docker_image_name, runtime_memory, job_payload)
    
    
    def clear(self, job_keys=None):
        # First, we clean Kubernetes jobs
        super().clear(all)

        # TODO: if all are deteleted -> suspend OneKE VMs (scale down) and
        #       delete them after X minutes
        pass


    def _check_oneke(self, service_id):
        # CASE1: client has created their own OneKE cluster
        # CASE2: OneKE cluster was created by lithops (with or without JSON file) 
        pass
    

    def _instantiate_oneke(self, template_id, oneke_config):
        # TODO: create private network if not passed

        # Instantiate OneKE
        oneke_json = json.loads(oneke_config)
        _json = self.client.templatepool[template_id].instantiate(json_str=oneke_json)

        # Get service_id from JSON
        service_id = list(_json.keys())[0]
        logger.debug("OneKE service ID: {}".format(service_id))
        return service_id


    def _wait_for_oneke(self, service_id, timeout):
        start_time = time.time()
        minutes_timeout = int(timeout/60)
        logger.debug("Initializing OneKE service. Be patient, this process can take up to {} minutes".format(minutes_timeout))
        while True:
            _service_json = self.client.servicepool[service_id].info()
            logs = _service_json[service_id]['TEMPLATE']['BODY'].get('log', [])
            if logs:
                last_log = logs[-1]
                logger.debug("Last log: {}".format(last_log))
                state = last_log['message'].split(':')[-1].strip()
                # Check OneKE deployment status
                if state == 'FAILED_DEPLOYING':
                    raise OneError("OneKE deployment has failed")
                if state == 'RUNNING':
                    logger.debug("OneKE is running")
                    break
            
            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise OneError("Deployment timed out after {} seconds. You can try again once OneKE is in RUNNING state with the service_id option.".format(timeout))
            
            time.sleep(10)


    def _get_kube_config(self, service_id):
        # Get master VM ID
        _service_json = self.client.servicepool[service_id].info()
        master_vm_id = next(
            (role['nodes'][0]['vm_info']['VM']['ID'] for role in _service_json[str(service_id)]['TEMPLATE']['BODY']['roles'] 
            if role['name'] == 'master'), 
            None
        )
        if master_vm_id is None:
            raise OneError("Master VM ID not found. Please change the name of the master node to 'master' and try again.")
        # Get kubeconfig
        vm = self.one.vm.info(int(master_vm_id))
        encoded_kubeconfig = vm.USER_TEMPLATE.get('ONEKE_KUBECONFIG')
        decoded_kubeconfig = base64.b64decode(encoded_kubeconfig).decode('utf-8')
        logger.debug("OpneNebula OneKE Kubeconfig: {}".format(decoded_kubeconfig))
        return decoded_kubeconfig