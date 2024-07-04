from ..k8s.k8s import KubernetesBackend
from .config import STATE, LCM_STATE

import oneflow
import pyone

import os
import re
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

        # Overwrite config values
        self.name = 'one'
        self.timeout = one_config['timeout']
        self.minimum_nodes = one_config['minimum_nodes']
        self.maximum_nodes = one_config['maximum_nodes']
        self.average_job_execution = one_config['average_job_execution']

        # Export environment variables
        os.environ['ONEFLOW_URL'] = one_config['oneflow_url']
        os.environ['ONESERVER_URL'] = one_config['oneserver_url']
        os.environ['ONE_AUTH'] = one_config['one_auth']

        logger.debug("Initializing Oneflow python client")
        self.client = oneflow.OneFlowClient()

        logger.debug("Initializing OpenNebula python client")
        self.pyone = _config_one()

        # service_template_id: instantiate master node
        if 'service_template_id' in one_config:
            self.service_id = self._instantiate_oneke(
                one_config['service_template_id'], 
                one_config['oneke_config'], 
                one_config['oneke_config_path']
            )
            self._wait_for_oneke('RUNNING')
        elif 'service_id' in one_config:
            self.service_id = one_config['service_id']
        else:
            raise OneError(
                "OpenNebula backend must contain 'service_template_id' or 'service_id'"
            )
        self._check_oneke()

        # Get and Save kubeconfig from OneKE
        kubecfg = self._get_kube_config()
        with open(one_config['kubecfg_path'], 'w') as file:
            file.write(kubecfg)
        self.kubecfg_path = one_config['kubecfg_path']

        super().__init__(one_config, internal_storage)
    

    def invoke(self, docker_image_name, runtime_memory, job_payload):
        super()._get_nodes()
        scale_nodes, pods, chunksize, worker_processes = self._granularity(
            job_payload['total_calls']
        )

        # Scale nodes
        if scale_nodes > len(self.nodes):
            self._scale_oneke(self.nodes, scale_nodes)

        # Setup granularity
        job_payload['max_workers'] = pods
        job_payload['chunksize'] = chunksize
        job_payload['worker_processes'] = worker_processes
        super().invoke(docker_image_name, runtime_memory, job_payload)
    

    def clear(self, job_keys=None):
        super().clear(job_keys)
        super()._get_nodes()
        self._scale_oneke(self.nodes, self.minimum_nodes)


    def _check_oneke(self):
        logger.info("Checking OpenNebula OneKE service status")

        # Check service status
        state = self._get_latest_state()
        if state != 'RUNNING':
            raise OneError(f"OpenNebula OneKE service is not 'RUNNING': {state}")

        # Check VMs status
        self._check_vms_status()
    

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


    def _wait_for_oneke(self, state):
        start_time = time.time()
        minutes_timeout = int(self.timeout/60)
        logger.info(
            f"Waiting for OneKE service to become {state}. "
            f"Wait time: {minutes_timeout} minutes"
        )
        while True:
            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > self.timeout:
                raise OneError(
                    f"Can't reach {state} state. OneKE timed out after {self.timeout} seconds. "
                    f"You can try again once OneKE is in `'RUNNING'` state with the `service_id` option"
                )

            # Check OneKE deployment status
            current_state = self._get_latest_state()
            if current_state == 'FAILED_DEPLOYING':
                raise OneError("OneKE deployment has failed")
            elif current_state == 'FAILED_SCALING':
                raise OneError("OneKE scaling has failed")
            elif current_state == state:
                break

            time.sleep(5)
        logger.info(f"OneKE service is {state} after {int(elapsed_time)} seconds")


    def _get_kube_config(self):
        # Get master VM ID
        master_vm_id = None
        _service_json = self.client.servicepool[self.service_id].info()
        roles = _service_json[str(self.service_id)]['TEMPLATE']['BODY']['roles']
        for role in roles:
            if role['name'] == 'master':
                master_vm_id = role['nodes'][0]['vm_info']['VM']['ID']
                break
        if master_vm_id is None:
            raise OneError(
                "Master VM ID not found. "
                "Please change the name of the master node role to 'master' and try again"
            )

        # Get kubeconfig
        vm = self.pyone.vm.info(int(master_vm_id))
        encoded_kubeconfig = vm.USER_TEMPLATE.get('ONEKE_KUBECONFIG')
        decoded_kubeconfig = base64.b64decode(encoded_kubeconfig).decode('utf-8')
        logger.debug(f"OpenNebula OneKE Kubeconfig: {decoded_kubeconfig}")
        return decoded_kubeconfig
    

    def _get_latest_state(self):
        _service_json = self.client.servicepool[self.service_id].info()
        logs = _service_json[str(self.service_id)]['TEMPLATE']['BODY'].get('log', [])
        for log in reversed(logs):
            if 'New state:' in log['message']:
                return log['message'].split(':')[-1].strip()
        raise OneError("No state found in logs")


    def _check_vms_status(self):
        _service_json = self.client.servicepool[self.service_id].info()
        vm_ids = {
            node['vm_info']['VM']['ID']
            for role in _service_json[str(self.service_id)]['TEMPLATE']['BODY']['roles']
            for node in role['nodes']
        }
        if len(vm_ids) == 0:
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


    def _granularity(self, total_functions):
        _host_cpu, _host_mem = self._one_resources()
        _node_cpu, _node_mem = self._node_resources()
        # OpenNebula available resources
        max_nodes_cpu = int(_host_cpu / _node_cpu)
        max_nodes_mem = int(_host_mem / _node_mem)
        # OneKE current available resources
        current_nodes = len(self.nodes)
        total_cpus_available = int(sum(float(node['cpu']) for node in self.nodes))
        # Set by the user, otherwise calculated based on OpenNebula available Resources
        max_nodes = min(max_nodes_cpu, max_nodes_mem) + current_nodes
        total_nodes = max_nodes if self.maximum_nodes == -1 else self.maximum_nodes
        
        if total_cpus_available > 0:
            best_time = (total_functions / total_cpus_available) * self.average_job_execution
        else:
            best_time = float('inf')
        best_nodes_needed = 0
        estimated_execution_time = (total_functions / total_cpus_available) * self.average_job_execution
        current_pods = total_cpus_available

        for additional_nodes in range(1, total_nodes - current_nodes + 1):
            new_total_cpus_available = total_cpus_available + (additional_nodes * int(_node_cpu))
            estimated_time_with_scaling = (total_functions / new_total_cpus_available) * self.average_job_execution
            total_creation_time = self._get_total_creation_time(additional_nodes)
            total_estimated_time_with_scaling = estimated_time_with_scaling + total_creation_time

            if total_estimated_time_with_scaling < best_time and new_total_cpus_available <= total_functions:
                best_time = total_estimated_time_with_scaling
                best_nodes_needed = additional_nodes
                current_pods = new_total_cpus_available
                estimated_execution_time = estimated_time_with_scaling

        nodes = current_nodes + best_nodes_needed
        pods = min(total_functions, current_pods)

        logger.info(
            f"Nodes: {nodes}, Pods: {pods}, Chunksize: 1, Worker Processes: 1"
        )
        logger.info(
            f"Estimated Execution Time (without creation): {estimated_execution_time:.2f} seconds"
        )
        return nodes, pods, 1, 1



    def _scale_oneke(self, nodes, scale_nodes):
        logger.info(f"Scaling workers from {len(nodes)} to {scale_nodes} nodes")
        # Ensure the service can be scaled
        state = self._get_latest_state()
        if state == 'COOLDOWN':
            if len(self.nodes) == 0:
                self._wait_for_oneke('RUNNING')
            else:
                logger.info("OneKE service is in 'COOLDOWN' state and does not need to be scaled")
                return
        self.client.servicepool[self.service_id].role["worker"].scale(int(scale_nodes))
        self._wait_for_oneke('COOLDOWN')


    def _one_resources(self):
        hostpool = self.pyone.hostpool.info()
        host = hostpool.HOST[0]
        
        total_cpu = host.HOST_SHARE.TOTAL_CPU
        used_cpu = host.HOST_SHARE.CPU_USAGE

        total_memory = host.HOST_SHARE.TOTAL_MEM
        used_memory = host.HOST_SHARE.MEM_USAGE

        one_cpu = (total_cpu - used_cpu)/100
        one_memory = (total_memory - used_memory)/1000
        logger.info(
            f"Available CPU: {one_cpu}, Available Memory: {one_memory}"
        )
        return one_cpu, one_memory


    def _node_resources(self):
        _service_json = self.client.servicepool[self.service_id].info()
        _service_roles = _service_json[str(self.service_id)]['TEMPLATE']['BODY']['roles']

        for role in _service_roles:
            if role['name'] == 'worker':
                vm_template_id = role['vm_template']
                break
        
        vm_template = self.pyone.template.info(int(vm_template_id)).TEMPLATE
        template_cpu = float(vm_template['CPU'])
        template_memory = float(vm_template['MEMORY'])
        logger.info(f"Template CPU: {template_cpu}, Template Memory: {template_memory}")
        return template_cpu, template_memory


    def _get_total_creation_time(self, additional_nodes):
        # TODO: monitor Scaling to set this value
        first_node_creation_time = 90
        additional_node_creation_time = 20

        if additional_nodes == 1:
            return first_node_creation_time
        return first_node_creation_time + (additional_nodes - 1) * additional_node_creation_time


    def _generate_runtime_meta(self, docker_image_name):
        super()._get_nodes()
        if len(self.nodes) == 0:
            self._scale_oneke(self.nodes, 1)
        super()._generate_runtime_meta(docker_image_name)