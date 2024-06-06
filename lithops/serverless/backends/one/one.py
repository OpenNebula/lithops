from ..k8s.k8s import KubernetesBackend

import os
import json
import logging
import urllib3
import oneflow

logger = logging.getLogger(__name__)
urllib3.disable_warnings()


class OneError(Exception):
    pass


class OpenNebula(KubernetesBackend):
    """
    A wrap-up around OpenNebula backend.
    """
    def __init__(self, one_config, internal_storage):
        logger.debug("Initializing OpenNebula backend")
        self.client = oneflow.OneFlowClient()

        # template_id: instantiate OneKE
        if 'template_id' in one_config:
            logger.info(one_config['template_id'])
            service_id = self._instantiate_oneke(one_config['template_id'], one_config['oneke_config'])
            self._wait_for_oneke(service_id)
        # service_id: check deployed OneKE is available
        elif 'service_id' in one_config:
            self._check_oneke(one_config['service_id'])
        else:
            raise OneError(f"OpenNebula backend must contain 'template_id' or 'service_id'")
        

        # Overwrite config values
        self.name = 'one'

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
        logger.info(oneke_config)
        
        #tmp_file_path = '/tmp/oneke_config.json'
        #with open(tmp_file_path, 'w') as f:
            #json.dump(oneke_config, f)

        # Pass the temporary file path to the update() function
        _json = self.client.templatepool[template_id].instantiate(json_str=oneke_config)

        # Remove the temporary file after use
        #os.remove(tmp_file_path)
        

        # Get service_id from JSON
        logger.info("JSON: {}".format(_json))

        pass


    def _wait_for_oneke(self, service_id):
        # TODO: wait for all the VMs
        
        # TODO: look onegate connectivity
        pass