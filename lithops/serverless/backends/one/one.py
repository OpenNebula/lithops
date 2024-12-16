import base64
import logging
import math
import time

import urllib3

from ..k8s.k8s import KubernetesBackend
from .config import ServiceState
from .gate import OneGateClient

logger = logging.getLogger(__name__)
urllib3.disable_warnings()


class OneError(Exception):
    pass


class OpenNebula(KubernetesBackend):
    """
    A wrap-up around OpenNebula backend.
    """

    def __init__(self, one_config, internal_storage):
        logger.info("Initializing OpenNebula backend")

        # Backend configuration
        self.name = "one"
        self.auto_scale = one_config["auto_scale"]
        self.runtime_cpu = float(one_config["runtime_cpu"])
        self.runtime_memory = float(one_config["runtime_memory"])
        self.worker_cpu = float(one_config["worker_cpu"])
        self.worker_memory = float(one_config["worker_memory"])
        self.job_clean = set()
        # TODO: get this values
        self.minimum_nodes = one_config["minimum_nodes"]
        self.maximum_nodes = one_config["maximum_nodes"]

        # Check OneKE service
        logger.debug("Initializing OneGate python client")
        self.client = OneGateClient()
        logger.info("Checking OpenNebula OneKE service status")
        self.service_id = self.client.get("service").get("SERVICE", {}).get("id")
        self._check_service_status()

        # Get and Save kubeconfig from OneKE
        kubecfg = self._get_kube_config()
        with open(one_config["kubecfg_path"], "w") as file:
            file.write(kubecfg)
        self.kubecfg_path = one_config["kubecfg_path"]

        super().__init__(one_config, internal_storage)

    def invoke(self, docker_image_name, runtime_memory, job_payload):
        # Wait for nodes to become available in Kubernetes
        for role in self.client.get("service").get("SERVICE", {}).get("roles", []):
            if role.get("name").lower() == "worker":
                oneke_workers = role.get("cardinality")
        self._wait_kubernetes_nodes(oneke_workers)

        # Scale nodes
        scale_nodes, pods, chunksize, worker_processes = self._granularity(
            job_payload["total_calls"]
        )
        if scale_nodes == 0 and len(self.nodes) == 0:
            raise OneError(
                "No nodes available and can not scale. Ensure nodes are active, "
                "detected by Kubernetes, and have enough resources to scale."
            )
        if scale_nodes > len(self.nodes) and self.auto_scale in {"all", "up"}:
            self._scale_oneke(self.nodes, scale_nodes)
            self._wait_kubernetes_nodes(scale_nodes)

        # Setup granularity
        job_payload["max_workers"] = pods
        job_payload["chunksize"] = chunksize
        job_payload["worker_processes"] = worker_processes
        return super().invoke(docker_image_name, runtime_memory, job_payload)

    def clear(self, job_keys=None):
        if not job_keys:
            return

        new_keys = set(job_keys) - self.job_clean
        if new_keys:
            self.job_clean.update(new_keys)
            super().clear(job_keys)
            super()._get_nodes()

            if (
                self.auto_scale in {"all", "down"}
                and len(self.nodes) > self.minimum_nodes
            ):
                self._scale_oneke(self.nodes, self.minimum_nodes)

    def _check_service_status(self):
        roles = self.client.get("service").get("SERVICE", {}).get("roles", [])
        failed_roles = []
        for role in roles:
            for node in role.get("nodes", []):
                vm_id = node.get("vm_info", {}).get("VM", {}).get("ID")
                vm = self.client.get(f"vms/{vm_id}")["VM"]
                if vm.get("STATE") != "3" or vm.get("LCM_STATE") != "3":
                    failed_roles.append(role.get("name"))
                    break
        if failed_roles:
            raise OneError(f"Non-running roles detected: {', '.join(failed_roles)}")

    def _get_kube_config(self):
        roles = self.client.get("service").get("SERVICE", {}).get("roles", [])
        master_role = next(
            (role for role in roles if role.get("name").lower() == "master"), None
        )
        if not master_role:
            raise OneError(
                "Master VM not found. Ensure OneKE master node role is named 'master'"
            )
        master_id = (
            master_role.get("nodes", [])[0].get("vm_info", {}).get("VM", {}).get("ID")
        )
        master_vm = self.client.get(f"vms/{master_id}")["VM"]
        encoded_kubeconfig = master_vm.get("USER_TEMPLATE", {}).get(
            "ONEKE_KUBECONFIG", None
        )
        if not encoded_kubeconfig:
            raise OneError("Kubernetes configuration missing in the OneKE master VM.")
        decoded_kubeconfig = base64.b64decode(encoded_kubeconfig).decode("utf-8")
        logger.debug(f"OpenNebula OneKE Kubeconfig: {decoded_kubeconfig}")
        return decoded_kubeconfig

    def _wait_for_state(self, state, timeout=300):
        start_time = time.time()
        minutes_timeout = int(timeout / 60)
        logger.info(
            f"Waiting for OneKE service to become {state}. "
            f"Wait time: {minutes_timeout} minutes"
        )
        while (elapsed_time := time.time() - start_time) <= timeout:
            service_state = (
                self.client.get("service", {}).get("SERVICE", {}).get("state")
            )
            if service_state == ServiceState[state].value:
                logger.info(
                    f"OneKE service is {state} after {int(elapsed_time)} seconds."
                )
                return
            time.sleep(1)
        raise OneError(
            f"Unable to reach {state} state. OneKE timed out after {timeout} seconds. "
            f"Please retry when OneKE is in the 'RUNNING' state."
        )

    def _granularity(self, total_functions):
        workers_per_pod = (self.worker_cpu - 1) // self.runtime_cpu
        oneke_nodes = len(self.nodes)
        oneke_workers = oneke_nodes * workers_per_pod
        # Determine granularity
        nodes = (
            oneke_nodes
            if total_functions <= oneke_workers
            else oneke_nodes + (self.maximum_nodes - oneke_nodes)
        )
        workers = int(min(workers_per_pod, math.ceil(total_functions / nodes)))
        logger.info(
            f"Nodes: {nodes}, Pods: {nodes}, Chunksize: 1, Worker Processes: {workers}"
        )
        return nodes, nodes, 1, workers

    def _scale_oneke(self, nodes, scale_nodes):
        logger.info(f"Scaling workers from {len(nodes)} to {scale_nodes} nodes")
        # Ensure the service can be scaled
        service = self.client.get("service").get("SERVICE", {})
        if service.get("state") == ServiceState.COOLDOWN.value:
            if len(self.nodes) == 0:
                self._wait_for_state("RUNNING")
            else:
                logger.info(
                    "OneKE service is in 'COOLDOWN' state and does not need to be scaled"
                )
                return
        self.client.scale(int(scale_nodes))
        self._wait_for_state("COOLDOWN")

    def deploy_runtime(self, docker_image_name, memory, timeout):
        super()._get_nodes()
        if len(self.nodes) == 0:
            self._scale_oneke(self.nodes, 1)
        return super().deploy_runtime(docker_image_name, memory, timeout)

    def _wait_kubernetes_nodes(self, total, timeout=60):
        logger.info(f"Waiting for {total} Kubernetes nodes to become available")
        super()._get_nodes()
        start_time = time.time()
        while len(self.nodes) < total and time.time() - start_time < timeout:
            time.sleep(1)
            super()._get_nodes()
