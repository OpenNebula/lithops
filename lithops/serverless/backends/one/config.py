import enum

from lithops.serverless.backends.k8s.config import DEFAULT_CONFIG_KEYS
from lithops.serverless.backends.k8s.config import load_config as load_k8


class OneConfigError(Exception):
    pass


@enum.unique
class ServiceState(enum.Enum):
    RUNNING = 2
    SCALING = 9
    COOLDOWN = 10


# Add OpenNebula defaults
DEFAULT_CONFIG_KEYS.update(
    {
        "minimum_nodes": 1,
        "maximum_nodes": 3,
    }
)


def load_config(config_data):
    # Load default config
    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data["one"]:
            config_data["one"][key] = DEFAULT_CONFIG_KEYS[key]

    # Ensure 'k8s' key exists and is a dictionary
    if "k8s" not in config_data or config_data["k8s"] is None:
        config_data["k8s"] = {}
        config_data["k8s"]["docker_user"] = config_data["one"]["docker_user"]
        config_data["k8s"]["docker_password"] = config_data["one"]["docker_password"]

    # Load k8s default config
    load_k8(config_data)
