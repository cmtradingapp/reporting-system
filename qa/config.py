import os
import yaml


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "qa_config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
