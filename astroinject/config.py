import yaml

def load_yaml_config(file_path):
    """Load a YAML file and return its content as a dictionary."""
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config