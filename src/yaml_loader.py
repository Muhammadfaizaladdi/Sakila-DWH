import yaml

def yaml_loader(filepath):
    """Loads a yaml file"""
    with open(filepath, "r") as file_descriptor:
        data = yaml.load(file_descriptor, Loader=yaml.FullLoader)
    return data