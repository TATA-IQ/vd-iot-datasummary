import yaml

from yaml.loader import SafeLoader


class Config:
    def yamlconfig(path):
        with open(path, "r") as f:
            data = list(yaml.load_all(f, Loader=SafeLoader))
            print(data)
        return data
