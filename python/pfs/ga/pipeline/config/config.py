import os
import commentjson
import yaml

class Config():
    """Base class for configurations"""

    def __init__(self, config=None):
        self.load(config)

    def _get_env(self, name, default=None):
        if name in os.environ and os.environ[name] is not None and os.environ[name] != '':
            return os.environ[name]
        else:
            return None

    # TODO: add json/yaml dump and load functions

    def load(self, source):
        # Load configuration from a dictionary or other sources (files)

        if source is None:
            pass
        elif isinstance(source, dict):
            self._load_impl(config=source)
        elif isinstance(source, str):
            # Configuration to be loaded from a file
            # Depending on the file extension, load the configuration file
            dir, filename = os.path.split(source)
            _, ext = os.path.splitext(filename)
            if ext == '.py':
                config = self.__load_config_py(source)
            elif ext == '.json':
                config = self.__load_config_json(source)
            elif ext == '.yaml':
                config = self.__load_config_yaml(source)
            else:
                raise ValueError(f'Unknown configuration file extension `{ext}`')
            
            self._load_impl(config=config)
        else:
            raise NotImplementedError()
        
    def __load_config_py(self, filename):
        # Load a python file with the configuration and execute it to get
        # the configuration dictionary

        with open(filename, 'r') as f:
            code = f.read()

        global_variables = {}
        local_variables = {}
        exec(code, global_variables, local_variables)

        if 'config' in local_variables:
            return local_variables['config']
        else:
            raise ValueError(f'Configuration not found in file `{filename}`')
    
    def __load_config_json(self, filename):
        # Load configuration from a JSON file with comments

        with open(filename, 'r') as f:
            config = commentjson.load(f)
        return config
    
    def __load_config_yaml(self, filename):
        # Load configuration from a YAML file

        with open(filename, 'r') as f:
            config = yaml.safe_load(f)
        return config
        
    def __merge_dict(self, a: dict, b: dict, ignore_collisions=False):
        """
        Deep-merge two dictionaries. This function will merge the two dictionaries
        recursively. If a key is present in both dictionaries, the value will be 
        merged recursively, unless a collision is detected.
        """

        kk = list(a.keys()) + list(b.keys())

        r = {}
        for k in kk:
            # Both are dictionaries, merge them
            if k in a and isinstance(a[k], dict) and k in b  and isinstance(b[k], dict):
                r[k] = self.__merge_dict(a[k], b[k], ignore_collisions=ignore_collisions)
            elif k in a and k in b:
                if ignore_collisions:
                    r[k] = a[k]
                else:
                    raise ValueError(f"Collision detected for key {k}")
            elif k in a:
                r[k] = a[k]
            elif k in b:
                r[k] = b[k]

        return r
        
    def _load_config_from_dict(self, config=None, type_map=None):
        # Load configuration from a dictionary

        if config is not None:
            # Iterate over all keys of the configuration and see if the keys match up with
            # member variables of the configuration class
            for k in config:
                if not hasattr(self, k):
                    raise ValueError(f'Member `{k}` of class `{type(self).__name__}` does not exist.')
                
                # If the member is found and it's a subclass of `Config`, just pass the dictionary
                # to it for further processing. If the member is found but its value if not a subclass
                # of `Config` but its name is in `type_map`, then instantiate the particular type defined
                # in the map. In all other cases, just set the member to the value found in the config dict.

                c = getattr(self, k)
                if isinstance(c, Config):
                    # This is a config class, it can initialize itself from the config dict
                    c._load_impl(config[k])
                elif type_map is not None and k in type_map:
                    # This member is part of the type_map, instantiate type and initialize
                    setattr(self, k, self.map_config_class(type_map[k], config=config[k]))
                elif isinstance(c, dict) and isinstance(config[k], dict):
                    # This member is a dictionary, merge with the config dict
                    setattr(self, k, self.__merge_dict(c, config[k]))
                else:
                    # This is a regular member, just set its value
                    setattr(self, k, config[k])

    def _load_impl(self, config=None):
        # The default is not to use type mapping
        self._load_config_from_dict(config=config)

    def map_config_class(self, type, config=None):
        if isinstance(config, dict):
            return { k: type(config=c) for k, c in config.items() }
        elif isinstance(config, list):
            return [ type(config=c) for c in config ]
        else:
            return type(config=config)