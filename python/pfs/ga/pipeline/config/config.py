import os
import numpy as np
import commentjson as json
import yaml

from ..setup_logger import logger
from .configjsonencoder import ConfigJSONEncoder
from .configyamlencoder import *

class Config():
    """Base class for configurations"""

    def __init__(self):
        self.__config_files = None                # List of configuration files loaded

    def __get_config_files(self):
        return self.__config_files

    config_files = property(__get_config_files)

    def _get_env(self, name, default=None):
        if name in os.environ and os.environ[name] is not None and os.environ[name] != '':
            return os.environ[name]
        else:
            return None

    # TODO: add json/yaml dump and load functions

    def load(self, source, ignore_collisions=False):
        # Load configuration from a dictionary or other sources (files)

        if source is None:
            pass
        elif isinstance(source, dict):
            # Configuration is a dictionary, no need to read from a file
            self._load_impl(config=source, ignore_collisions=ignore_collisions)
        elif isinstance(source, str) or isinstance(source, list):
            # Configuration to be loaded from a single file or from a list of files
            # When loading from a list of files, the configuration is merged

            # Keep track of config files used
            if self.__config_files is None:
                self.__config_files = []

            source = source if isinstance(source, list) else [ source ]
            for s in source:
                config = Config.load_dict(s)
                self._load_impl(config=config, ignore_collisions=ignore_collisions)
                self.__config_files.append(os.path.abspath(s))
        else:
            raise NotImplementedError()
        
    def save(self, path):
        # Save configuration to a file

        config = self._save_impl()
        Config.save_dict(config, path)

    def as_dict(self):
        # Return the configuration as a dictionary
        return self._save_impl()
        
    @staticmethod
    def load_dict(path):
        # Depending on the file extension, load the configuration file
        dir, filename = os.path.split(path)
        _, ext = os.path.splitext(filename)
        if ext == '.py':
            config = Config.__load_dict_py(path)
        elif ext == '.json':
            config = Config.__load_dict_json(path)
        elif ext == '.yaml':
            config = Config.__load_dict_yaml(path)
        else:
            raise ValueError(f'Unknown configuration file extension `{ext}`')
        
        return config
        
    @staticmethod
    def __load_dict_py(filename):
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
    
    @staticmethod
    def __load_dict_json(filename):
        # Load configuration from a JSON file with comments

        with open(filename, 'r') as f:
            config = json.load(f)
        return config
    
    @staticmethod
    def __load_dict_yaml(filename):
        # Load configuration from a YAML file

        with open(filename, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    @staticmethod
    def save_dict(config, path):
        # Depending on the file extension, save the configuration file
        dir, filename = os.path.split(path)
        _, ext = os.path.splitext(filename)
        if ext == 'py':
            Config.__save_dict_py(config, path)
        if ext == '.json':
            Config.__save_dict_json(config, path)
        elif ext == '.yaml':
            Config.__save_dict_yaml(config, path)
        else:
            raise ValueError(f'Unknown configuration file extension `{ext}`')
        
    @staticmethod
    def __save_dict_py(config, filename):
        # Save a python file with the configuration
        raise NotImplementedError()

    @staticmethod
    def __save_dict_json(config, filename):
        # Save configuration to a JSON file with comments
        with open(filename, 'w') as f:
            json.dump(config, f, sort_keys=False, cls=ConfigJSONEncoder)

    @staticmethod
    def __save_dict_yaml(config, filename):
        # Save configuration to a YAML file
        with open(filename, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
    @staticmethod
    def merge_dict(a: dict, b: dict, ignore_collisions=False):
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
                r[k] = Config.merge_dict(a[k], b[k], ignore_collisions=ignore_collisions)
            elif k in a and k in b:
                msg = f"Collision detected in the configuration for key `{k}`."
                if ignore_collisions:
                    r[k] = b[k]
                    logger.debug(msg)
                else:
                    raise ValueError(msg)
            elif k in a:
                r[k] = a[k]
            elif k in b:
                r[k] = b[k]

        return r
    
    @staticmethod
    def copy_dict(a: dict):
        # Make a deep copy of a dictionary
        r = {}
        for k in a.keys():
            if isinstance(a[k], dict):
                r[k] = Config.copy_dict(a[k])
            elif isinstance(a[k], list):
                r[k] = [ Config.copy_dict(c) for c in a[k] ]
            else:
                r[k] = a[k]
        return r
        
    def _load_config_from_dict(self, config=None, type_map=None, ignore_collisions=False):
        """
        Load configuration from a dictionary.

        This function iterates over the dictionary passed as `config` and sets the members
        of the configuration class to the values found in the dictionary. If a member is a subclass
        of `Config`, the value found in the dictionary (usually a sub-dictionary) is passed to the class
        for further processing. If key in the dictionary matches a key in `type_map`, then the value
        is passed to the class defined in the map.
        
        If the member is a dictionary and the value in the dictionary is also a dictionary, the two
        dictionaries are merged. If the member is a dictionary and the value in the dictionary is not
        a dictionary, the value is set as is. If the member is not a dictionary, the value is set as is.
        """

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
                    c._load_impl(config[k], ignore_collisions=ignore_collisions)
                elif type_map is not None and k in type_map:
                    # This member is part of the type_map, instantiate type and initialize
                    setattr(self, k, self.map_config_class(type_map[k], config=config[k], ignore_collisions=ignore_collisions))
                elif isinstance(c, dict) and isinstance(config[k], dict):
                    # This member is a dictionary, merge with the config dict
                    setattr(self, k, self.merge_dict(c, config[k], ignore_collisions=ignore_collisions))
                else:
                    # This is a regular member, just set its value
                    setattr(self, k, config[k])

    def _load_impl(self, config=None, ignore_collisions=False):
        """
        Config class specific implementation of how to load the configuration from
        a dictionary.
        
        When the dictionary is hierarchical, some entries might need to
        be converted into their own classes, while others can be left as is. To allow
        control of this behavior, derived classes can override this function and
        pass a `type_map` to `_load_config_from_dict`.
        """

        # The default is not to use type mapping
        self._load_config_from_dict(config=config, ignore_collisions=ignore_collisions)

    def map_config_class(self, type, config=None, ignore_collisions=False):
        if isinstance(config, dict):
            res = {}
            for k, c in config.items():
                v = type()
                v.load(c, ignore_collisions=ignore_collisions)
                res[k] = v
            return res
        elif isinstance(config, list):
            res = []
            for c in config:
                v = type()
                v.load(c, ignore_collisions=ignore_collisions)
                res.append(c)
            return res
        else:
            v = type()
            v.load(config, ignore_collisions=ignore_collisions)
            return v
        
    @staticmethod
    def _config_to_dict(obj):
        # Save configuration to a dictionary

        config = {}
        for k in obj.__dict__:
            if not k.startswith('_'):
                v = getattr(obj, k)
                config[k] = Config.__obj_to_dict(v)
        return config

    @staticmethod
    def __obj_to_dict(obj):
        if isinstance(obj, Config):
            return Config._config_to_dict(obj)
        elif isinstance(obj, np.ndarray):
            return [ Config.__obj_to_dict(v) for v in obj.tolist() ]
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, dict):
            return { k: Config.__obj_to_dict(v) for k, v in obj.items() }
        elif isinstance(obj, list):
            return [ Config.__obj_to_dict(v) for v in obj ]
        else:
            return obj

    def _save_impl(self):
        return self._config_to_dict(self)