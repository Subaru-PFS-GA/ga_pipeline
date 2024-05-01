class Config():
    """Base class for configurations"""

    def __init__(self, config=None):
        self.load(config)

    # TODO: add json/yaml dump and load functions

    def load(self, source):
        # Load configuration from a dictionary or other sources (files)

        if source is None:
            pass
        elif isinstance(source, dict):
            self._load_impl(config=source)
        else:
            raise NotImplementedError()
        
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
                    # TODO: merge dictionaries recursively?
                    setattr(self, k, { **c, **config[k] })
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