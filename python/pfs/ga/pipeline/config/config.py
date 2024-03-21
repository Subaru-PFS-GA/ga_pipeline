class Config():
    """Base class for configurations"""

    def __init__(self, config=None):
        self.load(config)

    # TODO: add json/yaml dump and load functions

    def load(self, source):
        if source is None:
            pass
        elif isinstance(source, dict):
            self._load_impl(config=source)
        else:
            raise NotImplementedError()
        
    def _load_config_from_dict(self, config=None, type_map=None):
        if config is not None:
            for k in config:
                if not hasattr(self, k):
                    raise ValueError(f'Member `{k}` of class `{type(self).__name__}` does not exist.')
                
                c = getattr(self, k)
                if isinstance(c, Config):
                    c._load_impl(config[k])
                elif type_map is not None and k in type_map:
                    setattr(self, k, self.map_config_class(type_map[k], config=config[k]))
                else:
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