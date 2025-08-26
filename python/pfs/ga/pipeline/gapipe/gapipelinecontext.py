from types import SimpleNamespace

class GAPipelineContext(SimpleNamespace):
    def __init__(self, /, **kwargs):
        super().__init__(**kwargs)