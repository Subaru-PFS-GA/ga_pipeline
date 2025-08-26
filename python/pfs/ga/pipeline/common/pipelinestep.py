from collections import namedtuple

# Create a named tuple for the pipeline step
PipelineStepResults = namedtuple('StepResults', ['success', 'skip_remaining', 'skip_substeps'])

class PipelineStep():
    def __init__(self, name=None):
        pass
