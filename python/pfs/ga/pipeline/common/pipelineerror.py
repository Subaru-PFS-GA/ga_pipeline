class PipelineError(Exception):
    def __init__(self, message):
        super(PipelineError, self).__init__(message)

    def __str__(self):
        return str(self.message)