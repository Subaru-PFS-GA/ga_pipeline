class PipelineError(Exception):
    def __init__(self, message):
        super(PipelineError, self).__init__(message)

        self.message = message          # Required for logging

    def __str__(self):
        return str(self.message)