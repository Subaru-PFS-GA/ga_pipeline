class PipelineException(Exception):
    def __init__(self, message):
        super(PipelineException, self).__init__(message)

    def __str__(self):
        return str(self.message)