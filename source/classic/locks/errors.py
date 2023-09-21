class ResourceIsLocked(BaseException):
    def __init__(self, resource: str):
        self.resource = resource
