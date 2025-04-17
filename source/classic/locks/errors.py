class ResourceIsLocked(BaseException):
    """
    Исключение, возникающее при невозможности получить блокировку.
    
    Args:
        resource: Имя заблокированного ресурса
    """
    def __init__(self, resource: str):
        self.resource = resource
