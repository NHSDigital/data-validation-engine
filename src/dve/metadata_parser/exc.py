"""Exceptions around entities and for use in validators"""


class EntityNotFoundError(KeyError):
    """Error for missing entities"""


class LocWarning(UserWarning):
    """Warning class with optional location parameter"""

    def __init__(self, msg, loc=""):
        self.msg = msg
        self.loc = loc
        super().__init__()

    def __str__(self):
        """string representation of a loc warning"""
        return f"{self.loc} {self.msg}"


class RecordRejection(ValueError):
    """Exception to indicate a record has been rejected"""


class FileRejection(ValueError):
    """Exception to indicate a file has been rejected"""


class GroupRejection(ValueError):
    """Exception to indicate a chain of records have been rejected
    normally used in a referential integrity check or for duplicated records
    """


class TypeNotFoundError(KeyError):
    """Exception raised when unable to locate correct type from passed in metadata"""


ERRORS = {
    "recordrejection": RecordRejection,
    "filerejection": FileRejection,
    "warning": LocWarning,
    "grouprejection": GroupRejection,
}
