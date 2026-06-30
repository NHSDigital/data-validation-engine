"""
Constants used within the error reports
"""

from enum import Enum


class ErrorReportStatus(Enum):
    """
    Constant to centrally hold error report status.
    """

    FILE_REJECTION = 1, "File Rejection"
    RECORD_REJECTION = 2, "Record Rejection"
    WARNING = 3, "Warning"

    @property
    def reporting_name(self):
        """
        The error report 'friendly' name.
        """
        return self.value[1]
