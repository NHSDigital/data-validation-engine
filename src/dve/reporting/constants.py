"""
Constants used within the error reports
"""

from enum import Enum


class ErrorReportCategories(Enum):
    """
    Categories available within the error report.
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


class ErrorReportStatus:  # pylint: disable=R0903
    """
    Statuses available within the error report.
    """

    PROCESSING_FAILED = "There was an issue processing the submission. Please contact support."
    FILE_REJECTION = "File has been rejected"
    RECORD_REJECTION = "File has been accepted with record rejections"
    ACCEPTED_WITH_WARNING = "File has been accepted, all records accepted with warnings"
    ACCEPTED = "File has been accepted, no issues to report"
