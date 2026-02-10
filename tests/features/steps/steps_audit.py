from behave import given, then
from behave.runner import Context
from context_tools import get_pipeline, get_submission_info


@given("I add initial audit entries for the submission")
def add_audit_entries(context: Context):
    sub_info = get_submission_info(context)
    pipeline = get_pipeline(context)
    pipeline._audit_tables.add_new_submissions([sub_info])
    pipeline._audit_tables.mark_transform([sub_info.submission_id])


@then("the latest audit record for the submission is marked with processing status {status}")
def check_latest_audit_record_for_submission(context: Context, status: str):
    pipeline = get_pipeline(context)
    proc_status = pipeline._audit_tables.get_current_processing_info(
        get_submission_info(context).submission_id
    )
    assert proc_status.processing_status == status, proc_status.processing_status
