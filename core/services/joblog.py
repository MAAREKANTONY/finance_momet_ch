from core.models import JobLog


def log_info(job: str, message: str = "", scenario=None, symbol=None):
    JobLog.objects.create(level="INFO", job=job, message=message, scenario=scenario, symbol=symbol)


def log_error(job: str, message: str = "", traceback: str = "", scenario=None, symbol=None):
    JobLog.objects.create(level="ERROR", job=job, message=message, traceback=traceback, scenario=scenario, symbol=symbol)
