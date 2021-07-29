from config import huey
from huey import crontab, CancelExecution
import shutil
from loguru import logger
import os
from dotenv import load_dotenv
from Runs import RunFolder, Nextseq
load_dotenv()

TOOL_NAME = os.getenv("TOOL_NAME")
BACKUP_LOCATION = os.getenv("BACKUP_LOCATION")

NEXTSEQ = {
    "NB501819": "/nextseq",
    "NB501061": "/nextseq-nb501061",
}


@huey.pre_execute()
def check_tool_exists(task):
    """
    Check whether the tool_name is present
    """
    if shutil.which(TOOL_NAME) is None:
        logger.critical(f"{TOOL_NAME} is not existed!")
        raise CancelExecution("Exit!")

@huey.task()
@huey.lock_task('monitor_lock')
def _monitor(machine_name):
    NS = Nextseq(machine_name, NEXTSEQ[machine_name], BACKUP_LOCATION)
    if NS.path_accessible:
        latest_run_backup_location = RunFolder(NS.latest_run_name, BACKUP_LOCATION)
        logger.info(
            f"{latest_run_backup_location.name}: {list(set(latest_run_backup_location.count_bgzf()))}")
        if not (latest_run_backup_location.is_existed and latest_run_backup_location.is_fully_copied):
            NS.copy()
        else:
            logger.critical(f"{latest_run_backup_location.name} is finished!")
    else:
        logger.critical(f"{NS.machine_path} is not available.")


@huey.periodic_task(crontab(minute='*/1'))
def monitor_nextseq():
    machines = list(NEXTSEQ.keys())
    _monitor.map(machines)
