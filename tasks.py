from config import huey
from huey import crontab, CancelExecution
import shutil
from loguru import logger
import os
from dotenv import load_dotenv
from Runs import RunFolder, Nextseq
from discord_webhook import DiscordWebhook as DWH
load_dotenv()

TOOL_NAME = os.getenv("TOOL_NAME")
BACKUP_LOCATION = os.getenv("BACKUP_LOCATION")

NEXTSEQ = {
    "NB501819": "/nextseq",
    "NB501061": "/nextseq-nb501061",
}

test_url = "https://discordapp.com/api/webhooks/764224137501212722/EtZcQ2HBU1apXJ5-eGwySsRUUK6rlTPJP2yRRqPiNOiQdajRtv1TDc3sUmoZg3cKfiCt"
nextseq_qib_url = "https://discord.com/api/webhooks/768133190979813427/nFuAtD97EEMit8mM1EEK1VvgFuIIA1eVz-Rqvm0UniLXEPULYVwLXnCY9d_eTWAyUYzc"

discord_urls = [test_url]

@huey.pre_execute()
def check_tool_exists(task):
    """
    Check whether the tool_name is present
    """
    if shutil.which(TOOL_NAME) is None:
        logger.critical(f"{TOOL_NAME} is not existed!")
        raise CancelExecution("Exit!")


@huey.task()
# @huey.lock_task('monitor_lock')
def _monitor(machine_name):
    NS = Nextseq(machine_name, NEXTSEQ[machine_name], BACKUP_LOCATION)
    if NS.path_accessible:
        latest_run_backup_location = RunFolder(NS.latest_run_name, BACKUP_LOCATION)

        if not latest_run_backup_location.is_existed:
            wh = DWH(discord_urls, username="Nextseq")
            wh_content = f"The nextseq **{machine_name}** has a new run kicked off: **{latest_run_backup_location.name}**"
            wh.content = wh_content
            wh.execute()

        if not (latest_run_backup_location.is_existed and latest_run_backup_location.is_fully_copied):
            NS.copy()
            logger.opt(ansi=True).info(
                f"{latest_run_backup_location.name}: <yellow>{list(set(latest_run_backup_location.count_bgzf()))}</>"
                )
        else:
            logger.critical(f"{latest_run_backup_location.name} is finished!")
    else:
        logger.critical(f"{NS.machine_path} is not available.")


@huey.periodic_task(crontab(minute='*/1'))
def monitor_nextseq():
    machines = list(NEXTSEQ.keys())
    _monitor.map(machines)
