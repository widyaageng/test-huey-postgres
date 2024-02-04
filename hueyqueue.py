import time
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler

from huey import SqliteHuey, crontab, RedisHuey
from huey.api import Task
from huey.signals import SIGNAL_CANCELED, SIGNAL_COMPLETE, SIGNAL_ERROR, SIGNAL_EXECUTING, SIGNAL_EXPIRED, \
    SIGNAL_INTERRUPTED, SIGNAL_LOCKED, SIGNAL_REVOKED, SIGNAL_RETRYING, SIGNAL_SCHEDULED
from huey_postgre import PostgreHuey
# huey = SqliteHuey(filename='huey.db', timeout=0.1)
# huey = RedisHuey(
#     'huey',
#     host='192.168.192.1',
#     port=6379,
#     db=0,
#     results=True,
#     result_store='redis')
metric = {
    "success_task": 0,
    "success_periodic": 0
}
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix=False,
    settings_files=['settings.toml'],
    load_dotenv=True,
)
huey = PostgreHuey(
                database = settings.postgres.huey_db,
                user = settings.postgres.huey_user,
                host = settings.postgres.huey_host,
                password = settings.postgres.huey_password,
                port = int(settings.postgres.huey_port))
huey.flush()

huey_file_format = "TESTHUEY: [%(asctime)s] [%(levelname)s] [%(filename)s: %(lineno)s] : %(message)s"
huey_console_format = "TESTHUEY: [%(asctime)s] [%(levelname)s] [%(filename)s: %(lineno)s] : %(message)s"
logger = logging.getLogger("huey")
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler("huey.log", maxBytes=10**6, backupCount=5, mode='a')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(huey_file_format))

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter(huey_console_format))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

@huey.periodic_task(crontab(minute='*/1'), retry_delay=3, delay=3)
@huey.on_startup()
def repeat_task():
    logger.info("repeat_task: executing task")
    time.sleep(0.1)
    logger.info("add_task: HUEY DONE REPEAT WORKING")
    metric["success_periodic"] += 1
    msf = f"METRIC: stats {metric} "
    logger.info(msf)
    return 'HUEY REPEAT WORKING'

@huey.task()
def add_task():
    logger.info("add_task: executing task")
    time.sleep(0.1)
    logger.info("add_task: HUEY DONE WORKING")
    metric["success_task"] += 1
    msf = f"METRIC: stats {metric} "
    logger.info(msf)
    return 'HUEY DONE WORKING'


@huey.signal(SIGNAL_SCHEDULED, SIGNAL_EXECUTING)
def task_commenced(signal, task: Task, exc=None, **kwargs) -> None:
    """
    This signal handler will be called for task commencement
    Reserved for future usage
    """
    msg = f"INFO: {task.name} task is commencing or executing"
    logger.info(msg)
    pass


@huey.signal(
        SIGNAL_ERROR,
        SIGNAL_REVOKED,
        SIGNAL_LOCKED,
        SIGNAL_EXPIRED,
        SIGNAL_INTERRUPTED,
        SIGNAL_CANCELED)
def task_not_executed_handler(signal, task: Task, exc=None, metric: dict = metric, **kwargs) -> None:
    """
    Signal handler is called for SIGNAL_ERROR, SIGNAL_LOCKED, SIGNAL_REVOKED.
    Reserved for future usage
    """
    msg = f"ERROR: {task.name} task failed to execute"
    logger.error(msg)
    pass


@huey.signal(SIGNAL_RETRYING)
def task_retrying(signal, task, **kwargs) -> None:
    """
    Signal handler is called for SIGNAL_RETRYING.
    Reserved for future usage
    """
    msg = f"WARNING: {task.name} task is retrying"
    logger.warn(msg)
    pass


@huey.signal(SIGNAL_COMPLETE)
def task_success(signal, task: Task, metric: dict=metric, **kwargs) -> None:
    """
    Signal handler is called for SIGNAL_COMPLETE.
    Reserved for future usage
    """
    msg = f"SUCCESS: {task.name} task completed successfully"
    logger.info(msg)
    pass