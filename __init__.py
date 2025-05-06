import asyncio

from fastapi import APIRouter
from loguru import logger

from .crud import db

from fastapi.staticfiles import StaticFiles

from lnbits.db import Database
from lnbits.helpers import template_renderer
from lnbits.tasks import catch_everything_and_restart

db = Database("ext_bets4sats")


bets4sats_ext: APIRouter = APIRouter(prefix="/bets4sats", tags=["Bets4Sats"])


bets4sats_static_files = [
    {
        "path": "/bets4sats/static",
        "name": "bets4sats_static",
    }
]

scheduled_tasks: list[asyncio.Task] = []

def bets4sats_stop() -> None:
    for task in scheduled_tasks:
        try:
            task.cancel()
        except Exception as ex:
            logger.warning(ex)

def bets4sats_renderer():
    return template_renderer(["lnbits/extensions/bets4sats/templates"])


from .tasks import (
    wait_for_paid_invoices,
    wait_for_reward_ticket_ids,
    purge_tickets_loop,
)
from . import views  # noqa: F401,E402
from . import views_api  # noqa: F401,E402


def bets4sats_start() -> None:
    from lnbits.tasks import create_permanent_unique_task

    task_inv = create_permanent_unique_task("ext_bets4sats_invoices", wait_for_paid_invoices)
    task_reward = create_permanent_unique_task("ext_bets4sats_rewards", wait_for_reward_ticket_ids)
    task_purge = create_permanent_unique_task("ext_bets4sats_purge", purge_tickets_loop)

    scheduled_tasks.extend([task_inv, task_reward, task_purge])

    __all__ = [
    "db",
    "bets4sats_ext",
    "bets4sats_static_files",
    "bets4sats_start",
    "bets4sats_stop",
]
