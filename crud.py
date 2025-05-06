from __future__ import annotations

import json
import datetime
from datetime import timedelta, timezone
from typing import List, Optional, Union

import shortuuid
from lnbits.db import Database
from lnbits.helpers import urlsafe_short_hash

from .models import (
    ChoiceAmountSum,
    Competition,
    CreateCompetition,
    Ticket,
    UpdateCompetition,
)

# ---------------------------------------------------------------------------
# Database handle – one per extension (connection pooling handled by LNbits)
# ---------------------------------------------------------------------------

db = Database("ext_bets4sats")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INVOICE_EXPIRY = 15 * 60  # 15 minutes
TICKET_PURGE_TIME = INVOICE_EXPIRY + 10  # safety margin

# ---------------------------------------------------------------------------
# Ticket helpers
# ---------------------------------------------------------------------------


async def create_ticket(
    ticket_id: str,
    wallet: str,
    competition: str,
    amount: int,
    reward_target: str,
    choice: int,
) -> Ticket:
    """Insert a new ticket in *INITIAL* state and decrement available tickets."""

    await db.execute(
        """
        INSERT INTO bets4sats.tickets (id, wallet, competition, amount, reward_target, choice, state, reward_msat, reward_failure, reward_payment_hash)
        VALUES (:id, :wallet, :competition, :amount, :reward_target, :choice, :state, :reward_msat, :reward_failure, :reward_payment_hash)
        """,
        {
            "id": ticket_id,
            "wallet": wallet,
            "competition": competition,
            "amount": amount,
            "reward_target": reward_target,
            "choice": choice,
            "state": "INITIAL",
            "reward_msat": 0,
            "reward_failure": "",
            "reward_payment_hash": "",
        },
    )

    # -------------------------------------------------------------
    # Decrement amount_tickets atomically while competition in INITIAL
    # -------------------------------------------------------------
    while True:
        competitiondata = await get_competition(competition)
        assert competitiondata, "Couldn't get competition from ticket being created"
        if competitiondata.state != "INITIAL":
            break
        amount_tickets = competitiondata.amount_tickets - 1
        update_result = await db.execute(
            """
            UPDATE bets4sats.competitions
            SET amount_tickets = :amount_tickets
            WHERE id = :id AND amount_tickets = :old_amount_tickets AND state = 'INITIAL'
            """,
            {
                "amount_tickets": amount_tickets,
                "id": competition,
                "old_amount_tickets": competitiondata.amount_tickets,
            },
        )
        if update_result.rowcount:
            break

    ticket = await get_ticket(ticket_id)
    assert ticket, "Newly created ticket couldn't be retrieved"
    return ticket


async def purge_expired_tickets(competition_id: str) -> None:
    purge_time = datetime.datetime.now(timezone.utc) - datetime.timedelta(seconds=TICKET_PURGE_TIME)
    delete_result = await db.execute(
        f"""
        DELETE FROM bets4sats.tickets
        WHERE competition = :competition AND state = 'INITIAL' AND time < {db.timestamp_placeholder('time')}
        """,
        {"competition": competition_id, "time": purge_time.timestamp()},
    )

    if not delete_result.rowcount:
        return

    # Give back the freed capacity to the competition
    while True:
        competitiondata = await get_competition(competition_id)
        assert competitiondata, "Couldn't get competition data for tickets being purged"
        amount_tickets = competitiondata.amount_tickets + delete_result.rowcount
        update_result = await db.execute(
            """
            UPDATE bets4sats.competitions
            SET amount_tickets = :amount_tickets
            WHERE id = :id AND amount_tickets = :old_amount_tickets
            """,
            {
                "amount_tickets": amount_tickets,
                "id": competition_id,
                "old_amount_tickets": competitiondata.amount_tickets,
            },
        )
        if update_result.rowcount:
            break


async def cas_ticket_state(ticket_id: str, old_state: str, new_state: str) -> bool:
    update_result = await db.execute(
        """
        UPDATE bets4sats.tickets
        SET state = :new_state
        WHERE id = :id AND state = :old_state
        """,
        {"new_state": new_state, "id": ticket_id, "old_state": old_state},
    )
    return bool(update_result.rowcount)


async def set_ticket_funded(ticket_id: str) -> None:
    cas_success = await cas_ticket_state(ticket_id, "INITIAL", "FUNDED")
    if not cas_success:
        return

    ticket = await get_ticket(ticket_id)
    assert ticket, "Ticket not found after funding"

    # -------------------------------------------------------------
    # Update competition aggregates
    # -------------------------------------------------------------
    while True:
        competitiondata = await get_competition(ticket.competition)
        assert competitiondata, "Couldn't get competition from ticket being paid"
        if competitiondata.state != "INITIAL":
            break
        sold = competitiondata.sold + 1
        choices = json.loads(competitiondata.choices)
        choices[ticket.choice]["total"] += ticket.amount  # fixed variable name
        update_result = await db.execute(
            """
            UPDATE bets4sats.competitions
            SET sold = :sold, choices = :choices
            WHERE id = :id AND sold = :old_sold AND state = 'INITIAL'
            """,
            {
                "sold": sold,
                "choices": json.dumps(choices),
                "id": ticket.competition,
                "old_sold": competitiondata.sold,
            },
        )
        if update_result.rowcount:
            break


async def update_ticket(ticket_id: str, **kwargs) -> Ticket:
    setters = ", ".join([f"{field} = :{field}" for field in kwargs])
    params = {**kwargs, "id": ticket_id}
    await db.execute(
        f"UPDATE bets4sats.tickets SET {setters} WHERE id = :id",
        params,
    )
    ticket = await get_ticket(ticket_id)
    assert ticket, "Newly updated ticket couldn't be retrieved"
    return ticket


async def get_ticket(ticket_id: str) -> Optional[Ticket]:
    return await db.fetchone(
        "SELECT * FROM bets4sats.tickets WHERE id = :id",
        {"id": ticket_id},
        Ticket,
    )


async def get_tickets(wallet_ids: Union[str, List[str]]) -> List[Ticket]:
    if isinstance(wallet_ids, str):
        wallet_ids = [wallet_ids]
    q = ",".join([f"'{wallet_id}'" for wallet_id in wallet_ids])
    return await db.fetchall(
        f"SELECT * FROM bets4sats.tickets WHERE wallet IN ({q})",
        model=Ticket,
    )


async def delete_ticket(ticket_id: str) -> None:
    await db.execute("DELETE FROM bets4sats.tickets WHERE id = :id", {"id": ticket_id})


async def delete_competition_tickets(competition_id: str) -> None:
    await db.execute("DELETE FROM bets4sats.tickets WHERE competition = :competition", {"competition": competition_id})


# ---------------------------------------------------------------------------
# Competition helpers
# ---------------------------------------------------------------------------


async def create_competition(data: CreateCompetition) -> Competition:
    competition_id = urlsafe_short_hash()
    register_id = shortuuid.random()
    choices_json = json.dumps([
        {"title": choice["title"], "total": 0}
        for choice in json.loads(data.choices)
    ])
    await db.execute(
        """
        INSERT INTO bets4sats.competitions (
            id, wallet, register_id, name, info, banner, closing_datetime,
            amount_tickets, min_bet, max_bet, sold, choices, winning_choice, state
        ) VALUES (
            :id, :wallet, :register_id, :name, :info, :banner, :closing_datetime,
            :amount_tickets, :min_bet, :max_bet, 0, :choices, -1, 'INITIAL'
        )
        """,
        {
            "id": competition_id,
            "wallet": data.wallet,
            "register_id": register_id,
            "name": data.name,
            "info": data.info,
            "banner": data.banner,
            "closing_datetime": data.closing_datetime,
            "amount_tickets": data.amount_tickets,
            "min_bet": data.min_bet,
            "max_bet": data.max_bet,
            "choices": choices_json,
        },
    )
    competition = await get_competition(competition_id)
    assert competition, "Newly created competition couldn't be retrieved"
    return competition


async def update_competition(competition_id: str, data: UpdateCompetition) -> Optional[Competition]:
    setters: list[str] = []
    params: dict = {"id": competition_id}

    if data.amount_tickets is not None:
        setters.append("amount_tickets = :amount_tickets")
        params["amount_tickets"] = data.amount_tickets
    if data.closing_datetime is not None:
        setters.append("closing_datetime = :closing_datetime")
        params["closing_datetime"] = data.closing_datetime

    if not setters:
        return await get_competition(competition_id)

    update_result = await db.execute(
        f"UPDATE bets4sats.competitions SET {', '.join(setters)} WHERE id = :id AND state = 'INITIAL'",
        params,
    )
    if not update_result.rowcount:
        return None
    return await get_competition(competition_id)


async def cas_competition_state(competition_id: str, old_state: str, new_state: str) -> bool:
    update_result = await db.execute(
        """
        UPDATE bets4sats.competitions
        SET state = :new_state
        WHERE id = :id AND state = :old_state
        """,
        {"new_state": new_state, "id": competition_id, "old_state": old_state},
    )
    return bool(update_result.rowcount)


async def set_winning_choice(competition_id: str, winning_choice: int) -> None:
    await db.execute(
        """
        UPDATE bets4sats.competitions
        SET winning_choice = :winning_choice
        WHERE id = :id
        """,
        {"winning_choice": winning_choice, "id": competition_id},
    )


async def sum_choices_amounts(competition_id: str) -> List[ChoiceAmountSum]:
    rows = await db.fetchall(
        """
        SELECT choice, SUM(amount) AS amount_sum
        FROM bets4sats.tickets
        WHERE competition = :competition
        GROUP BY choice
        """,
        {"competition": competition_id},
    )
    return [ChoiceAmountSum(**row) for row in rows]


async def update_competition_winners(competition_id: str, choices: str, winning_choice: int):
    await db.execute(
        """
        UPDATE bets4sats.competitions
        SET choices = :choices, winning_choice = :winning_choice
        WHERE id = :id
        """,
        {"choices": choices, "winning_choice": winning_choice, "id": competition_id},
    )
    if winning_choice < 0:
        await db.execute(
            """
            UPDATE bets4sats.tickets
            SET state = 'CANCELLED_UNPAID'
            WHERE competition = :competition AND state = 'FUNDED'
            """,
            {"competition": competition_id},
        )
    else:
        # Mark winners / losers accordingly
        await db.execute(
            """
            UPDATE bets4sats.tickets
            SET state = 'WON_UNPAID'
            WHERE competition = :competition AND state = 'FUNDED' AND choice = :winning_choice
            """,
            {"competition": competition_id, "winning_choice": winning_choice},
        )
        await db.execute(
            """
            UPDATE bets4sats.tickets
            SET state = 'LOST'
            WHERE competition = :competition AND state = 'FUNDED' AND choice != :winning_choice
            """,
            {"competition": competition_id, "winning_choice": winning_choice},
        )


async def get_competition(competition_id: str) -> Optional[Competition]:
    return await db.fetchone(
        "SELECT * FROM bets4sats.competitions WHERE id = :id",
        {"id": competition_id},
        Competition,
    )


async def get_competitions(wallet_ids: Union[str, List[str]]) -> List[Competition]:
    if isinstance(wallet_ids, str):
        wallet_ids = [wallet_ids]
    q = ",".join([f"'{wallet_id}'" for wallet_id in wallet_ids])
    return [
        Competition(**row)
        for row in await db.fetchall(
            f"SELECT * FROM bets4sats.competitions WHERE wallet IN ({q})",
        )
    ]


async def get_all_competitions() -> List[Competition]:
    rows = await db.fetchall("SELECT * FROM bets4sats.competitions")
    return [Competition(**row) for row in rows]


async def delete_competition(competition_id: str) -> None:
    await db.execute("DELETE FROM bets4sats.competitions WHERE id = :id", {"id": competition_id})


# ---------------------------------------------------------------------------
# Helper queries for background tasks
# ---------------------------------------------------------------------------


async def get_wallet_competition_tickets(competition_id: str) -> List[Ticket]:
    rows = await db.fetchall(
        "SELECT * FROM bets4sats.tickets WHERE competition = :competition",
        {"competition": competition_id},
    )
    return [Ticket(**row) for row in rows]


async def get_state_competition_tickets(competition_id: str, states: List[str]) -> List[Ticket]:
    assert states, "get_state_competition_tickets called with no states"
    placeholders = ",".join([f"'{state}'" for state in states])
    return [
        Ticket(**row)
        for row in await db.fetchall(
            f"SELECT * FROM bets4sats.tickets WHERE competition = :competition AND state IN ({placeholders})",
            {"competition": competition_id},
        )
    ]


async def is_competition_payment_complete(competition_id: str) -> bool:
    row = await db.fetchone(
        """
        SELECT id FROM bets4sats.tickets
        WHERE competition = :competition
        AND state NOT IN ('CANCELLED_PAID', 'CANCELLED_PAYMENT_FAILED', 'WON_PAID', 'WON_PAYMENT_FAILED', 'LOST')
        LIMIT 1
        """,
        {"competition": competition_id},
    )
    return not bool(row)
