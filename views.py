import json
import hmac
from datetime import date, datetime
from http import HTTPStatus

from fastapi import Depends, Request
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException
from starlette.responses import HTMLResponse

from lnbits.core.models import User
from lnbits.decorators import check_user_exists

from . import bets4sats_ext, bets4sats_renderer
from .crud import get_competition, get_ticket

templates = Jinja2Templates(directory="templates")


@bets4sats_ext.get("/", response_class=HTMLResponse)
async def index(request: Request, user: User = Depends(check_user_exists)):
    return bets4sats_renderer().TemplateResponse(
        "bets4sats/index.html", {"request": request, "user": user.dict()}
    )


@bets4sats_ext.get("/competitions/{competition_id}", response_class=HTMLResponse)
async def display(request: Request, competition_id):
    competition = await get_competition(competition_id)
    if not competition:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Competition does not exist."
        )

    return bets4sats_renderer().TemplateResponse(
        "bets4sats/display.html",
        {
            "request": request,
            "competition_id": competition_id,
            "competition_name": competition.name,
            "competition_info": competition.info,
            "competition_banner": json.dumps(competition.banner),
            "competition_state": competition.state,
            "competition_closing_datetime": competition.closing_datetime,
            "competition_choices": competition.choices,
            "competition_winning_choice": competition.winning_choice,
            "competition_amount_tickets": competition.amount_tickets,
            "competition_min_bet": competition.min_bet,
            "competition_max_bet": competition.max_bet,
        },
    )


@bets4sats_ext.get("/tickets/{ticket_id}", response_class=HTMLResponse)
async def ticket(request: Request, ticket_id):
    ticket = await get_ticket(ticket_id)
    if not ticket:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Ticket does not exist."
        )

    competition = await get_competition(ticket.competition)
    if not competition:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Competition does not exist."
        )

    return bets4sats_renderer().TemplateResponse(
        "bets4sats/ticket.html",
        {
            "request": request,
            "ticket_id": ticket_id,
            "ticket_amount": ticket.amount,
            "competition_name": competition.name,
            "competition_id": competition.id,
            "ticket_choice": json.loads(competition.choices)[ticket.choice]["title"],
            "ticket_state": ticket.state,
        },
    )


@bets4sats_ext.get("/register/{competition_id}/{register_id}", response_class=HTMLResponse)
async def register(request: Request, competition_id, register_id):
    competition = await get_competition(competition_id)
    if competition is None or not hmac.compare_digest(competition.register_id, register_id):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Competition does not exist."
        )
    return bets4sats_renderer().TemplateResponse(
        "bets4sats/register.html",
        {
            "request": request,
            "competition_id": competition_id,
            "competition_name": competition.name,
            "competition_choices": competition.choices,
            "register_id": competition.register_id,
        },
    )
