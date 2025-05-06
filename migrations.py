
# bets4sats/migrations.py
#
# NOTE: every table that must exist in a _fresh_ install goes in m001_initial.
# Add m00X_* functions only for _future_ changes to that baseline.

async def m001_initial(db):
    # == competitions =========================================
    await db.execute(
        """
        CREATE TABLE bets4sats.competitions (
            id              TEXT PRIMARY KEY,
            wallet          TEXT NOT NULL,
            register_id     TEXT NOT NULL,
            name            TEXT NOT NULL,
            info            TEXT NOT NULL,
            banner          TEXT NOT NULL,
            closing_datetime TEXT NOT NULL,
            amount_tickets  INTEGER NOT NULL,
            min_bet         INTEGER NOT NULL,
            max_bet         INTEGER NOT NULL,
            sold            INTEGER NOT NULL,
            choices         TEXT NOT NULL,
            winning_choice  INTEGER NOT NULL DEFAULT -1,   -- -1 = not decided yet
            state           TEXT NOT NULL,
            time            TIMESTAMP NOT NULL DEFAULT """
        + db.timestamp_now
        + """
        );
        """
    )

    # == tickets ==============================================
    await db.execute(
        """
        CREATE TABLE bets4sats.tickets (
            id                  TEXT PRIMARY KEY,
            wallet              TEXT NOT NULL,
            competition         TEXT NOT NULL,
            amount              INTEGER NOT NULL,
            reward_target       TEXT NOT NULL,
            choice              INTEGER NOT NULL,
            state               TEXT NOT NULL,
            reward_msat         INTEGER NOT NULL,
            reward_failure      TEXT NOT NULL,
            reward_payment_hash TEXT NOT NULL,
            time                TIMESTAMP NOT NULL DEFAULT """
        + db.timestamp_now
        + """
        );
        """
    )
