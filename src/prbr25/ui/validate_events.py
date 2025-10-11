from typing import Dict

from InquirerPy import inquirer
from pandas import DataFrame, Series
from prbr25_rds_client.postgres import Postgres

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)
from prbr25.ui.utils import clear_screen


def query_events_to_validate() -> DataFrame:
    sql = Postgres(
        POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PORT
    )
    query = """
        SELECT *
        FROM raw_events
        WHERE validated = False
        ORDER BY start_at ASC
    """
    return sql.query_db(query, "raw_events")


def iterate_events(state: Dict, df: DataFrame):
    for _, row in df.iterrows():
        choice = display_event(row)
        if choice == "Exit":
            return
        store_user_choices(state, choice, row)


def store_user_choices(state: Dict, choice: str, row: Series):
    if choice == "Consolidate":
        state["consolidated_ids"].append(row["id"])
    else:
        state["rejected_ids"].append(row["id"])
    state["validated_ids"].append(row.id)


def display_event(row: Series) -> str:
    clear_screen()
    choice = inquirer.select(
        message="".join(
            f"{col}: {val}\n" for col, val in row.items() if "id" not in str(col)
        ),
        choices=[
            "Consolidate",
            "Reject",
            "Exit",
        ],
    ).execute()
    return choice


def validate_events():
    state = {"validated_ids": [], "consolidated_ids": [], "rejected_ids": []}
    df = query_events_to_validate()
    iterate_events(state, df)
    print(
        f"Accepted tournaments:\n {(df[df.id.isin(state['consolidated_ids'])].loc[:, ['tournament_name', 'event_name']])} "
    )

    print(
        f"Rejected tournaments:\n {(df[df.id.isin(state['rejected_ids'])].loc[:, ['tournament_name', 'event_name']])} "
    )


validate_events()
