from pandas import DataFrame
from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)
from prbr25.consolidate.entrant import get_list_of_dq_players

logger = setup_logger(__name__)


def create_df_from_df_ids(ids: list) -> DataFrame:
    df = DataFrame({"id": ids, "value": [0] * len(ids), "n_dqs": [0] * len(ids)})
    return df


def consolidate_events(ids: list):
    sql = Postgres(
        POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PORT
    )
    df = create_df_from_df_ids(ids)
    sql.insert_values_to_table(df, "consolidated_events")


def update_tournament_values(
    sql: Postgres, event_id: int, matches_df: DataFrame, players_df: DataFrame
):
    dq_players_list = get_list_of_dq_players(matches_df)
    n_dqs = len(dq_players_list)
    player_bonus_score = players_df[~players_df["player_id"].isin(dq_players_list)][
        "value"
    ].sum()
    num_entrants = query_number_of_entrants(sql, event_id)
    tournament_score = num_entrants - n_dqs + player_bonus_score
    logger.debug("Updating tournament values")
    update_consolidated_event_values(sql, tournament_score, n_dqs, event_id)
    return tournament_score, dq_players_list


def query_number_of_entrants(sql: Postgres, event_id: int):
    query = f"""SELECT num_entrants FROM raw_events WHERE id = {event_id}"""
    df = sql.query_db(query, "raw_events")
    return df["num_entrants"].iloc[0]


def update_consolidated_event_values(
    sql: Postgres, tournament_score: int, n_dqs: int, event_id
):
    query = f"""UPDATE consolidated_events
                SET value = {tournament_score}, n_dqs = {n_dqs}
                WHERE id = {event_id}"""
    sql.execute_update(query)


def sort_event_ids_by_start_date(sql: Postgres, event_ids: list) -> list:
    if not event_ids:
        return []

    ids_str = ",".join(str(id) for id in event_ids)
    query = f"""
        SELECT id 
        FROM raw_events 
        WHERE id IN ({ids_str})
        ORDER BY start_at ASC
    """
    df = sql.query_db(query, "raw_events")
    return df["id"].tolist()
