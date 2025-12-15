from pandas import DataFrame
from prbr25_db_ops.event.event_data import query_event_info_from_id
from prbr25_db_ops.player.search import fetch_all_players
from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres
from prbr25_startgg_queries.entrypoint import edit_filtered_column_from_id

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)
from prbr25.consolidate.entrant import validate_player
from prbr25.consolidate.pandas_utils import get_empty_players_dataframe
from prbr25.exceptions.exit_player_validation import ExitPlayerValidation
from prbr25.ui.utils import display_event_being_validated

logger = setup_logger(__name__)


def query_entrants_to_validate(sql: Postgres) -> DataFrame:
    query = """
        SELECT *
        FROM entrants
        WHERE validated = False
    """
    return sql.query_db(query, "entrants")


def iterate_consolidated_events(
    sql: Postgres, unvalidated_entrants_df: DataFrame, validated_entrant_ids: list
):
    for event_id in unvalidated_entrants_df.event_id.unique():
        event_info = query_event_info_from_id(sql, event_id)
        unvalidated_event_entrants_df = unvalidated_entrants_df.loc[
            unvalidated_entrants_df.event_id == event_id
        ]
        player_df = fetch_all_players(sql)
        display_event_being_validated(event_info)
        try:
            new_players_df = iterate_players(
                unvalidated_event_entrants_df,
                sql,
                validated_entrant_ids,
                event_info,
                player_df,
            )
            edit_filtered_column_from_id(
                validated_entrant_ids, "entrants", "validated", True
            )
            if len(new_players_df) > 0:
                sql.insert_values_to_table(new_players_df, "players")
            else:
                logger.info(
                    f"No new players for {event_info.tournament_name}: {event_info.event_name}"
                )
        except ExitPlayerValidation:
            break


def validate_players():
    sql = Postgres(
        POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PORT
    )
    validated_entrant_ids = []
    unvalidated_entrants_df = query_entrants_to_validate(sql)
    iterate_consolidated_events(sql, unvalidated_entrants_df, validated_entrant_ids)


def iterate_players(
    unvalidated_entrants_df: DataFrame,
    sql: Postgres,
    validated_entrant_ids: list,
    event_info: DataFrame,
    existing_player_df: DataFrame,
) -> DataFrame:
    total_entrants_to_validate = len(unvalidated_entrants_df)
    counter = 1
    tournament_player_df = get_empty_players_dataframe()

    for _, entrant in unvalidated_entrants_df.iterrows():
        logger.info("")
        logger.info(
            f"Consolidating {counter}/{total_entrants_to_validate}: {entrant.tag}"
        )
        try:
            tournament_player_df = validate_player(
                sql,
                entrant,
                validated_entrant_ids,
                tournament_player_df,
                event_info,
                existing_player_df,
            )
        except ExitPlayerValidation as e:
            if validated_entrant_ids:
                edit_filtered_column_from_id(
                    validated_entrant_ids, "entrants", "validated", True
                )
            if len(tournament_player_df) > 0:
                sql.insert_values_to_table(tournament_player_df, "players")
            else:
                logger.info(
                    f"No new players for {event_info.tournament_name.iloc[0]}: {event_info.event_name.iloc[0]}"
                )
            raise e
        counter += 1
    return tournament_player_df
