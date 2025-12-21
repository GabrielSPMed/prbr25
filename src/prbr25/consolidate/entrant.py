from pandas import DataFrame, Series, isna
from prbr25_db_ops.player.search import get_tag_from_player_id
from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres
from prbr25_startgg_queries.entrypoint import edit_filtered_column_from_id

from prbr25.ui.create_players import display_similar_players, new_player_screen

logger = setup_logger(__name__)


def validate_player(
    sql: Postgres,
    entrant: Series,
    validated_entrant_ids: list,
    tournament_player_df: DataFrame,
    event_info: DataFrame,
    existing_player_df: DataFrame,
    players_to_merge: list,
) -> DataFrame:
    if not isna(entrant.player_id):
        existing_tag = get_tag_from_player_id(sql, entrant.player_id)
        if existing_tag:
            logger.info("Is already in the database")
            if entrant.tag != existing_tag:
                logger.info(f"Updating tag from '{existing_tag}' to '{entrant.tag}'")
                edit_filtered_column_from_id(
                    [entrant.player_id], "players", "tag", entrant.tag
                )
            validated_entrant_ids.append(entrant.id)
            return tournament_player_df

    display_similar_players(entrant, existing_player_df, event_info)
    tournament_player_df = new_player_screen(
        entrant, tournament_player_df, players_to_merge
    )
    validated_entrant_ids.append(entrant.id)
    return tournament_player_df


def query_players_participated_in_event(sql: Postgres, event_id: int):
    query = f"""SELECT e.id, e.player_id, p.value
                FROM entrants e
                JOIN players p ON e.player_id = p.id
                WHERE e.event_id = {event_id}"""
    return sql.query_db(query, "entrants")


def get_list_of_dq_players(matches_df: DataFrame):
    double_dq_list = get_double_dq_losers(matches_df)
    return filter_never_won_without_dq(matches_df, double_dq_list)


def get_double_dq_losers(df: DataFrame) -> list:
    dq_df = df[df["dq"]]
    counts = dq_df["losing_player_id"].value_counts()
    double_dq_ids = counts[counts == 2].index.tolist()
    return double_dq_ids


def filter_never_won_without_dq(df: DataFrame, player_ids: list) -> list:
    winners = df[~df["dq"]]["winning_player_id"].unique()
    return [pid for pid in player_ids if pid not in winners]
