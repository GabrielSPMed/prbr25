from pandas import DataFrame, Series
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
) -> DataFrame:
    if entrant.player_id:
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

    logger.info("is anonymous")
    display_similar_players(entrant, existing_player_df, event_info)
    tournament_player_df = new_player_screen(entrant, tournament_player_df)
    validated_entrant_ids.append(entrant.id)
    return tournament_player_df
