from pandas import DataFrame
from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres
from prbr25_startgg_queries.entrypoint import fetch_matches_df

from prbr25.consolidate.entrant import query_players_participated_in_event
from prbr25.consolidate.events import update_tournament_values
from prbr25.consolidate.standings import consolidate_standings
from prbr25.ui.tournament_tier import display_tournament_tier

logger = setup_logger(__name__)


def consolidate_matches(sql: Postgres, event_id: int):
    logger.debug("Event players completed, fetching matches")
    matches_raw_df = fetch_matches_df(event_id)
    logger.debug("Event matches fetched, querying entrants for event")
    player_df = query_players_participated_in_event(sql, event_id)
    entrant_player_id_map = dict(zip(player_df["id"], player_df["player_id"]))
    matches_raw_df = matches_raw_df.replace(entrant_player_id_map)
    logger.debug("Inserting matches to database")
    sql.insert_values_to_table(matches_raw_df, "matches")
    return matches_raw_df, player_df, entrant_player_id_map


def consolidate_matches_and_standings(
    sql: Postgres, event_id: int, event_info: DataFrame
):
    matches_df, updated_player_df, entrant_player_id_map = consolidate_matches(
        sql, event_id
    )
    tournament_score, dq_players_list = update_tournament_values(
        sql, event_id, matches_df, updated_player_df
    )
    weight_dict = consolidate_standings(
        sql,
        event_id,
        entrant_player_id_map,
        tournament_score,
        dq_players_list,
    )
    display_tournament_tier(
        event_info.tournament_name.iloc[0],
        event_info.event_name.iloc[0],
        weight_dict,
        tournament_score,
        len(dq_players_list),
        event_info.num_entrants.iloc[0],
    )
