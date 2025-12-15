from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres
from prbr25_startgg_queries.entrypoint import fetch_matches_df

from prbr25.consolidate.entrant import query_players_participated_in_event

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
    return matches_raw_df, player_df
