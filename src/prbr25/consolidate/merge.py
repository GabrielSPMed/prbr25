from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres

logger = setup_logger(__name__)


def merge_players(sql: Postgres, merge_list: list):
    for merge_pair in merge_list:
        merge_player_pair(sql, merge_pair)


def merge_player_pair(sql: Postgres, merge_pair: dict):
    base_id = merge_pair["base"]
    delete_id = merge_pair["delete"]
    logger.debug(f"Merging player id {delete_id} into player id {base_id}")
    update_entrant_ids(sql, base_id, delete_id)
    logger.debug("Successfully updated entrants table, starting matches table")
    update_match_player_ids(sql, base_id, delete_id)
    logger.debug("Successfully updated matches table, starting standings table")
    delete_player(sql, delete_id)
    logger.debug(f"Successfully merged {delete_id} into {base_id}")


def update_entrant_ids(sql: Postgres, base_id: int, delete_id: int):
    query = generate_update_query_id(base_id, delete_id, "entrants", "player_id")
    sql.execute_update(query)


def update_match_player_ids(sql: Postgres, base_id: int, delete_id: int):
    cols = ["player_1_id", "player_2_id", "winning_player_id", "losing_player_id"]
    for col in cols:
        query = generate_update_query_id(base_id, delete_id, "matches", col)
        sql.execute_update(query)


def generate_update_query_id(
    base_id: int, delete_id: int, table_name: str, variable_name: str
):
    return f"""
        UPDATE {table_name}
        SET {variable_name} = {base_id}
        WHERE {variable_name} = {delete_id}
    """


def update_standings_table(sql: Postgres, base_id: int, delete_id: int):
    query = generate_update_query_id(base_id, delete_id, "standings", "player_id")
    sql.execute_update(query)


def delete_player(sql: Postgres, player_id: int):
    query = f"""
        DELETE FROM players
        WHERE id = {player_id}
    """
    sql.execute_update(query)
