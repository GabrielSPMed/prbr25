from InquirerPy import inquirer
from prbr25_rds_client.postgres import Postgres

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)
from prbr25.consolidate.merge import merge_player_pair


def ask_for_player_id(merge_base: bool):
    if merge_base:
        message = "Enter the player id for the player that will REMAIN after merging: "
    else:
        message = (
            "Enter the player id for the player that will be DELETED after merging: "
        )
    player_id = inquirer.text(message=message).execute()
    return int(player_id)


def display_merge_players():
    base_id = ask_for_player_id(True)
    delete_id = ask_for_player_id(False)
    merge_pair = {"base": base_id, "delete": delete_id}
    sql = Postgres(
        POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PORT
    )
    merge_player_pair(sql, merge_pair)
