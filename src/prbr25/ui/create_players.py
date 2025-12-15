from InquirerPy import inquirer
from pandas import DataFrame, Series, concat
from prbr25_db_ops.player.tag_search import fuzzy_tag_search
from prbr25_logger.logger import setup_logger

from prbr25.exceptions.exit_player_validation import ExitPlayerValidation
from prbr25.ui.not_yet_implemented import not_yet_implemented
from prbr25.ui.utils import display_dataframe, display_event_being_validated

logger = setup_logger(__name__)


def display_similar_players(entrant: Series, players: DataFrame, event_info: DataFrame):
    display_event_being_validated(event_info)
    if len(players) > 0:
        players_search_df = fuzzy_tag_search(players.copy(), entrant.tag)
        players_search_df = players_search_df.sort_values(
            by="tag_match_score", ascending=False
        ).head(5)
        col_list = ["tag", "id", "url", "state", "anonymous", "tag_match_score"]
        logger.info("Best national matches")
        display_dataframe(players_search_df, col_list)
        logger.info(f"Best matches in {event_info.address_state.iloc[0]}")
        display_dataframe(
            players_search_df[
                players_search_df.state == event_info.address_state.iloc[0]
            ],
            col_list,
        )

    else:
        logger.info("NO AVAILABLE TAGS TO COMPARE")


def new_player_screen(entrant: Series, existing_player_df: DataFrame) -> DataFrame:
    if entrant.player_id:
        message = f"Choose an option for {entrant.tag}:"
    else:
        message = f"Choose an option for ANONYMOUS {entrant.tag}:"
    choice = inquirer.select(
        message=message,
        choices=[
            "Create new player",
            "Merge entrant into existing player",
            "Merge existing player into entrant",
            "Exit",
        ],
    ).execute()

    match choice:
        case "Create new player":
            if entrant.player_id:
                return create_player_known_id(existing_player_df, entrant)
            else:
                return create_player_anonymous(existing_player_df, entrant)
        case "Merge entrant into existing player":
            not_yet_implemented()
            raise ExitPlayerValidation()
        case "Merge existing player into entrant":
            not_yet_implemented()
            raise ExitPlayerValidation()
        case _:
            raise ExitPlayerValidation()


def create_player_known_id(existing_player_df: DataFrame, entrant: Series) -> DataFrame:
    player_value = inquirer.text(
        message=f"{entrant.tag} value:",
        validate=lambda x: x.isdigit(),
        invalid_message="Must be a number",
    ).execute()
    player_state = inquirer.text(message=f"Where is {entrant.tag} from?").execute()
    new_row = DataFrame(
        [
            {
                "id": entrant.player_id,
                "tag": entrant.tag,
                "value": int(player_value),
                "url": entrant.url,
                "state": player_state,
                "anonymous": False,
            }
        ]
    )
    return concat([existing_player_df, new_row], ignore_index=True)


def create_player_anonymous(
    existing_player_df: DataFrame, entrant: Series
) -> DataFrame:
    player_value = inquirer.text(
        message=f"{entrant.tag} value:",
        validate=lambda x: x.isdigit(),
        invalid_message="Must be a number",
    ).execute()
    player_state = inquirer.text(message=f"Where is {entrant.tag} from?").execute()
    new_row = DataFrame(
        [
            {
                "id": entrant.id,
                "tag": entrant.tag,
                "value": int(player_value),
                "url": entrant.url,
                "state": player_state,
                "anonymous": True,
            }
        ]
    )
    return concat([existing_player_df, new_row], ignore_index=True)
