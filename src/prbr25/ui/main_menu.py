from InquirerPy import inquirer
from prbr25_startgg_queries.entrypoint import refresh_raw_events

from prbr25.ui.merge_players import display_merge_players
from prbr25.ui.not_yet_implemented import not_yet_implemented
from prbr25.ui.upload_tournament import upload_tournament
from prbr25.ui.utils import clear_screen
from prbr25.ui.validate_events import validate_events
from prbr25.ui.validate_players import validate_players


def main_menu():
    while True:
        clear_screen()
        choice = inquirer.select(
            message="Welcome to PRBR25🇧🇷:",
            choices=[
                "Refresh Events",
                "Add Event URL",
                "Validate Events",
                "Validate Players",
                "Merge Players",
                "Generate Monthly Report",
                "Exit",
            ],
        ).execute()

        match choice:
            case "Refresh Events":
                refresh_raw_events()
            case "Add Event URL":
                upload_tournament()
            case "Validate Events":
                validate_events()
            case "Validate Players":
                validate_players()
            case "Merge Players":
                display_merge_players()
            case "Generate Monthly Report":
                not_yet_implemented()
            case _:
                break
        input("Press Enter to go back...")
