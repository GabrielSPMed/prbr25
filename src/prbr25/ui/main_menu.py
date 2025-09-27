from dotenv import load_dotenv
from InquirerPy import inquirer
from prbr25_startgg_queries.entrypoint import refresh_raw_events

from prbr25.ui.not_yet_implemented import not_yet_implemented
from prbr25.ui.utils import clear_screen

load_dotenv()


def main_menu():
    while True:
        clear_screen()
        choice = inquirer.select(
            message="Welcome to PRBR25:\n\n",
            choices=[
                "Refresh Events",
                "Add Event URL",
                "Validate Events",
                "Validate Players",
                "Exit",
            ],
        ).execute()

        if choice == "Refresh Events":
            refresh_raw_events()
        elif choice == "Add Event URL":
            not_yet_implemented()
        elif choice == "Validate Events":
            not_yet_implemented()
        elif choice == "Validate Players":
            not_yet_implemented()
        else:
            break


main_menu()
