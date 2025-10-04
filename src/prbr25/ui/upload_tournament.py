from InquirerPy import inquirer
from prbr25_startgg_queries.entrypoint import (
    retrieve_events_and_phases_from_tournament_url,
)


def upload_tournament():
    url = inquirer.text(
        message="Enter the tournament URL (or 'back' to return): "
    ).execute()
    if url.lower() != "back":
        retrieve_events_and_phases_from_tournament_url(url)
