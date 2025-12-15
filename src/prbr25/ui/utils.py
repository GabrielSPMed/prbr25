import os
import shutil

from pandas import DataFrame
from prbr25_logger.logger import setup_logger
from tabulate import tabulate

logger = setup_logger(__name__)


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def print_centered(text):
    width = shutil.get_terminal_size().columns
    print(text.center(width))


def display_event_being_validated(event_info: DataFrame):
    clear_screen()
    logger.info(
        f"{event_info.tournament_name.iloc[0]}: {event_info.event_name.iloc[0]}"
    )
    logger.info(
        f"Localizacao: {event_info.city.iloc[0]}, {event_info.address_state.iloc[0]}"
    )
    logger.info(event_info.url.iloc[0])


def display_dataframe(df: DataFrame, col_list: list = []):
    if col_list:
        df = df[col_list]
    print(
        tabulate(
            df.values.tolist(),
            headers=df.columns.tolist(),
            tablefmt="grid",
        )
    )
