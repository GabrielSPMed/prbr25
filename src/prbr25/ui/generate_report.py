from datetime import datetime

from dateutil.relativedelta import relativedelta
from InquirerPy import inquirer
from prbr25_db_ops.reporting.lock.updating import update_monthly_lock_file
from prbr25_db_ops.reporting.player.performance_evaluation import (
    get_player_monthly_performance,
)
from prbr25_db_ops.reporting.tournament.consolidated import get_validated_tournaments
from prbr25_db_ops.reporting.tournament.rejected import get_rejected_tournaments
from prbr25_rds_client.postgres import Postgres

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
    report_folder,
)
from prbr25.ui.utils import clear_screen


def generate_report(year_start: int, month_start: int):
    date_start = datetime(year_start, month_start, 1)
    choices = [
        f"{(date_start + relativedelta(months=i)).month}/{(date_start + relativedelta(months=i)).year}"
        for i in range(12)
    ]
    choices.append("Exit")
    while True:
        clear_screen()
        choice = inquirer.select(
            message="Select the month you want to generate a report for:",
            choices=choices,
        ).execute()
        if choice == "Exit":
            return
        else:
            month, year = choice.split("/")
            month = int(month)
            year = int(f"20{year}")
            sql = Postgres(
                POSTGRES_USERNAME,
                POSTGRES_PASSWORD,
                POSTGRES_HOST,
                POSTGRES_DB,
                POSTGRES_PORT,
            )
            path = f"{report_folder}/{month}"
            _, id_series = get_validated_tournaments(
                month, year, sql, save=True, path=path
            )
            get_rejected_tournaments(month, year, sql, save=True, path=path)
            get_player_monthly_performance(
                sql, id_series.to_list(), save=True, path=path
            )
            update_monthly_lock_file(".", month, report=True)
            return
