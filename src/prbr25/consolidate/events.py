from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres

from prbr25.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)


def create_df_from_df_ids(ids: list) -> DataFrame:
    df = DataFrame({"id": ids, "value": [0] * len(ids), "n_dqs": [0] * len(ids)})
    return df


def consolidate_events(ids: list):
    sql = Postgres(
        POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PORT
    )
    df = create_df_from_df_ids(ids)
    sql.insert_values_to_table(df, "consolidated_events")
