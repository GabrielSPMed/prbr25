from pandas import DataFrame


def get_empty_players_dataframe() -> DataFrame:
    cols = ["id", "tag", "value", "url", "state", "anonymous"]
    return DataFrame(columns=cols)
