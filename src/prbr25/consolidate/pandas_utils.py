from pandas import DataFrame


def get_empty_players_dataframe() -> DataFrame:
    cols = ["id", "tag", "value", "url", "state", "anonymous"]
    df = DataFrame(columns=cols)
    df = df.astype({"id": "Int64", "value": "Int64", "anonymous": "bool"})
    return df
