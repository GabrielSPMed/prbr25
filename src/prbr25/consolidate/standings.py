from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres
from prbr25_startgg_queries.entrypoint import fetch_raw_standings_df
from prbr25_weights.weights import get_weights_for_score


def consolidate_standings(
    sql: Postgres,
    event_id: int,
    entrant_player_id_map: dict,
    tournament_score: int,
    dq_players_list: list,
) -> dict:
    standings_df = fetch_raw_standings_df(event_id)
    standings_df = standings_df.replace(entrant_player_id_map)
    standings_df.loc[standings_df["player_id"].isin(dq_players_list), "dq"] = True
    weight_dict = get_weights_for_score(tournament_score)
    standings_df = set_weights_to_standings(standings_df, weight_dict)
    sql.insert_values_to_table(standings_df, "standings")
    return weight_dict


def set_weights_to_standings(standings_df: DataFrame, weight_dict: dict) -> DataFrame:
    possible_positions = [1, 2, 3, 4, 5, 7, 9, 13, 17, 25]
    for position in possible_positions:
        standings_df.loc[standings_df["standing"] == position, "perf_score"] = (
            weight_dict[f"{position}"]
        )
    return standings_df
