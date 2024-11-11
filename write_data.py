import os

import pandas as pd

from read_projections import get_player_data
from utils import calculate_pts


async def write_points_to_file(data_source, season, autosub, gameweeks=range(1, 39)):
    player_data = await get_player_data(data_source=data_source, use_cached=False)
    for gw in gameweeks:
        folder = os.path.join("data", season, "picks", autosub, f"{gw:02d}")
        chunks = [int(x.split(".")[0]) for x in os.listdir(folder)]

        dfs = []
        for chunk in chunks:
            print(gw, chunk)
            df = calculate_pts(gw, chunk, player_data, autosub, season)
            df = df[["user_id", "total_pts"]]
            dfs.append(df)

        score = pd.concat(dfs)
        score = score.groupby("user_id").aggregate("sum")
        output_filepath = os.path.join(
            "data", season, f"{data_source}_points", autosub, f"{gw:02d}.parquet.gzip"
        )
        score.to_parquet(output_filepath, compression="gzip")


def get_points(data_source, season, autosub, gw):
    filename = f"{gw:02d}.parquet.gzip"
    filepath = os.path.join("data", season, f"{data_source}_points", autosub, filename)
    df = pd.read_parquet(filepath)
    df = df.rename(columns={"total_pts": f"{gw:02d}"})
    return df


def write_overall_table(data_source, season, autosub):
    folder = os.path.join("data", season, f"{data_source}_points", autosub)
    gameweeks = [int(x.split(".")[0]) for x in os.listdir(folder) if x[0].isnumeric()]
    gameweeks = sorted(gameweeks)
    df = get_points(data_source, gameweeks[0])
    for gw in gameweeks[1:]:
        gw_df = get_points(data_source, gw)
        df = pd.concat([df, gw_df], axis=1)
    filepath = os.path.join(
        "data", season, f"{data_source}_points", autosub, "overall.parquet.gzip"
    )
    df.to_parquet(filepath)
