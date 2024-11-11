import os

import pandas as pd


def get_overall_rank(
    data_source,
    user_ids,
    season,
    autosub,
    gameweeks,
    filename="overall.parquet.gzip",
    return_all=False,
):
    pts_folder = f"{data_source}_points"
    cols = [f"{gw:02d}" for gw in gameweeks]
    filepath = os.path.join("data", "24-25", pts_folder, autosub, filename)
    df = pd.read_parquet(filepath)
    df = df[cols]
    if data_source == "FPL":
        df = df.astype("Int64")
    df["total"] = df[cols].sum(axis=1)
    df["rank"] = df["total"].rank(ascending=False, method="min").astype(int)

    if return_all:
        return df.sort_values(by=["rank", "user_id"], ascending=[True, True])
    if user_ids:
        return df.loc[user_ids].sort_values(
            by=["rank", "user_id"], ascending=[True, True]
        )
    else:
        return df.sort_values(by=["rank", "user_id"], ascending=[True, True]).head(20)


def get_real_overall(
    user_ids=None, season="24-25", gameweeks=range(1, 39), autosub=True, return_all=True
):
    return get_overall_rank(
        data_source="FPL",
        user_ids=user_ids,
        season=season,
        autosub=autosub,
        gameweeks=gameweeks,
        return_all=return_all,
    )


def get_xg_overall(
    user_ids=None, season="24-25", gameweeks=range(1, 39), autosub=True, return_all=True
):
    return get_overall_rank(
        data_source="fpldata",
        user_ids=user_ids,
        season=season,
        autosub=autosub,
        gameweeks=gameweeks,
        return_all=return_all,
    )


def get_md_free_overall(
    user_ids=None, season="24-25", gameweeks=range(1, 39), autosub=True, return_all=True
):
    return get_overall_rank(
        data_source="fplreview_free",
        user_ids=user_ids,
        season=season,
        autosub=autosub,
        gameweeks=gameweeks,
        return_all=return_all,
    )


def get_md_paid_overall(
    user_ids=None, season="24-25", gameweeks=range(1, 39), autosub=True, return_all=True
):
    return get_overall_rank(
        data_source="fplreview_paid",
        user_ids=user_ids,
        season=season,
        autosub=autosub,
        gameweeks=gameweeks,
        return_all=return_all,
    )
