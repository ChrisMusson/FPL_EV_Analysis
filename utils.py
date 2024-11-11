import asyncio
import os
import random

import aiohttp
import pandas as pd
import requests


async def fetch(session, url, max_retries=1000, cooldown=0.5):
    headers = {
        "User-Agent": f"Dalvik {random.choice(range(1, 20))}.{random.choice(range(1,4))}"
    }
    retries_count = 0
    while True:
        try:
            async with session.get(url, headers=headers) as response:
                return await response.json()
        except aiohttp.client_exceptions.ContentTypeError:
            retries_count += 1
            if retries_count > max_retries:
                print("Max retries exceeded for", url)
                raise Exception(f"Could not fetch {url} after {max_retries} retries")
            if cooldown:
                await asyncio.sleep(cooldown)


def get_league_users(league_id):
    users = []
    with requests.Session() as s:
        i = 1
        while True:
            url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/?page_standings={i}"
            r = s.get(url).json()
            users += [x["entry"] for x in r["standings"]["results"]]
            if not r["standings"]["has_next"]:
                break
            i += 1
    return users


def calculate_pts(gw, chunk_number, player_data, autosub="pre_autosub", season="24-25"):
    # print(player_data)
    player_pts = {int(x): v["pts"] for x, v in player_data[gw].items()}
    player_validity = {int(x): int(v["is_valid"]) for x, v in player_data[gw].items()}

    filename = f"{chunk_number:02d}.parquet.gzip"
    filepath = os.path.join("data", season, "picks", autosub, f"{gw:02d}", filename)
    if not os.path.exists(filepath):
        return
    df = pd.read_parquet(filepath)

    # starting 11 points
    for i in range(1, 12):
        df[f"pts{i:02d}"] = df[f"p{i:02d}"].map(player_pts)
    df["starting_11_pts"] = df[[f"pts{i:02d}" for i in range(1, 12)]].sum(axis=1)

    # bench pts
    for i in range(12, 16):
        df[f"pts{i:02d}"] = df[f"p{i:02d}"].map(player_pts)
    df["bench_pts"] = df[[f"pts{i:02d}" for i in range(12, 16)]].sum(axis=1) * (
        df["chip"] == "bboost"
    )

    # captain pts
    df["cap_pts"] = df["captain"].map(player_pts)
    df["valid_cap"] = df["captain"].map(player_validity)
    df["vc_pts"] = df["vice_captain"].map(player_pts)
    multiplier = 1 + (df["chip"] == "3xc")

    df["captain_pts"] = (
        (df["cap_pts"] * df["valid_cap"]) + ((1 - df["valid_cap"]) * df["vc_pts"])
    ) * multiplier

    df["total_pts"] = (
        df["starting_11_pts"]
        + df["bench_pts"]
        + df["captain_pts"]
        - df["transfer_cost"]
    )

    return df
