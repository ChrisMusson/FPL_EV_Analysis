import asyncio
import json
import os

import aiohttp
import pandas as pd

from utils import fetch


async def get_player_real_points(filename, use_cached=False):
    filepath = os.path.join("projections", filename)
    if os.path.exists(filepath) and use_cached:
        with open(filename, "r") as f:
            data = json.load(f)
            return {
                int(gw): {
                    int(player_id): {
                        "pts": int(x["pts"]),
                        "is_valid": bool(x["is_valid"]),
                    }
                    for player_id, x in v.items()
                }
                for gw, v in data.items()
            }

    async with aiohttp.ClientSession() as session:
        url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        resp = await fetch(session, url)
        player_ids = [x["id"] for x in resp["elements"]]
        urls = [
            f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
            for player_id in player_ids
        ]
        tasks = [fetch(session, url) for url in urls]
        res = await asyncio.gather(*tasks)

    data = {
        gw: {i: {"pts": 0, "is_valid": False} for i in player_ids}
        for gw in range(1, 39)
    }
    for player in res:
        if player["history"] == []:
            continue
        player_id = player["history"][0]["element"]
        for match in player["history"]:
            data[match["round"]][player_id]["pts"] += match["total_points"]
            if (
                match["minutes"] > 0
                or match["yellow_cards"] > 0
                or match["red_cards"] > 0
            ):
                data[match["round"]][player_id]["is_valid"] = True

    with open(filepath, "w") as f:
        f.write(json.dumps(data))
    return data


def get_player_fpl_data_pts():
    xp_data = pd.read_csv(os.path.join("projections", "fpl-data-stats.csv"))
    xp_data = xp_data.pivot_table(values="xP", columns="gameweek", index="id").fillna(0)
    xp_data_dct = xp_data.to_dict()
    return {
        gw: {
            player_id: {"pts": xp_data_dct[gw][player_id], "is_valid": True}
            for player_id in xp_data_dct[gw]
        }
        for gw in xp_data_dct
    }


def get_player_fplreview_free_pts():
    with open(os.path.join("projections", "fplreview_free.json"), "r") as f:
        data = json.load(f)
        review_data = {
            int(gw): {
                int(player_id): {
                    "pts": float(player_data["points_md"]),
                    "is_valid": True,
                }
                for player_id, player_data in gw_data.items()
            }
            for gw, gw_data in data.items()
        }
    return review_data


def get_player_fplreview_paid_pts():
    xpts_data = pd.read_csv(os.path.join("projections", "fplreview_md_paid.csv"))
    xpts_data = xpts_data.set_index("ID")
    cols = [x for x in xpts_data.columns if "_Pts" in x]
    xpts_data = xpts_data[cols]
    xpts_data.columns = range(1, len(cols) + 1)
    xpts_data = xpts_data.to_dict()

    return {
        gw: {pid: {"pts": pts, "is_valid": True} for pid, pts in gw_data.items()}
        for gw, gw_data in xpts_data.items()
    }


async def get_player_data(data_source, use_cached):
    if data_source == "FPL":
        return await get_player_real_points("real_points.json", use_cached=use_cached)
    elif data_source == "fpldata":
        return get_player_fpl_data_pts()
    elif data_source == "fplreview_free":
        return get_player_fplreview_free_pts()
    elif data_source == "fplreview_paid":
        return get_player_fplreview_paid_pts()
