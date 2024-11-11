import asyncio
import os

from calculate_rank import (
    get_md_free_overall,
    get_md_paid_overall,
    get_real_overall,
    get_xg_overall,
)
from utils import get_league_users
from write_data import write_overall_table, write_points_to_file

SEASON = "24-25"
AUTOSUB = "post_autosub"


async def main():
    func_map = {
        "FPL": get_real_overall,
        "fpldata": get_xg_overall,
        "fplreview_free": get_md_free_overall,
        "fplreview_paid": get_md_paid_overall,
    }

    # 'fpldata', 'fplreview_free', `fplreview_paid` or 'FPL'
    data_source = "fplreview_paid"

    # will calculate pts/xpts for given gameweeks. Only needs to be run if new picks data has been added, and even then, only
    # needs to be run for those particular gameweeks
    await write_points_to_file(
        data_source, season=SEASON, autosub=AUTOSUB, gameweeks=[11]
    )

    # adds new data onto the overall table. Not needed unless new data has been added by the above line
    write_overall_table(data_source, season=SEASON, autosub=AUTOSUB)

    gameweeks = range(1, 12)
    user_ids = get_league_users(7639)  # bullet wisdom league IDs

    df = func_map[
        data_source
    ](
        user_ids=user_ids,
        season=SEASON,
        gameweeks=gameweeks,
        autosub=AUTOSUB,
        return_all=False,  # warning: turning this to True will write all 10m+ rows to csv
    )
    df.to_csv(
        os.path.join("results", f"{data_source}_{AUTOSUB}.csv", float_format="%.3f")
    )


if __name__ == "__main__":
    asyncio.run(main())
