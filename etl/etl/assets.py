import requests
import pandas as pd
from dagster import asset, get_dagster_logger


@asset  # add the asset decorator to tell Dagster this is an asset
def topstory_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]
    return top_new_story_ids


@asset
def topstories(topstory_ids):
    logger = get_dagster_logger()

    results = []
    for item_id in topstory_ids:
        url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        item = requests.get(url).json()
        results.append(item)

        if len(results) % 20 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    return df
