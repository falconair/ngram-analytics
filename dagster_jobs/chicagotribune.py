import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.chicagotribune.com/feed/",
    "business": "https://www.chicagotribune.com/business/feed/",
    "education": "https://www.chicagotribune.com/news/education/feed/",
    "entertainment": "https://www.chicagotribune.com/things-to-do/entertainment/feed/",
    "food": "https://www.chicagotribune.com/things-to-do/restaurants-food-drink/feed/",
    "opinion": "https://www.chicagotribune.com/opinion/feed/",
    "sports": "https://www.chicagotribune.com/sports/feed/",
}


@asset
def chicagotribune_raw() -> list[dict]:
    """Fetch all Chicago Tribune RSS feeds"""
    all_entries = []
    for feed_name, url in FEEDS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            all_entries.append({
                "feed": feed_name,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", ""),
                "author": entry.get("author", ""),
                "tags": ", ".join(tag.get("term", "") for tag in entry.get("tags", [])),
            })
    return all_entries


@asset
def chicagotribune_csv(chicagotribune_raw: list[dict]) -> str:
    """Save Chicago Tribune feed data to CSV"""
    df = pd.DataFrame(chicagotribune_raw)
    output_dir = Path("dagster_data/chicagotribune")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Chicago Tribune assets
chicagotribune_job = define_asset_job(
    "chicagotribune_job",
    selection=["chicagotribune_raw", "chicagotribune_csv"],
    description="Job to fetch and process Chicago Tribune RSS feeds"
)

# Schedule to run every 4 hours
chicagotribune_schedule = ScheduleDefinition(
    job=chicagotribune_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="chicagotribune_schedule",
    description="Run Chicago Tribune RSS feed collection every morning"
)
