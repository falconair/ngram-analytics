import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.startribune.com/local/index.rss2",
    "business": "https://www.startribune.com/business/index.rss2",
    "sports": "https://www.startribune.com/sports/index.rss2",
}


@asset
def startribune_raw() -> list[dict]:
    """Fetch all Star Tribune (Minneapolis) RSS feeds"""
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
def startribune_csv(startribune_raw: list[dict]) -> str:
    """Save Star Tribune (Minneapolis) feed data to CSV"""
    df = pd.DataFrame(startribune_raw)
    output_dir = Path("dagster_data/startribune")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Star Tribune assets
startribune_job = define_asset_job(
    "startribune_job",
    selection=["startribune_raw", "startribune_csv"],
    description="Job to fetch and process Star Tribune RSS feeds"
)

# Schedule to run every 6 hours
startribune_schedule = ScheduleDefinition(
    job=startribune_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="startribune_schedule",
    description="Run Star Tribune RSS feed collection every morning"
)
