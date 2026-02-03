import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "news": "https://www.washingtontimes.com/rss/headlines/news",
}


@asset
def washingtontimes_raw() -> list[dict]:
    """Fetch all Washington Times RSS feeds"""
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
def washingtontimes_csv(washingtontimes_raw: list[dict]) -> str:
    """Save Washington Times feed data to CSV"""
    df = pd.DataFrame(washingtontimes_raw)
    output_dir = Path("dagster_data/washingtontimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Washington Times assets
washingtontimes_job = define_asset_job(
    "washingtontimes_job",
    selection=["washingtontimes_raw", "washingtontimes_csv"],
    description="Job to fetch and process Washington Times RSS feeds"
)

# Schedule to run every 6 hours
washingtontimes_schedule = ScheduleDefinition(
    job=washingtontimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="washingtontimes_schedule",
    description="Run Washington Times RSS feed collection every morning"
)
