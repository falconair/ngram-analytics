import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.seattletimes.com/feed/",
    "business": "https://www.seattletimes.com/business/feed/",
    "local_news": "https://www.seattletimes.com/seattle-news/feed/",
    "sports": "https://www.seattletimes.com/sports/feed/",
}


@asset
def seattletimes_raw() -> list[dict]:
    """Fetch all Seattle Times RSS feeds"""
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
def seattletimes_csv(seattletimes_raw: list[dict]) -> str:
    """Save Seattle Times feed data to CSV"""
    df = pd.DataFrame(seattletimes_raw)
    output_dir = Path("dagster_data/seattletimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Seattle Times assets
seattletimes_job = define_asset_job(
    "seattletimes_job",
    selection=["seattletimes_raw", "seattletimes_csv"],
    description="Job to fetch and process Seattle Times RSS feeds"
)

# Schedule to run every 4 hours
seattletimes_schedule = ScheduleDefinition(
    job=seattletimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="seattletimes_schedule",
    description="Run Seattle Times RSS feed collection every morning"
)
