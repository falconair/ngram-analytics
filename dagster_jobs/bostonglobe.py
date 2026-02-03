import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.bostonglobe.com/arc/outboundfeeds/rss/section/photo/bigpicture/",
    "business": "https://www.bostonglobe.com/rss/business/",
    "metro": "https://www.bostonglobe.com/rss/metro/",
    "opinion": "https://www.bostonglobe.com/rss/opinion/",
    "sports": "https://www.bostonglobe.com/rss/sports/",
}


@asset
def bostonglobe_raw() -> list[dict]:
    """Fetch all Boston Globe RSS feeds"""
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
def bostonglobe_csv(bostonglobe_raw: list[dict]) -> str:
    """Save Boston Globe feed data to CSV"""
    df = pd.DataFrame(bostonglobe_raw)
    output_dir = Path("dagster_data/bostonglobe")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Boston Globe assets
bostonglobe_job = define_asset_job(
    "bostonglobe_job",
    selection=["bostonglobe_raw", "bostonglobe_csv"],
    description="Job to fetch and process Boston Globe RSS feeds"
)

# Schedule to run every 4 hours
bostonglobe_schedule = ScheduleDefinition(
    job=bostonglobe_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="bostonglobe_schedule",
    description="Run Boston Globe RSS feed collection every morning"
)
