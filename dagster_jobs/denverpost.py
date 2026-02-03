import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.denverpost.com/feed/",
    "business": "https://www.denverpost.com/business/feed/",
    "entertainment": "https://www.denverpost.com/entertainment/feed/",
    "news": "https://www.denverpost.com/news/feed/",
    "sports": "https://www.denverpost.com/sports/feed/",
}


@asset
def denverpost_raw() -> list[dict]:
    """Fetch all Denver Post RSS feeds"""
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
def denverpost_csv(denverpost_raw: list[dict]) -> str:
    """Save Denver Post feed data to CSV"""
    df = pd.DataFrame(denverpost_raw)
    output_dir = Path("dagster_data/denverpost")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Denver Post assets
denverpost_job = define_asset_job(
    "denverpost_job",
    selection=["denverpost_raw", "denverpost_csv"],
    description="Job to fetch and process Denver Post RSS feeds"
)

# Schedule to run every 6 hours
denverpost_schedule = ScheduleDefinition(
    job=denverpost_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="denverpost_schedule",
    description="Run Denver Post RSS feed collection every morning"
)
