import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://abc13.com/feed/",
}


@asset
def abc13_raw() -> list[dict]:
    """Fetch all ABC13 Houston RSS feeds"""
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
def abc13_csv(abc13_raw: list[dict]) -> str:
    """Save ABC13 Houston feed data to CSV"""
    df = pd.DataFrame(abc13_raw)
    output_dir = Path("dagster_data/abc13")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for ABC13 assets
abc13_job = define_asset_job(
    "abc13_job",
    selection=["abc13_raw", "abc13_csv"],
    description="Job to fetch and process ABC13 RSS feeds"
)

# Schedule to run every morning at 6 AM
abc13_schedule = ScheduleDefinition(
    job=abc13_job,
    cron_schedule="0 6 * * *",  # Every day at 6 AM
    name="abc13_schedule",
    description="Run ABC13 RSS feed collection every morning at 6 AM"
)