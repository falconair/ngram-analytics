import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.houstonchronicle.com/rss/feed/Texas-news-205.php",
    "houston_news": "https://www.houstonchronicle.com/rss/feed/Houston-news-304.php",
    "texas_politics": "https://www.houstonchronicle.com/rss/feed/Texas-politics-213.php",
}


@asset
def houstonchronicle_raw() -> list[dict]:
    """Fetch all Houston Chronicle RSS feeds"""
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
def houstonchronicle_csv(houstonchronicle_raw: list[dict]) -> str:
    """Save Houston Chronicle feed data to CSV"""
    df = pd.DataFrame(houstonchronicle_raw)
    output_dir = Path("dagster_data/houstonchronicle")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Houston Chronicle assets
houstonchronicle_job = define_asset_job(
    "houstonchronicle_job",
    selection=["houstonchronicle_raw", "houstonchronicle_csv"],
    description="Job to fetch and process Houston Chronicle RSS feeds"
)

# Schedule to run every 4 hours
houstonchronicle_schedule = ScheduleDefinition(
    job=houstonchronicle_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="houstonchronicle_schedule",
    description="Run Houston Chronicle RSS feed collection every morning"
)
