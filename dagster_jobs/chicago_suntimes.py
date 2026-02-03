import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://chicago.suntimes.com/rss/index.xml",
    "news": "https://chicago.suntimes.com/rss/news.xml",
    "politics": "https://chicago.suntimes.com/rss/politics.xml",
    "sports": "https://chicago.suntimes.com/rss/sports.xml",
}


@asset
def chicago_suntimes_raw() -> list[dict]:
    """Fetch all Chicago Sun-Times RSS feeds"""
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
def chicago_suntimes_csv(chicago_suntimes_raw: list[dict]) -> str:
    """Save Chicago Sun-Times feed data to CSV"""
    df = pd.DataFrame(chicago_suntimes_raw)
    output_dir = Path("dagster_data/chicago_suntimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Chicago Sun Times assets
chicago_suntimes_job = define_asset_job(
    "chicago_suntimes_job",
    selection=["chicago_suntimes_raw", "chicago_suntimes_csv"],
    description="Job to fetch and process Chicago Sun Times RSS feeds"
)

# Schedule to run every 6 hours
chicago_suntimes_schedule = ScheduleDefinition(
    job=chicago_suntimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="chicago_suntimes_schedule",
    description="Run Chicago Sun Times RSS feed collection every morning"
)
