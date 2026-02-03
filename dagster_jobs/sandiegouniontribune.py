import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "news": "https://www.sandiegouniontribune.com/arcio/feed/",
    "sports": "https://www.sandiegouniontribune.com/sports/feed/",
}


@asset
def sandiegouniontribune_raw() -> list[dict]:
    """Fetch all San Diego Union-Tribune RSS feeds"""
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
def sandiegouniontribune_csv(sandiegouniontribune_raw: list[dict]) -> str:
    """Save San Diego Union-Tribune feed data to CSV"""
    df = pd.DataFrame(sandiegouniontribune_raw)
    output_dir = Path("dagster_data/sandiegouniontribune")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for San Diego Union Tribune assets
sandiegouniontribune_job = define_asset_job(
    "sandiegouniontribune_job",
    selection=["sandiegouniontribune_raw", "sandiegouniontribune_csv"],
    description="Job to fetch and process San Diego Union Tribune RSS feeds"
)

# Schedule to run every 6 hours
sandiegouniontribune_schedule = ScheduleDefinition(
    job=sandiegouniontribune_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="sandiegouniontribune_schedule",
    description="Run San Diego Union Tribune RSS feed collection every morning"
)
