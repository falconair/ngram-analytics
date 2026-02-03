import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.twincities.com/feed/",
}


@asset
def twincities_raw() -> list[dict]:
    """Fetch all St. Paul Pioneer Press RSS feeds"""
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
def twincities_csv(twincities_raw: list[dict]) -> str:
    """Save St. Paul Pioneer Press feed data to CSV"""
    df = pd.DataFrame(twincities_raw)
    output_dir = Path("dagster_data/twincities")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Twin Cities assets
twincities_job = define_asset_job(
    "twincities_job",
    selection=["twincities_raw", "twincities_csv"],
    description="Job to fetch and process Twin Cities RSS feeds"
)

# Schedule to run every 8 hours
twincities_schedule = ScheduleDefinition(
    job=twincities_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="twincities_schedule",
    description="Run Twin Cities RSS feed collection every morning"
)
