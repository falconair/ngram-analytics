from dagster import Definitions, load_assets_from_package_module, define_asset_job, ScheduleDefinition
import dagster_jobs
from dagster_jobs.abc13 import abc13_schedule
from dagster_jobs.baltimoresun import baltimoresun_schedule
from dagster_jobs.boston_com import boston_com_schedule
from dagster_jobs.bostonglobe import bostonglobe_schedule
from dagster_jobs.bostonherald import bostonherald_schedule
from dagster_jobs.chicago_suntimes import chicago_suntimes_schedule
from dagster_jobs.chicagotribune import chicagotribune_schedule
from dagster_jobs.cleveland import cleveland_schedule
from dagster_jobs.courant import courant_schedule
from dagster_jobs.dailynews import dailynews_schedule
from dagster_jobs.denverpost import denverpost_schedule
from dagster_jobs.fox2now import fox2now_schedule
from dagster_jobs.fox5sandiego import fox5sandiego_schedule
from dagster_jobs.houstonchronicle import houstonchronicle_schedule
from dagster_jobs.kdvr import kdvr_schedule
from dagster_jobs.kron4 import kron4_schedule
from dagster_jobs.ktla import ktla_schedule
from dagster_jobs.kxan import kxan_schedule
from dagster_jobs.laist import laist_schedule
from dagster_jobs.latimes import latimes_schedule
from dagster_jobs.mercurynews import mercurynews_schedule
from dagster_jobs.metrotimes import metrotimes_schedule
from dagster_jobs.nj import nj_schedule
from dagster_jobs.nydailynews import nydailynews_schedule
from dagster_jobs.nypost import nypost_schedule
from dagster_jobs.nytimes import nytimes_schedule
from dagster_jobs.ocregister import ocregister_schedule
from dagster_jobs.omaha import omaha_schedule
from dagster_jobs.oregonlive import oregonlive_schedule
from dagster_jobs.orlandosentinel import orlandosentinel_schedule
from dagster_jobs.post_gazette import post_gazette_schedule
from dagster_jobs.reviewjournal import reviewjournal_schedule
from dagster_jobs.sandiegouniontribune import sandiegouniontribune_schedule
from dagster_jobs.seattletimes import seattletimes_schedule
from dagster_jobs.staradvertiser import staradvertiser_schedule
from dagster_jobs.startribune import startribune_schedule
from dagster_jobs.triblive import triblive_schedule
from dagster_jobs.tulsaworld import tulsaworld_schedule
from dagster_jobs.twincities import twincities_schedule
from dagster_jobs.washingtonpost import washingtonpost_schedule
from dagster_jobs.washingtontimes import washingtontimes_schedule
from dagster_jobs.wfla import wfla_schedule
from dagster_jobs.wivb import wivb_schedule

all_assets = load_assets_from_package_module(dagster_jobs)

daily_job = define_asset_job("daily_feeds_job", selection="*")
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="0 6 * * *")

# Collect all individual RSS feed schedules
all_rss_schedules = [
    abc13_schedule,
    baltimoresun_schedule,
    boston_com_schedule,
    bostonglobe_schedule,
    bostonherald_schedule,
    chicago_suntimes_schedule,
    chicagotribune_schedule,
    cleveland_schedule,
    courant_schedule,
    dailynews_schedule,
    denverpost_schedule,
    fox2now_schedule,
    fox5sandiego_schedule,
    houstonchronicle_schedule,
    kdvr_schedule,
    kron4_schedule,
    ktla_schedule,
    kxan_schedule,
    laist_schedule,
    latimes_schedule,
    mercurynews_schedule,
    metrotimes_schedule,
    nj_schedule,
    nydailynews_schedule,
    nypost_schedule,
    nytimes_schedule,
    ocregister_schedule, 
    omaha_schedule,
    oregonlive_schedule,
    orlandosentinel_schedule,
    post_gazette_schedule,
    reviewjournal_schedule,
    sandiegouniontribune_schedule,
    seattletimes_schedule,
    staradvertiser_schedule,
    startribune_schedule,
    triblive_schedule,
    tulsaworld_schedule,
    twincities_schedule,
    washingtonpost_schedule,
    washingtontimes_schedule,
    wfla_schedule,
    wivb_schedule,
]

defs = Definitions(assets=all_assets, schedules=[daily_schedule] + all_rss_schedules)
