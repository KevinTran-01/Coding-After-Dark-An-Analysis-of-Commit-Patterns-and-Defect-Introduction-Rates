"""
converts UTC timestamps to developer local time when commits were pushed
"""

import pandas as pd
import time
import pytz
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from timezonefinder import TimezoneFinder
from dotenv import load_dotenv

load_dotenv()

INPUT_CSV = "commits.csv"
OUTPUT_CSV = "updated_commits.csv"

tf = TimezoneFinder()
geolocator = Nominatim(user_agent="commit_timezone_research")
location_cache = {}

geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=2,    # 2 seconds between requests
    max_retries=3,           # retry 3 times
    error_wait_seconds=10    # wait 10 sec if a 429 error
)

"""
get the coordinate of location with geolocator
then get the timezone of that coordinate with TimezoneFinder
"""
def location_to_timezone(location_str):
    if not isinstance(location_str, str) or location_str.strip() == "":
        return None
    
    location_str = location_str.strip().lower()

    if location_str in location_cache:
        return location_cache[location_str]

    try:
        geo = geocode(location_str, timeout=5)
        if geo:
            tz_str = tf.timezone_at(lat=geo.latitude, lng=geo.longitude)
            location_cache[location_str] = tz_str
            time.sleep(1.1) 
            return tz_str
        
    except Exception as e:
        print(f"Could not resolve location '{location_str}': {e}")

    location_cache[location_str] = None
    return None

"""
Parameters: UTC time stamp string, time zone string
ex: 2024-03-15 02:30:00+00:00, America/Los Angeles
Convert the UTC time to developer's time based on their time zone
"""
def convert_to_local(utc_ts_str, tz_str):
    """Convert UTC to local time"""
    try:
        utc_dt = pd.to_datetime(utc_ts_str, utc=True)
        local_tz = pytz.timezone(tz_str)
        local_dt = utc_dt.astimezone(local_tz)
        return local_dt.hour, local_dt.isoformat()

    except Exception:
        return None, None


"""
For later analysis
"""
def assign_time_bucket(hour):
    if hour is None:
        return "unknown"
    if 0 <= hour < 4:
        return "0-4: late night"
    elif 4 <= hour < 9:
        return "4-9: early morning"
    elif 9 <= hour < 17:
        return "9-17: work hours"
    elif 17 <= hour < 21:
        return "evening"
    else:
        return "night"

# load INPUT file
df = pd.read_csv(INPUT_CSV)
 
local_hours = []
local_timestamps = []
time_buckets = []
timezone_used = []
timezone_resolved = []

unique_locations = df["author_location"].dropna().unique()
print(f"{len(unique_locations)} unique locations\n")

for loc in unique_locations:
    tz=location_to_timezone(loc)
    status= tz if tz else "unresolved"
    print(f"{loc!r:40s} - {status}")

for _, row in df.iterrows():
    location = row.get("author_location", "")
    utc_ts = row.get("utc_timestamp", "")
    tz_str = location_to_timezone(location)

    if tz_str:
        local_hour, local_ts = convert_to_local(utc_ts, tz_str)
        resolved = True
    else:
        local_hour=row.get("utc_hour", None)
        local_ts = utc_ts
        resolved = False
    
    bucket = assign_time_bucket(local_hour)

    local_hours.append(local_hour)
    local_timestamps.append(local_ts)
    time_buckets.append(bucket)
    timezone_used.append(tz_str if tz_str else "UTC_fallback")
    timezone_resolved.append(resolved)

df["local_hour"] = local_hours
df["local_timestamp"] = local_timestamps
df["time_bucket"] = time_buckets
df["timezone_used"] = timezone_used
df["timezone_resolved"] = timezone_resolved

# Save
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")