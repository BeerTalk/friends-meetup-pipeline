-- One row per real check-in: valid coordinates, deduplicated, with local time and a geography point.
select
    user_id,
    venue_id,
    ts_utc,
    tz_offset_min,
    datetime(timestamp_add(ts_utc, interval tz_offset_min minute)) as ts_local,
    lat,
    lon,
    st_geogpoint(lon, lat) as geo,  -- longitude first
    h3_r9,
    category,
    country
from {{ source('raw', 'checkins') }}
where lat between -90 and 90
  and lon between -180 and 180
  and not (lat = 0 and lon = 0)
  and ts_utc is not null
qualify row_number() over (partition by user_id, venue_id, ts_utc order by ts_utc) = 1
