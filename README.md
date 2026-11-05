# Do friends who meet up stick around?

Location data pipeline on Gowalla check-ins and friendships (Stanford SNAP loc-gowalla dataset:
about 6.4M check-ins from roughly 107k users, Feb 2009 to Oct 2010, plus the friendship network).
Question: does meeting friends in person early on relate to retention, and could a "friends nearby"
nudge help?

## Stack

Spark (filter and enrich raw data) -> BigQuery (warehouse) -> dbt (models and tests) -> Dagster
(orchestration) -> Streamlit (results).

## Project layout

- `spark/` raw data to clean Parquet with H3 cells and local time
- `scripts/` download and load to BigQuery
- `data_explore/` data profiling script, output and charts
- `dbt_project/` staging and mart models, tests
- `orchestration/` Dagster definitions
- `app/` Streamlit demo

## How to run

1. `bash scripts/download_data.sh`
2. `python data_explore/explore.py`
3. `python spark/prepare_checkins.py`
4. Set `BQ_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS`, then `python scripts/load_to_bigquery.py`
5. `cd dbt_project && dbt build`
6. `dagster dev -f orchestration/definitions.py`

## Data profile

6,442,892 check-ins by 107,092 users at 1,280,969 venues, from 4 February 2009 to 23 October 2010.
One row is one check-in: one user, one venue, one timestamp (UTC).

| Measure | Value | What it means |
| --- | --- | --- |
| Check-ins per user | median 25, mean 60 | Heavily skewed; report medians, not averages |
| Top 1% of users | 18.2% of all check-ins | A small group drives volume; cap or band heavy users in features |
| Users with one check-in | 7.4% | Most users returned at least once |
| Time between a user's check-ins | median 4.6 hours, 25% within 16 minutes | A 60-minute meetup window is strict enough to mean two people were there together |
| Busiest areas | Stockholm, Austin, San Francisco Bay Area, New York, Dallas | Four of the top 10 areas are in Sweden; results mix US and European behaviour |
| Friendships | 950,327 pairs among 196,591 users, all recorded in both directions | The graph is clean and symmetric |

## Data issues found

Ordered by impact on the analysis.

**1. The sample appears to include only users with friends.**
All 107,092 users who check in appear in the friendship graph; real apps always have users without friends.
*Impact:* this is likely a property of how the dataset was collected, so results describe socially connected users, not all Gowalla users. A comparison of users with and without friends is not possible.
*Action:* the analysis compares friends who meet in person with friends who do not, and findings are stated as applying to connected users only. To confirm against the dataset's collection method.

**2. Usable history starts in September 2009 and ends mid-October 2010.**
Fewer than 1,500 check-ins per month before September 2009, and the data stops on 23 October 2010.
*Impact:* early months are too small to analyse, and October 2010 looks like a 7% decline only because it is a partial month. Users who joined near the end have not had time to churn.
*Action:* trends use September 2009 to September 2010; retention analysis excludes users who joined in the final 120 days.

**3. About 10% of check-ins happen within the same minute as the user's previous one.**
661,978 check-ins, at different venues (true duplicates are only 601).
*Impact:* if these are rapid batch check-ins rather than real visits, they could create false friend meetups.
*Action:* to investigate gap in seconds and distance between these check-ins before meetup detection; flagged in the meetups model until resolved.

**4. Timestamps are in UTC with no time zone.**
*Impact:* hour-of-day and day-of-week patterns are shifted for every city, by 6 to 8 hours for the US.
*Action:* local time is derived from each venue's coordinates in the Spark step.

**5. 89,499 users in the friendship graph never check in.**
*Impact:* friendships involving them can never produce a meetup, so meetup rates are lower than they would be among active users.
*Action:* meetup rates are calculated over friend pairs where both users have check-ins.

**6. Small data quality issues.**
135 check-ins at (0, 0), 29 with impossible coordinates, and 601 exact duplicate rows. Every duplicate key is an exact copy, so there are no conflicting versions.
*Impact:* under 0.01% of rows, but (0, 0) points would form a false busy location and duplicates could create false meetups.
*Action:* removed in `stg_checkins` and covered by dbt tests.

**7. Confirmed clean.**
No null values in any column; no timestamps outside the documented period; only 12 of 1,280,969 venues change location by more than about 1 km.

## Pipeline and engineering decisions

### Row reconciliation

Every step was checked against the one before it, so no rows were lost or duplicated along the way.

| Stage | Check-ins | Friendship rows | Note |
| --- | --- | --- | --- |
| Raw files | 6,442,892 | 1,900,654 | Each friendship listed in both directions |
| After Spark cleaning | 6,442,728 | 1,900,654 | 135 (0, 0) points and 29 out-of-range points removed |
| Loaded into BigQuery | 6,442,728 | 1,900,654 | Matches exactly |

The 601 duplicate check-ins are kept in the raw layer on purpose and removed in the dbt staging model, so the raw tables stay a faithful copy of the source.

### Spark: clean, add local time and H3 cells

- **Time zone and H3 cell are computed once per venue, not once per check-in:** 1.28M lookups instead of 6.44M, then joined back. This is safe because venue coordinates are stable (only 12 venues move by more than about 1 km).
- **Vectorised (pandas) UDFs** for the time zone and H3 lookups, which process rows in batches rather than one at a time.
- **Local time uses time zone names** (such as `Europe/Stockholm`), so daylight saving is handled correctly. Top zones by check-ins: America/Chicago (1.37M, covering Austin, Dallas and Chicago), America/New_York (1.03M), Europe/Stockholm (0.93M), America/Los_Angeles (0.91M).
- **Output is Parquet, partitioned by month**, written with overwrite, so rerunning the job always produces the same result.

### BigQuery: load and table design
cd /workspaces/friends-meetup-pipeline/dbt_project
- **Partition pruning, verified with a dry run:** selecting `ts_utc` for October 2010 scans 5,939,664 bytes, exactly 742,458 rows × 8 bytes, so only October's partitions are read. The same column across the whole table is 51,541,824 bytes, so the date filter reads about 88% less data.
- **`raw.checkins` is partitioned by day on `ts_utc` and clustered by `user_id`.** Most queries filter on a time window and compute features per user, so this limits the data each query scans.
- **Parquet files are uploaded directly to BigQuery.** Loading through pandas ran out of memory, because it held two full copies of 6.4M rows. Direct upload streams each file from disk and keeps the column types.
- **The load is idempotent:** the first file replaces the table and the rest append. When a load was interrupted partway through, a rerun rebuilt the table cleanly with no manual cleanup.
- **At larger scale:** stage the files in Cloud Storage and run a single wildcard load job, which is faster than 62 separate jobs and either fully succeeds or fully fails.


## Findings

**Month-2 retention: did the user check in again between day 30 and day 60?** Features come from each user's first 30 days, so the outcome cannot leak into them. Users who joined in the last 60 days of data are excluded, because their month 2 cannot be observed (85,020 users included).

| Early activity (first 30 days) | Did not meet a friend | Met a friend | Difference |
| --- | --- | --- | --- |
| 1 to 4 check-ins | 28.9% | 30.5% | +1.6 points |
| 5 to 14 check-ins | 67.4% | 69.2% | +1.8 points |
| 15+ check-ins | 85.3% | 89.1% | +3.8 points |

- **Early activity is the strongest signal.** Retention rises from 29% to 67% to 85% across the activity bands.
- **Meeting a friend adds a small, consistent lift of 2 to 4 points in every band.** It is clearly significant in the 15+ band (about 10 standard errors), probably real in 5 to 14, and too small to be sure in 1 to 4.
- **Comparing within activity bands was essential.** Without bands, users who met a friend look 17 points better (about 77% versus 60%), because 63% of them are heavy users. Within bands the gap is 2 to 4 points, so the naive comparison overstates the effect about fivefold.
- **Most meetups look like friends arriving together:** the median gap between the two check-ins is 3 minutes.

**What this does not show:** this is correlation, not cause. Users who meet friends may differ in other ways, the sample contains only users with friends, and some meetups may be burst check-ins. A randomised test of a "friends nearby" nudge would be needed to measure its real effect.

**Product implication:** a meetup nudge is unlikely to transform retention; the realistic upside is a few points, mostly among already engaged users. Improving early engagement looks like the bigger lever.

### dbt models

| Model | Grain | Rows | Key tests |
| --- | --- | --- | --- |
| `stg_checkins` | one check-in | 6,442,127 | no nulls in key columns; 601 duplicates removed |
| `stg_friendships` | one friendship pair | 950,327 | each pair stored once |
| `fct_user_activity` | one user | 107,092 | `user_id` unique |
| `user_week_features` | one user per week | 978,900 | key unique; weekly totals add back to all check-ins |
| `meetups` | one friend pair per venue per day | 206,447 | key unique; pairs stored smaller ID first |
| `user_retention` | one user | 107,092 | one row per user; activity bands valid |

## What changes at 100x

To do.
