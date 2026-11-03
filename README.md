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

## Findings

To do.

## What changes at 100x

To do.
