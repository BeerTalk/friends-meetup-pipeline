from pathlib import Path
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import numpy as np

CHARTS = Path("data_explore/charts")
CHARTS.mkdir(parents = True, exist_ok = True)
con = duckdb.connect()
#2010-10-19T23:55:27Z time stamp sample, no headers
#https://snap.stanford.edu/data/loc-gowalla.html
con.sql("""
    CREATE view checkins as 
    
    SELECT user_id, strptime(ts, '%Y-%m-%dT%H:%M:%SZ')
    as ts_utc , lat, lon, venue_id
    from read_csv('data/raw/loc-gowalla_totalCheckins.txt', delim = '\t', header = False,
        columns = {'user_id' : 'BIGINT', 'ts' :'VARCHAR', 'lat':'double', 'lon':'double', 'venue_id' :'BIGINT'})
""")

con.sql("""
    CREATE view edges as 
    SELECT * from read_csv('data/raw/loc-gowalla_edges.txt', delim = '\t', header = false,
        columns = {'user_a':'BIGINT', 'user_b' : 'BIGINT'})

""")

def query_df(sql):
    #query to dataframe
    return con.sql(sql).df()

def section(title):
    print (f"\n{'=' *70}\n {title}\n {'='*70}")

def save(fig, name):
    fig.tight_layout()
    fig.savefig(CHARTS / name, dpi = 120)
    plt.close(fig)
    print(f"Saved chart: {CHARTS / name}")


# 1. check for grain n keys
# check if there is duplicated check in or instance where user is at more than 1 place at a time

section("1. Grain n Key")
print(query_df("""
    SELECT  count(*) as checkins, count(distinct user_id) as users, count(distinct venue_id) as venues, count(distinct(user_id, venue_id, ts_utc)) as distinct_keys,
            min(ts_utc) as first_check, max(ts_utc) as last_check
    FROM checkins
""")
)

#2 volume seasonality check
section("2. volume over time")
mom = query_df("""
    SELECT  date_trunc('month', ts_utc) as month, count(*) as checkins,
            count(distinct user_id) as active_users from checkins
            group by 1
            order by 1

 """)
mom["mom_growth"] = mom["active_users"].pct_change().round(2)
print(mom.to_string(index = False))

fig,ax = plt.subplots(figsize=(9,4))
ax.plot(mom["month"], mom["active_users"], marker = "o")
ax.set_title("Monthly - Active Users")
ax.set_ylabel("Users")
save(fig, "1_MOM_active_users.png")

#3 check for null
section("3. Null counts")
print(query_df("""
SELECT  count(*) - count(user_id) as null_user,
        count(*) - count(ts_utc) as null_ts_utc,
        count(*) - count(lat) as null_lat,
        count(*) - count(lon) as null_lon,
        count(*) - count(venue_id) as null_venue_id
FROM checkins
"""))

#4. data validation
section("4. data validation")
print(query_df("""
SELECT  count(*) filter(where lat = 0 and lon = 0) as at_0_0,
        count(*) filter(where lat not between -90 and 90 or lon not between -180 and 180) as out_of_range,
        count(*) filter(where ts_utc < '2009-01-01') as before_2009,
        count(*) filter(where ts_utc > '2011-01-01') as after_2010
FROM checkins

"""))

#5. check for duplicates
section("5. duplicate check")
print(query_df("""
SELECT    (SELECT count(*) FROM checkins) 
        - (SELECT count(*) FROM (SELECT distinct * FROM checkins)) as exact_dup,
          (SELECT count(*) FROM checkins)
        - (SELECT count(distinct(user_id, venue_id, ts_utc)) FROM checkins) as key_duplicates
"""))

#6 spread check
section("6.user check in spread")
# num of checkins by user
per_user = query_df("SELECT user_id, count(*) as n from checkins group by 1")
top_1_percent = per_user["n"].quantile(0.99)
#check medium, 90% and top 1 %
print(per_user["n"].describe(percentiles = [0.5, 0.9, 0.99]).round(2).to_string())
print(f"Percent of users with 1 check-in: {(per_user["n"] ==1).mean():.1%}")
print(f"Percent of checkins from top 1% users: "f"{per_user.loc[per_user['n']>= top_1_percent,"n"]
.sum()/
            per_user["n"].sum():.1%}"
        )
fig,ax = plt.subplots(figsize = (8,4))
binss = np.logspace(0, np.log10(per_user["n"].max()+1),40)
ax.hist(per_user["n"], bins = binss)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_title("check-ins per user (log scale)")
ax.set_xlabel("checkins")
ax.set_ylabel("users")
save(fig,"2_checins_per_user_log10.png")

#7 find the busiest areas
con.sql("""
    create view checkins_area as 
    SELECT *, cast(round(lat) as int) || ',' || cast(round(lon) as int) as area
    FROM checkins
    where not(lon = 0 and lat = 0)
""")

areas = query_df("""
    SELECT area, count(*) as checkins, count(distinct(user_id)) as users
    FROM checkins_area
    group by 1
    order by users desc
    limit 10
""")

print(areas.to_string(index = False))
top3 = areas['area'].head(3).tolist()

#8 aggregate activities by time stamp
section("8, Time stamp patterns by area (UTC)")
hourly = query_df(f"""
    SELECT area, hour(ts_utc) as hour_utc, count(*) as checkins
    FROM checkins_area
    WHERE area in ({", ".join(f" '{a}'" for a in top3)})
    group by 1,2
""")

hourly['percent'] = hourly['checkins'] / hourly.groupby('area')['checkins'].transform("sum")


fig,ax = plt.subplots(figsize = (9,4))

for areas in top3:
    row = hourly[hourly['area']==areas].sort_values("hour_utc")
    ax.plot(row['hour_utc'], row['percent'], marker = 'o',label = areas)
ax.set_title("Percent of check in s by hour, top 3 are in UTC")
ax.set_xlabel("Hour UTC")
ax.set_ylabel("Percent of area checkins")
ax.set_xticks(range(0,24,2))
ax.legend(title = "area(lat,lon)")
save(fig, "3_hour_by_area.png")

#9 check if coordinates move for the venue
section("9. Venue coordinates consistency")
print(query_df(""" 
    SELECT count(*) as venues, count(*) filter(where lat_spread > 0.01 or lon_spread>0.01) as venues_moved
    FROM( SELECT venue_id, max(lat) - min(lat) as lat_spread, max(lon)-min(lon) as lon_spread from checkins group by 1)
"""))
#10 lets check the relationsips https://snap.stanford.edu/data/loc-gowalla.html

section ("10. Friendship check")
print(query_df("""
    SELECT  count(*) as directed_edges,
            count(*) filter (where user_a < user_b) as unique_pairs,
            count(*) filter(where user_a = user_b) as invalid_pairs,
            count(*) filter(where not exists( SELECT 1 from edges r where r.user_a = e.user_b and r.user_b = e.user_a))
            as edges_missing
            FROM edges e
"""))

print(query_df("""
    with active as (select distinct user_id from checkins),
         friend_users as (select distinct user_a as user_id from edges)
    select (select count(*) from active) as users_with_checkins,
           (select count(*) from friend_users) as users_in_friend_graph,
           (select count(*) from active join friend_users using (user_id)) as in_both
"""))

#11 time between checkins
section("11. time between user's consecutive checkins in mins")
print(query_df("""
    with gaps as (
        select date_diff('minute',
                                lag(ts_utc) over (partition by user_id order by ts_utc), 
                                ts_utc) as gap_min 
                                from checkins)
        select  quantile_cont(gap_min, 0.25) as percent_25,
                quantile_cont(gap_min, 0.5) as median,
                quantile_cont(gap_min, 0.9) as percent_90,
                count(*) filter (where gap_min = 0) as same_time
        from gaps
        where gap_min is not null
"""))
#section 12
section("12. Feasibility: friend meetups at the same venue within 60 minutes")

# Put each check-in in an hour bucket, so we only compare check-ins close in time
con.sql("""
    create or replace table venue_hours as
    select distinct user_id, venue_id, ts_utc,
           cast(epoch(ts_utc) // 3600 as bigint) as hour_bucket
    from checkins
""")

# Friends at the same venue within 60 minutes.
# Each check-in is also copied into the hour before and after,
# so 10:50 and 11:20 still match across the hour boundary.
con.sql("""
    create or replace table meetups as
    with b_expanded as (
        select v.user_id, v.venue_id, v.ts_utc, v.hour_bucket + o.k as hour_bucket
        from venue_hours v
        cross join (values (-1), (0), (1)) as o(k)
    )
    select distinct a.user_id as user_a, b.user_id as user_b, a.venue_id,
           least(a.ts_utc, b.ts_utc) as met_at
    from venue_hours a
    join b_expanded b
      on a.venue_id = b.venue_id
     and a.hour_bucket = b.hour_bucket
     and a.user_id < b.user_id
     and abs(date_diff('minute', a.ts_utc, b.ts_utc)) <= 60
    join edges e
      on e.user_a = a.user_id
     and e.user_b = b.user_id
""")

print(query_df("""
    with active as (select distinct user_id from checkins),
    active_pairs as (
        select count(*) as n
        from edges e
        join active a1 on a1.user_id = e.user_a
        join active a2 on a2.user_id = e.user_b
        where e.user_a < e.user_b
    ),
    met_users as (
        select user_a as user_id from meetups
        union
        select user_b from meetups
    )
    select (select count(*) from meetups) as meetup_events,
           (select count(distinct (user_a, user_b)) from meetups) as friend_pairs_who_met,
           (select n from active_pairs) as active_friend_pairs,
           (select count(distinct (user_a, user_b)) from meetups)
               / (select n from active_pairs) as share_of_pairs_who_met,
           (select count(*) from met_users) as users_who_met,
           (select count(*) from met_users) / (select count(*) from active) as share_of_users_who_met
""").T)

