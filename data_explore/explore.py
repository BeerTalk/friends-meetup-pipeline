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
save(fig,"checins_per_user_log10.png")

#7 find the busiest areas
con.sql("""
    create view checkins_area as 

""")