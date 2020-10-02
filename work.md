# Coding Challenge

## Preparation

While there are many fields in the log file, to achive goal of this challenge, we only need some of them.

Specifically, access timestamp, client IP address, and URL is required.

A script (located in `data/prep.js`) is written to extract those fileds.

The suggested Hortonworks HDP Sandbox is being used. (https://www.cloudera.com/downloads/hortonworks-sandbox.html)

```bash
node prep.js 2015_07_22_mktplace_shop_web_log_sample.log ppchal.csv
```

By running the command above, we'll get a CSV file like this ready to be imported to Hadoop.

```csv
1437555628019,123.242.248.130,https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null
1437555627894,203.91.211.44,https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2
1437555627885,1.39.32.179,https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2
1437555628048,180.179.213.94,https://paytm.com:443/shop/p/micromax-yu-yureka-moonstone-grey-MOBMICROMAX-YU-DUMM141CD60AF7C_34315
1437555628036,120.59.192.208,https://paytm.com:443/papi/v1/expresscart/verify
1437555628033,117.239.195.66,https://paytm.com:443/api/user/favourite?channel=web&version=2
1437555628055,101.60.186.26,https://paytm.com:443/favicon.ico
```

```bash
# Upload CSV file to HDFS
hdfs dfs -put ppchal.csv /tmp/
```

HiveQL is used to query and manipulate all the data. It allows user to write SQL-style queries while transforming those commands into actual MapReduce code.

```sql
# Creates a Hive table
CREATE TABLE IF NOT EXISTS ppchal (
    ts BIGINT,
    ip STRING,
    url STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

# Imports data in to the table
LOAD DATA INPATH '/tmp/log_cleaned.csv' OVERWRITE INTO TABLE ppchal;
```

## Determine Session

To identify sessions using time window. Window function might be useful.
Assuming each IP address is an unique user.

```sql
# To retreieve timestamp of last hit before current
last_value(ts) OVER (PARTITION BY ip ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
```

By substracting such value from current hit timestamp, we are now able to check how long it has passed since last hit.

If the value does not exist or it's greater thatn 15 minutes, that hit is treated as the start of a new session.

```sql
CREATE TEMPORARY TABLE ppchal_with_sess_mark AS
SELECT IF (
           nvl(ts - last_value(ts) OVER (PARTITION BY ip
                                         ORDER BY ts ASC
                                         RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                                        ) - 15*60*1000, 1
              ) > 0,
           1,
           0
          ) AS is_sess_start, *
FROM ppchal;
```

By marking session starting points, it's now easier to group hits by session.

To make queries about sesions possible, it's required to identify a session in some way.
The timestamp of the first hit in the session is defined to be the session ID.

Although there may be many hits coming at the same time from different user, uniqueness is maintained by involving IP address in upcoming queries.

```sql
CREATE TEMPORARY TABLE ppchal_with_sess_id AS
SELECT max(is_sess_start * ts) OVER (PARTITION BY ip
                                     ORDER BY ts ASC
                                     RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                    ) as sess_id,
       *
FROM ppchal_with_sess_mark;
```

## Analytical Goals

> 2. Determine the average session time

Average session time can easily be calculated using aggregation functions.

```sql
WITH t AS (SELECT max(ts) - min(ts) AS duration FROM ppchal_with_sess_id GROUP BY ip, sess_id)
SELECT avg(duration) FROM t;
```

```
+---------------------+ 
| _c0 |
+---------------------+
| 100198.65927142512 |
+---------------------+
```

The data imported is in ms, so average session time is around 100 seconds.

I've noticed that there are actually many sessions (around 20.34%) with only one hit, those sessions with zero duration influences the result.

```sql
WITH t AS (SELECT IF(max(ts) - min(ts) = 0, 1, 0) AS is_zero_dur_sess FROM ppchal_with_sess_id GROUP BY ip, sess_id)
SELECT count(*), is_zero_dur_sess FROM t GROUP BY is_zero_dur_sess;
```

```
+--------+----------------------+
| _c0 | is_zero_dur_sess |
+--------+----------------------+
| 22667 | 1 |
| 88756 | 0 |
+--------+----------------------+
```

> 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

By using `DISTINCT`, counting unique values is possible. The following query shows unique URLs by sessions.

```sql
SELECT ip, sess_id, count(DISTINCT url) as unique_hits from ppchal_with_sess_id GROUP BY ip, sess_id;
```

It's also possible to find out average unique hits per session.

```sql
WITH t AS (SELECT ip, sess_id, count(DISTINCT url) as unique_hits from ppchal_with_sess_id GROUP BY ip, sess_id) SELECT AVG(unique_hits) FROM t;
```

A session involves around 8.2 hits in average.

```
+--------------------+
| _c0 |
+--------------------+
| 8.271146890677867 |
+--------------------+
```

> 4. Find the most engaged users, ie the IPs with the longest session times

```sql
SELECT max(ts) - min(ts) AS duration, count(DISTINCT url), ip, sess_id from ppchal_with_sess_id GROUP BY ip, sess_id ORDER BY duration DESC limit 100;
```

Many sessions have long durations as shown.

```
+-----------+------------------+----------------+
| duration | ip | sess_id |
+-----------+------------------+----------------+
| 2069162 | 52.74.219.71 | 1437561028220 |
| 2068849 | 119.81.61.166 | 1437561028314 |
| 2068756 | 106.186.23.95 | 1437561028615 |
| 2068713 | 125.19.44.66 | 1437561028187 |
| 2068320 | 125.20.39.66 | 1437561028338 |
| 2067235 | 192.8.190.10 | 1437561029188 |
| 2067023 | 54.251.151.39 | 1437561030025 |
| 2066961 | 180.211.69.209 | 1437561028361 |
| 2065638 | 180.179.213.70 | 1437561028929 |
| 2065594 | 203.189.176.14 | 1437561030693 |
| 2065587 | 213.239.204.204 | 1437561029067 |
| 2065520 | 122.15.156.64 | 1437561029904 |
```