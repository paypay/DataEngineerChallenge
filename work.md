node prep.js 2015_07_22_mktplace_shop_web_log_sample.log log_cleaned.csv
hdfs dfs -put log_cleaned.csv /tmp/

CREATE TABLE IF NOT EXISTS ppchallenge (
    ts BIGINT,
    ip STRING,
    url STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/log_cleaned.csv' OVERWRITE INTO TABLE ppchallenge;

CREATE TEMPORARY TABLE ppchal_sorted AS SELECT * FROM ppchallenge ORDER BY ts ASC;

CREATE TEMPORARY TABLE ppchal_with_neighbouring_ts AS SELECT ts - last_value(ts) OVER (PARTITION BY ip ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS t_from_prev_hit, first_value(ts) OVER (PARTITION BY ip ORDER BY ts ASC RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) - ts AS t_to_next_hit, * from ppchal_sorted;

CREATE TEMPORARY TABLE ppchal_with_sess_mark AS SELECT IF(isnull(t_from_prev_hit) OR t_from_prev_hit > 15*60*1000, 1, 0) as is_sess_start, IF(isnull(t_to_next_hit) OR t_to_next_hit > 15*60*1000, 1, 0) as is_sess_end, ts, ip, url from ppchal_with_neighbouring_ts LIMIT 100;

CREATE TEMPORARY TABLE ppchal_with_sess_sort_key AS SELECT (is_sess_start * 100000000000000 + ts) as sess_start_sort_key, (if(is_sess_end = 1, 0, 1) * 100000000000000 + ts) as sess_end_sort_key, * from ppchal_with_sess_mark;


CREATE TEMPORARY TABLE ppchal_with_sess_mark AS SELECT if (nvl(ts - last_value(ts) OVER (PARTITION BY ip ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) - 15*60*1000, 1) > 0, 1, 0) AS is_sess_start, * from ppchallenge;

CREATE TEMPORARY TABLE ppchal_with_sess_id AS SELECT max(is_sess_start * ts) OVER (PARTITION BY ip order by ts range between UNBOUNDED PRECEDING AND CURRENT ROW) as sess_id, * FROM ppchal_with_sess_mark



Average session time

WITH t as (select max(ts) - min(ts) as duration from ppchal_with_sess_id GROUP BY ip, sess_id) SELECT avg(duration) FROM t

+---------------------+ 
| _c0 |
+---------------------+
| 100198.65927142512 |
+---------------------+

Unique URL visits per session

SELECT ip, sess_id, count(DISTINCT url) as unique_hits from ppchal_with_sess_id GROUP BY ip, sess_id limit 100;



Most engaged user

select max(ts) - min(ts) as duration, count(DISTINCT url), ip, sess_id from ppchal_with_sess_id GROUP BY ip, sess_id ORDER BY duration DESC limit 100;