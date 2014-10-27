set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=false;
--^ To work around HIVE-3296, we have SETs before any comments

-- Clone one day worth of data to a temp table
--
-- Usage:
--     hive -f clone-xcs.hql -d year=2014 -d month=10 -d day=11 -d hour=12 -d xcs=515-05 -d table=tmp_clone
--
-- set hivevar:year=2014;
-- set hivevar:month=10;
-- set hivevar:day=11;
-- set hivevar:hour=12;
-- set hivevar:xcs=515-05;
-- set hivevar:table=tmp_clone;

use yurik;

CREATE TABLE IF NOT EXISTS ${table} (
  `hostname` string,
  `sequence` bigint,
  `dt` string,
  `time_firstbyte` float,
  `ip` string,
  `cache_status` string,
  `http_status` string,
  `response_size` bigint,
  `http_method` string,
  `uri_host` string,
  `uri_path` string,
  `uri_query` string,
  `content_type` string,
  `referer` string,
  `x_forwarded_for` string,
  `user_agent` string,
  `accept_language` string,
  `x_analytics` string,
  `webrequest_source` string,
  `year` int,
  `month` int,
  `day` int,
  `hour` int)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ${table}

        SELECT
            *
        FROM wmf_raw.webrequest
        WHERE
            webrequest_source IN ('text', 'mobile')
            AND year=${year}
            AND month=${month}
            AND day=${day}
            AND hour=${hour}
            AND x_analytics LIKE '%zero=%'
            AND SUBSTR(uri_path, 1, 6) = '/wiki/'
            AND (
                    (
                        SUBSTR(ip, 1, 9) != '10.128.0.'
                        AND SUBSTR(ip, 1, 11) NOT IN (
                            '208.80.152.',
                            '208.80.153.',
                            '208.80.154.',
                            '208.80.155.',
                            '91.198.174.'
                        )
                    ) OR x_forwarded_for != '-'
                )
            AND SUBSTR(uri_path, 1, 31) != '/wiki/Special:CentralAutoLogin/'
            AND http_status NOT IN ( '301', '302', '303' )
            AND uri_host RLIKE '^[A-Za-z0-9-]+(\\.(zero|m))?\\.[a-z]*\\.org$'
            AND NOT (SPLIT(TRANSLATE(SUBSTR(uri_path, 7), ' ', '_'), '#')[0] RLIKE '^[Uu]ndefined$')
            AND regexp_extract(x_analytics, 'zero=([^\;]+)') = '${xcs}'
;
