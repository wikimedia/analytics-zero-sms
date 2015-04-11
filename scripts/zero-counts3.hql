set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=false;
--^ To work around HIVE-3296, we have SETs before any comments

-- Extracts zero stats from webrequests into a separate table
--
-- Usage:
--     hive -f zero-counts3.hql -d year=2015 -d month=4 -d day=9 -d date=2015-04-09 -d dsttable=zero_webstats3
--     Date is duplicated because I haven't figured an easy way to set date=printf()
--
-- set hivevar:year=2014;
-- set hivevar:month=10;
-- set hivevar:day=21;
-- set hivevar:date=2014-10-21;
-- set hivevar:dsttable=zero_webstats3;

use yurik;


CREATE TABLE IF NOT EXISTS ${dsttable} (
  xcs string,
  via string,
  ipset string,
  https string,
  lang string,
  subdomain string,
  site string,
  count bigint)
PARTITIONED BY (
  date string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t';


-- ALTER TABLE ${dsttable} DROP IF EXISTS PARTITION(date < '${date}')


INSERT OVERWRITE TABLE ${dsttable}
    PARTITION(date="${date}") IF NOT EXISTS
    SELECT
        xcs, via, ipset, https, lang, subdomain, site, COUNT(*) count
    FROM (
        SELECT
            COALESCE(regexp_extract(x_analytics, 'zero=([^\;]+)'), '') xcs,
            COALESCE(regexp_extract(x_analytics, 'proxy=([^\;]+)'), '') via,
            COALESCE(regexp_extract(x_analytics, 'zeronet=([^\;]+)'), '') ipset,
            if (x_analytics LIKE '%https=1%', 'https', '') https,
            COALESCE(regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 1), '') lang,
            COALESCE(regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 3), '') subdomain,
            COALESCE(regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 4), '') site

        FROM wmf.webrequest
        WHERE
            webrequest_source IN ('text', 'mobile')
            AND year=${year}
            AND month=${month}
            AND day=${day}
            AND x_analytics LIKE '%zero=%'  -- use is_zero field instead, once db is fixed
            AND is_pageview
    ) prepared
    GROUP BY xcs, via, ipset, https, lang, subdomain, site
    DISTRIBUTE BY printf('%d-%02d-%02d', ${year}, ${month}, ${day});
