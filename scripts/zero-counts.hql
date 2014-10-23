set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=false;
--^ To work around HIVE-3296, we have SETs before any comments

-- Extracts zero stats from webrequests into a separate table
--
-- Usage:
--     hive -f zero-counts.hql -d year=2014 -d month=9 -d day=15 -d date=2014-09-15
--     Date is duplicated because I haven't figured an easy way to set date=printf()
--
-- set hivevar:year=2014;
-- set hivevar:month=10;
-- set hivevar:day=21;
-- set hivevar:date=2014-10-21;

use yurik;


CREATE TABLE IF NOT EXISTS zero_webstats (
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


-- ALTER TABLE zero_webstats DROP IF EXISTS PARTITION(date < '${date}')


INSERT OVERWRITE TABLE zero_webstats
    PARTITION(date="${date}") IF NOT EXISTS
    SELECT
        xcs, via, ipset, https, lang, subdomain, site, COUNT(*) count
    FROM (
        SELECT
            regexp_extract(x_analytics, 'zero=([^\;]+)') xcs,
            regexp_extract(x_analytics, 'proxy=([^\;]+)') via,
            regexp_extract(x_analytics, 'zeronet=([^\;]+)') ipset,
            if (x_analytics LIKE '%https=1%', 'https', '') https,
            regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 1) lang,
            regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 3) subdomain,
            regexp_extract(uri_host, '^([A-Za-z0-9-]+)(\\.(zero|m))?\\.([a-z]*)\\.org$', 4) site

        FROM wmf_raw.webrequest
        WHERE
            webrequest_source IN ('text', 'mobile')
            AND year=${year}
            AND month=${month}
            AND day=${day}
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

    ) prepared
    GROUP BY xcs, via, ipset, https, lang, subdomain, site
    DISTRIBUTE BY printf('%d-%02d-%02d', ${year}, ${month}, ${day});
