SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=false;
--^ To work around HIVE-3296, we have SETs before any comments

-- Count all per-country webrequests into a separate table
--
-- Usage:
--     hive -f countrycounts.hql -d year=2015 -d month=4 -d day=10 -d date=2015-04-10 -d dsttable=countrycounts
--     Date is duplicated because I haven't figured an easy way to set date=printf()
--
-- set hivevar:year=2015;
-- set hivevar:month=4;
-- set hivevar:day=10;
-- set hivevar:date=2015-04-10;
-- set hivevar:dsttable=countrycounts;
--
-- For dropping partitions, use
--     ALTER TABLE ${dsttable} DROP IF EXISTS PARTITION(date < '${date}')


use yurik;

CREATE TABLE IF NOT EXISTS ${dsttable} (
  via string,
  https string,
  uri_host string,
  country string,
  size bigint,
  count bigint,
  count_pageview bigint
) PARTITIONED BY (
  date string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t';


INSERT OVERWRITE TABLE ${dsttable} PARTITION(date="${date}") IF NOT EXISTS
  SELECT
      via, https, uri_host, country,
      SUM(response_size) size,
      COUNT(1) count,
      SUM(CASE WHEN is_pageview THEN 1 ELSE 0 END) as count_pageview
  FROM (
      SELECT

        COALESCE(regexp_extract(x_analytics, 'proxy=([^\;]+)'), '') via,
        if (x_analytics_map['https']=1, 'https', '') https,
        if (geocoded_data['country_code'] RLIKE '^[A-Z][A-Z]$'
          AND lower(uri_host) RLIKE '^([a-z0-9-]+\\.)*[a-z]*wik[it][a-z]*\\.[a-z]+(:80)?$',
          COALESCE(regexp_extract(lower(uri_host), '^([^:]+)(:80)?$', 1), ''),
          '-') uri_host,
        if (geocoded_data['country_code'] RLIKE '^[A-Z][A-Z]$'
          AND lower(uri_host) RLIKE '^([a-z0-9-]+\\.)*[a-z]*wik[it][a-z]*\\.[a-z]+(:80)?$',
          geocoded_data['country_code'],
          '--') country,
        response_size,
        is_pageview

      FROM wmf.webrequest
      WHERE year=${year} AND month=${month} AND day=${day}

  ) prepared
  GROUP BY via, https, uri_host, country;
