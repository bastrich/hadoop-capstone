-- events
CREATE EXTERNAL TABLE IF NOT EXISTS events
(
    product_name      string,
    product_price     int,
    purchase_time     string,
    product_category  string,
    client_ip_address string
)
    PARTITIONED BY (purchase_date string)
    ROW FORMAT
        DELIMITED FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/events';


-- subnet_countries
DROP TABLE IF EXISTS tmp_ip_blocks;
DROP TABLE IF EXISTS tmp_countries;

CREATE EXTERNAL TABLE IF NOT EXISTS tmp_ip_blocks
(
    network                        string,
    geoname_id                     bigint,
    registered_country_geoname_id  bigint,
    represented_country_geoname_id bigint,
    is_anonymous_proxy             int,
    is_satellite_provider          int
)
    ROW FORMAT
        DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/geo_data/ip_blocks'
    TBLPROPERTIES ("skip.header.line.count" = "1");

CREATE EXTERNAL TABLE tmp_countries
(
    geoname_id           bigint,
    locale_code          string,
    continent_code       string,
    continent_name       string,
    country_iso_code     string,
    country_name         string,
    is_in_european_union int
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            "separatorChar" = ",",
            "quoteChar"     = "\""
        )
    STORED AS TEXTFILE
    LOCATION '/geo_data/countries'
    TBLPROPERTIES ("skip.header.line.count" = "1");

CREATE TABLE IF NOT EXISTS ip_blocks
(
    network                        string,
    geoname_id                     bigint,
    registered_country_geoname_id  bigint,
    represented_country_geoname_id bigint,
    is_anonymous_proxy             int,
    is_satellite_provider          int
)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS countries
(
    geoname_id           bigint,
    locale_code          string,
    continent_code       string,
    continent_name       string,
    country_iso_code     string,
    country_name         string,
    is_in_european_union int
)
    STORED AS PARQUET;

INSERT INTO TABLE countries SELECT * FROM tmp_countries;
INSERT INTO TABLE ip_blocks SELECT * FROM tmp_ip_blocks;

DROP TABLE IF EXISTS tmp_ip_blocks;
DROP TABLE IF EXISTS tmp_countries;
