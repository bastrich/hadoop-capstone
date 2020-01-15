CREATE SCHEMA IF NOT EXISTS hive;
CREATE TABLE hive.top10_categories
(
    product_category varchar,
    purchases bigint
);
CREATE TABLE hive.top10_products_within_categories
(
    product_category varchar,
    product_name  varchar,
    purchases  bigint,
    rank  int
);
CREATE TABLE hive.top10_countries
(
    country varchar,
    sum_price bigint
);

CREATE SCHEMA IF NOT EXISTS spark_rdd;
CREATE TABLE spark_rdd.top10_categories
(
    product_category varchar,
    purchases bigint
);
CREATE TABLE spark_rdd.top10_products_within_categories
(
    product_category varchar,
    product_name  varchar,
    purchases  bigint,
    rank  int
);
CREATE TABLE spark_rdd.top10_countries
(
    country varchar,
    sum_price bigint
);

CREATE SCHEMA IF NOT EXISTS spark_datasets;
-- CREATE TABLE spark_datasets.top10_categories
-- (
--     product_category varchar,
--     purchases bigint
-- );
-- CREATE TABLE spark_datasets.top10_products_within_categories
-- (
--     product_category varchar,
--     product_name  varchar,
--     purchases  bigint,
--     rank  int
-- );
-- CREATE TABLE spark_datasets.top10_countries
-- (
--     country varchar,
--     sum_price bigint
-- );