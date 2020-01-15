-- top 10 most frequently purchased categories
INSERT OVERWRITE DIRECTORY '/top10_categories'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE

SELECT product_category, count(*) as purchases FROM events GROUP BY product_category ORDER BY purchases DESC LIMIT 10;

-- top 10 most frequently purchased product in each category
WITH purchases AS (
    SELECT product_category, product_name, count(*) as purchases FROM events GROUP BY product_category, product_name
)

INSERT OVERWRITE DIRECTORY '/top10_products_within_categories'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE

SELECT product_category,
       product_name,
       purchases,
       rank

FROM (SELECT product_category,
             product_name,
             purchases,
             row_number() OVER (PARTITION BY product_category ORDER BY purchases DESC, product_name) as rank
      FROM purchases
     ) AS numbered

WHERE rank <= 10;

-- top 10 countries with the highest money spending
DROP FUNCTION IF EXISTS is_ip_of_subnet;
CREATE FUNCTION is_ip_of_subnet AS 'com.bastrich.IsIpOfSubnet' USING JAR 'hdfs:///apps/hive-udf.jar';

INSERT OVERWRITE DIRECTORY '/top10_countries'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE

SELECT countries.country_name as country, sum(product_price) as sum_price
FROM events
         JOIN ip_blocks ON is_ip_of_subnet(client_ip_address, network)
         JOIN countries ON ip_blocks.geoname_id = countries.geoname_id
GROUP BY countries.country_name
ORDER BY sum_price DESC
LIMIT 10;