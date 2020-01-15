CREATE OR REPLACE FUNCTION has_the_same_content(t1_name VARCHAR, t1_columns VARCHAR[],
                                 t2_name VARCHAR, t2_columns VARCHAR[])
    RETURNS BOOLEAN AS
$$
DECLARE
    c1 INT;
c2 INT;
i INT := 1;
columns_size1 INT := array_length(t1_columns, 1);
columns_size2 INT := array_length(t2_columns, 1);
join_condition VARCHAR := '';
joined_count INT;
query VARCHAR;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || t1_name INTO c1;
EXECUTE 'SELECT COUNT(*) FROM ' || t2_name INTO c2;

IF (c1 != c2) THEN
        RAISE NOTICE 'TABLES SHOULD HAVE THE SAME SIZE';
RETURN FALSE;
END IF;

IF (columns_size1 != columns_size2) THEN
        RAISE NOTICE 'TABLES SHOULD HAVE THE SAME COLUMNS';
RETURN FALSE;
END IF;

i := 1;
LOOP
        EXIT WHEN i > columns_size1;
join_condition := join_condition || ' t1.' || t1_columns[i] || '=t2.' || t2_columns[i];
IF (i != columns_size1) THEN
            join_condition := join_condition || ' AND';
END IF;
i := i + 1;
END LOOP;

query := 'SELECT COUNT(*) FROM
                         (SELECT DISTINCT ' || array_to_string(t1_columns, ',') || ' FROM ' || t1_name || ') as t1
    INNER JOIN (SELECT DISTINCT ' || array_to_string(t2_columns, ',')  || ' FROM ' || t2_name || ') as t2
    ON' || join_condition;

RAISE NOTICE '%', query;

EXECUTE query INTO joined_count;

IF (joined_count != c1) THEN
        RAISE NOTICE 'TABLES CONTENT SHOULD BE THE SAME';
RETURN FALSE;
END IF;

RETURN TRUE;
END;
$$
LANGUAGE PLPGSQL;

SELECT has_the_same_content('hive.top10_categories', '{product_category, purchases}', 'spark_rdd.top10_categories', '{product_category, purchases}'), has_the_same_content('spark_rdd.top10_categories', '{product_category, purchases}', 'spark_datasets.top10_categories', '{"\"productCategory\"", purchases}');
SELECT has_the_same_content('hive.top10_products_within_categories', '{product_category, product_name, purchases, rank}','spark_rdd.top10_products_within_categories', '{product_category, product_name, purchases, rank}'), has_the_same_content('spark_rdd.top10_products_within_categories', '{product_category, product_name, purchases, rank}', 'spark_datasets.top10_products_within_categories', '{"\"productCategory\"", "\"productName\"", purchases, rank}');
SELECT has_the_same_content('hive.top10_countries', '{country, sum_price}', 'spark_rdd.top10_countries', '{country, sum_price}'),has_the_same_content('spark_rdd.top10_countries', '{country, sum_price}', 'spark_datasets.top10_countries', '{"\"countryName\"", price}');