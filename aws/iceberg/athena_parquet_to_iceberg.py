# Step 1 - create new db for iceberg
create_db = """
create database iceberg_database;
"""

# Step 2 - create the iceberg table
iceberg_table = """
CREATE TABLE iceberg_database.amazon_reviews_iceberg(
marketplace string,
customer_id string,
review_id string,
product_category string,
product_id string,
product_parent string,
product_title string,
star_rating int,
helpful_votes int,
total_votes int,
vine string,
verified_purchase string,
review_headline string,
review_body string,
review_date bigint)
LOCATION 's3://iceberg-workshop-865670164267/amazon_reviews_iceberg/'
TBLPROPERTIES (
'table_type'='ICEBERG',
'format'='parquet',
'write_target_data_file_size_bytes'='536870912'
)
"""

# Step 3 - Insert some data
insert_sql = """
insert into iceberg_database.amazon_reviews_iceberg
select *
from default.amazon_reviews_parquet
where product_category in ('Gift_Card', 'Apparel','Software')
"""

# Step 4 - Verify
query_sql = """
SELECT * FROM "iceberg_database"."amazon_reviews_iceberg" limit 10;
"""

# Step 5 - Alter table
alter_sql = """
ALTER TABLE iceberg_database.amazon_reviews_iceberg ADD COLUMNS (comment string)

UPDATE iceberg_database.amazon_reviews_iceberg
SET comment = 'High rated'
Where star_rating >=4;

SELECT * FROM iceberg_database.amazon_reviews_iceberg
Where star_rating >=4 limit 10
"""

# Step 5 - Time travel
time_travel = """
SELECT * FROM "iceberg_database"."amazon_reviews_iceberg$history"
"""
# This shows the snapshots, can then query a old one
time_travel_query = """
select * from iceberg_database.amazon_reviews_iceberg FOR VERSION AS OF  <<replace snapshot_id>>
where marketplace ='UK'
"""
# or query by a timestamp
time_travel_query = """
select * from iceberg_database.amazon_reviews_iceberg for TIMESTAMP AS OF TIMESTAMP '2024-08-07 09:15:00' 
where marketplace ='UK'
"""

# Step 6 - Delete and restore using time travel via snapshot version of prior version
delete_sql = """
delete from iceberg_database.amazon_reviews_iceberg
where product_category = 'Software'

SELECT * FROM "iceberg_database"."amazon_reviews_iceberg$history"

insert into iceberg_database.amazon_reviews_iceberg
select * from iceberg_database.amazon_reviews_iceberg FOR VERSION AS OF 1552749759202172302
where product_category = 'Software' limit 10

"""