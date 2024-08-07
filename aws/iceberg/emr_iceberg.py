# Create cluster with the Iceberg classification:
# [{ "Classification":"iceberg-defaults",
#     "Properties":{"iceberg.enabled":"true"}
# }]
# or include /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar in the smr jars dir

# Config for Spark
spark_config = """
{
"conf":{
         "spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
         "spark.sql.catalog.glue_catalog":"org.apache.iceberg.spark.SparkCatalog",
         "spark.sql.catalog.glue_catalog.catalog-impl":"org.apache.iceberg.aws.glue.GlueCatalog",
         "spark.sql.catalog.glue_catalog.warehouse":"s3://iceberg-workshop-865670164267/glue_catalog/",
         "spark.sql.catalog.glue_catalog.io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
         "spark.sql.catalog.glue_catalog.lock-impl":"org.apache.iceberg.aws.glue.DynamoLockManager",
         "spark.sql.catalog.glue_catalog.lock.table":"myGlueLockTable"
        }
}
"""

spark.sql("use glue_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS products")
spark.sql("use products")

spark.sql(""" DROP TABLE if exists glue_catalog.products.amazonreviews""")

spark.sql(""" CREATE TABLE glue_catalog.products.amazonreviews 
(marketplace	string
,customer_id	string
,review_id	string
,product_category	string
,product_id	string
,product_parent	string
,product_title	string
,star_rating	int
,helpful_votes	int
,total_votes	int
,vine	string
,verified_purchase	string
,review_headline	string
,review_body	string
,review_date	bigint)

USING iceberg 
""")

# test the tables created in glue catalog.
spark.sql("show tables").show()

# read
df = spark.read.parquet(
    "s3://iceberg-workshop-865670164267/productreviews/simulatedproductreviews.parquet"
)

df.sortWithinPartitions("review_date").writeTo(
    "glue_catalog.products.amazonreviews").append()

%%sql
select * from glue_catalog.products.amazonreviews limit 9

# delete

spark.sql("""delete from glue_catalog.products.amazonreviews
where verified_purchase = 'N'""")

spark.sql(
    """select * from glue_catalog.products.amazonreviews where verified_purchase = 'N'limit 9"""
).show()

# updates

spark.sql("""UPDATE glue_catalog.products.amazonreviews
SET marketplace = 'USA'
WHERE marketplace = 'US'""")

%%sql
select * from glue_catalog.products.amazonreviews limit 5



# Schema evolution

spark.sql(
    """ALTER TABLE glue_catalog.products.amazonreviews ADD COLUMNS (high_rated_product string comment 'Highly rated comment')"""
)

spark.sql(
    """UPDATE glue_catalog.products.amazonreviews SET high_rated_product = 'High rated' where star_rating >=4"""
)

%%sql
Select customer_id,review_id,product_id, product_title, star_rating, high_rated_product from glue_catalog.products.amazonreviews limit 9

# Drop column

spark.sql(
    """ALTER TABLE glue_catalog.products.amazonreviews DROP COLUMN high_rated_product"""
)
# Time travel

spark.sql("SELECT * FROM glue_catalog.products.amazonreviews.history").show()

spark.sql("SELECT * FROM glue_catalog.products.amazonreviews.snapshots").show()

spark.sql(
    "CALL glue_catalog.system.rollback_to_snapshot('products.amazonreviews', xxxxxxxxxxxxx)"
)

# Partition evolution

spark.sql("select * from glue_catalog.products.amazonreviews.partitions").show()

%%sh
aws s3 ls s3://iceberg-workshop-865670164267/glue_catalog/products.db/amazonreviews/data/

%%sql
ALTER TABLE glue_catalog.products.amazonreviews ADD COLUMNS (review_dt date);

%%sql
UPDATE glue_catalog.products.amazonreviews set review_dt = date_add(to_date('1970-01-01'),cast(review_date as integer));

%%sql
ALTER TABLE glue_catalog.products.amazonreviews add PARTITION FIELD years(review_dt)

%%sh
aws s3 ls s3://iceberg-workshop-865670164267/glue_catalog/products.db/amazonreviews/data/

%%sql

INSERT INTO glue_catalog.products.amazonreviews
SELECT * FROM glue_catalog.products.amazonreviews WHERE year(review_dt)=1998
union
SELECT * FROM glue_catalog.products.amazonreviews WHERE year(review_dt)=2015

%%sh
aws s3 ls s3://iceberg-workshop-865670164267/glue_catalog/products.db/amazonreviews/data/

%%sql
ALTER TABLE glue_catalog.products.amazonreviews ADD PARTITION FIELD months(review_dt)

%%sql
INSERT INTO glue_catalog.products.amazonreviews
SELECT * FROM glue_catalog.products.amazonreviews WHERE year(review_dt)=1998 and month(review_dt)=9
union
SELECT * FROM glue_catalog.products.amazonreviews WHERE year(review_dt)=2000 and month(review_dt)=9


%%sh
echo "Top level ------------------------------------------------------------------------------------"
aws s3 ls s3://MYBUCKET/glue_catalog/products.db/amazonreviews/data/
echo ""
echo "inside of review_dt_year=1998 directory-------------------------------------------------------"
aws s3 ls s3://MYBUCKET/glue_catalog/products.db/amazonreviews/data/review_dt_year=1998/
echo ""
echo "inside of review_dt_year=2000 directory-------------------------------------------------------"
aws s3 ls s3://MYBUCKET/glue_catalog/products.db/amazonreviews/data/review_dt_year=2000/

%%sql

SELECT marketplace, product_title, review_dt FROM glue_catalog.products.amazonreviews WHERE year(review_dt)=1998 and month(review_dt)=9 limit 5

%%sql
    select marketplace, customer_id, product_category, product_title, star_rating, verified_purchase, review_headline, review_dt
    from glue_catalog.products.amazonreviews where year(review_dt) = 1999 limit 10
