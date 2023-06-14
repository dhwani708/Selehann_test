from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_unixtime, current_date
from pyspark.sql.types import StringType, FloatType, DateType
import re
# Create SparkSession
spark = SparkSession.builder.appName("CustomerReviewsAnalysis").getOrCreate()

# Step 1: Create a table with the specified columns
spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_reviews (
        uuid STRING,
        product_id STRING,
        user_id STRING,
        user_name STRING,
        rating FLOAT,
        review_date DATE,
        review_title STRING,
        review_text STRING
    )
    USING DELTA
    PARTITIONED BY (product_id)
    LOCATION 's3://your-bucket/customer_reviews/'
""")
# Step 2: Read the dataset and validate the data
df = spark.read.format("delta").load("s3://t53u259ow3u4329/customer_reviews/")

# Define regex patterns for validation
uuid_pattern = re.compile(r"^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$")
date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# UDF for generating UUID based on product_id, user_id, and review_date
generate_uuid = udf(lambda product_id, user_id, review_date: str(hash(product_id + user_id + str(review_date))))

# UDF for cleaning special characters from strings
clean_string = udf(lambda s: re.sub(r"[^\w\s]", "", s) if s is not None else None)

# UDF for converting improperly formatted dates to standard date format
convert_date = udf(lambda d: None if d is None else d if date_pattern.match(d) else None, DateType())


# Apply data validation and cleaning
df = df.withColumn("uuid", generate_uuid(col("product_id"), col("user_id"), col("review_date"))) \
    .withColumn("user_name", clean_string(col("user_name"))) \
    .withColumn("review_title", clean_string(col("review_title"))) \
    .withColumn("review_text", clean_string(col("review_text"))) \
    .withColumn("review_date", convert_date(col("review_date"))) \
    .filter(col("uuid").rlike(uuid_pattern)) \
    .filter(col("rating").between(1.0, 5.0)) \
    .filter(col("product_id").isNotNull() & col("user_id").isNotNull() & col("user_name").isNotNull() &
            col("rating").isNotNull() & col("review_date").isNotNull() & col("review_title").isNotNull() &
            col("review_text").isNotNull()) \
    .filter(col("user_name") != "") \
    .filter(col("review_title") != "") \
    .filter(col("review_text") != "") \
    .filter(col("review_date").isNotNull()) \
    .filter(col("review_date") <= current_date())

# Step 3: Calculate average rating for each product
average_rating = df.groupBy("product_id").agg({"rating": "avg"})
# Step 4: Find top 10 products with highest average ratings
top_products = average_rating.orderBy(col("avg(rating)").desc()).limit(10)
# Step 5: Identify top three users with most reviews submitted
top_users = df.groupBy("user_id").count().orderBy(col("count").desc()).limit(3)
# Display the results
top_products.show() top_users.show()
# Stop the SparkSession
spark
