from pyspark.sql.functions import *
from pyspark.sql.types import *

# what if new column comes
def  Add_newField(df):
    df = df.withColumn(
        "new_field",
        get_json_object(col("decoded"), "$.new_field")
    )
    return df

# type cast times and extract day ,hour,and week
def Parse_Time(df):
    df = df \
        .withColumn("event_timestamp", to_timestamp("timestamp")) \
        .withColumn("event_date", to_date("event_timestamp")) \
        .withColumn("event_hour", hour("event_timestamp")) \
        .withColumn("event_week", weekofyear("event_timestamp"))
    return df

# filter out bad data like null user_id
def Filtering(df):
    df = df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("event_type").isin("search", "click", "cart", "checkout", "purchase"))
    return df

# remove duplicate event_ids
def Remove_Duplicate(df):
    df = df.dropDuplicates(["event_id"])
    return df