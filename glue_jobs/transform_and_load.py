from pyspark.sql.functions import col, to_date, count, countDistinct, sum, avg, desc, row_number
from pyspark.sql.window import Window

def MyTransform(glueContext, dfc):
    spark = glueContext.spark_session
    
    # Read CSV files
    songs_df = spark.read.csv('/songs/songs.csv', header=True, inferSchema=True)
    streams_df = spark.read.csv('/streams1.csv', header=True, inferSchema=True)
    users_df = spark.read.csv('users.csv', header=True, inferSchema=True)
    
    # Handle missing values in songs_df
    songs_df = songs_df.dropna(subset=['artists', 'album_name', 'track_name'])
    
    # Convert timestamp columns to timestamp type
    streams_df = streams_df.withColumn("listen_time", col("listen_time").cast("timestamp"))
    users_df = users_df.withColumn("created_at", col("created_at").cast("timestamp"))
    
    # Remove duplicate records
    songs_df = songs_df.dropDuplicates()
    streams_df = streams_df.dropDuplicates()
    users_df = users_df.dropDuplicates()
    
    # Join dataframes
    merged_df = streams_df.join(songs_df, on='track_id', how='left')
    merged_df = merged_df.join(users_df, on='user_id', how='left')
    merged_df = merged_df.withColumn('listen_date', to_date('listen_time'))
    
