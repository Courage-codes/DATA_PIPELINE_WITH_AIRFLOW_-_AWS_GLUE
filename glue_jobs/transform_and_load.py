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
    
    # Daily Genre Listen Count
    daily_genre_listen_count = merged_df.groupBy('listen_date', 'track_genre').agg(count('*').alias('listen_count'))
    
    # Daily Genre Unique Listeners
    daily_genre_unique_listeners = merged_df.groupBy('listen_date', 'track_genre').agg(countDistinct('user_id').alias('unique_listeners'))
    
    # Daily Genre Total Listening Time
    daily_genre_total_listening_time = merged_df.groupBy('listen_date', 'track_genre').agg(sum('duration_ms').alias('total_listening_time_ms'))
    
    # Daily Genre Average Listening Time per User
    daily_genre_user_total_listening_time = merged_df.groupBy('listen_date', 'track_genre', 'user_id').agg(sum('duration_ms').alias('total_listening_time_ms'))
    daily_genre_avg_listening_time_per_user = daily_genre_user_total_listening_time.groupBy('listen_date', 'track_genre').agg(avg('total_listening_time_ms').alias('avg_listening_time_ms_per_user'))
    
    # Daily Genre Top 3 Songs
    window_spec_songs = Window.partitionBy('listen_date', 'track_genre').orderBy(desc('listen_count'))
    daily_genre_top_songs = merged_df.groupBy('listen_date', 'track_genre', 'track_name').agg(count('*').alias('listen_count'))
    daily_genre_top_songs = daily_genre_top_songs.withColumn('row_number', row_number().over(window_spec_songs))
    daily_genre_top_songs = daily_genre_top_songs.filter(col('row_number') <= 3).drop('row_number')
    
    # Daily Top 5 Genres
    window_spec_genres = Window.partitionBy('listen_date').orderBy(desc('listen_count'))
    daily_genre_listen_count_for_top_genres = merged_df.groupBy('listen_date', 'track_genre').agg(count('*').alias('listen_count'))
    daily_top_genres = daily_genre_listen_count_for_top_genres.withColumn('row_number', row_number().over(window_spec_genres))
    daily_top_genres = daily_top_genres.filter(col('row_number') <= 5).drop('row_number')
    
    # Return a dictionary of DataFrames for preview or further processing
    return {
        "daily_genre_listen_count": daily_genre_listen_count,
        "daily_genre_unique_listeners": daily_genre_unique_listeners,
        "daily_genre_total_listening_time": daily_genre_total_listening_time,
        "daily_genre_avg_listening_time_per_user": daily_genre_avg_listening_time_per_user,
        "daily_genre_top_songs": daily_genre_top_songs,
        "daily_top_genres": daily_top_genres
    }
    
