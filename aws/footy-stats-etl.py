from pyspark.sql.functions import explode, col, monotonically_increasing_id, udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from datetime import datetime
from pyspark.sql import SparkSession


def create_spark_session():
    """
    - Create a Spark Session object or update an existing one.
    """
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_daily_match(spark, input_path, output_path):
    """
    - Combine the input JSON files into a single dataframe.
    - Extract columns to create "matches" and "time" dimension tables.
    - Select fields in nested structure and combine separate dataframes
      to create "goals" fact table.
    - These tables are to be appended as new matche data are accumulated
      and historical data are never changed.
    """
    match_data = input_path + 'matches/*.json'
    df = spark.read.json(match_data)
    
    # Create matches dimension table
    match_df = df.select(col('id').alias('match_id'), 'homeID', 'home_name', 'homeGoalCount', 'awayID', 'away_name', 
                         'awayGoalCount', 'competition_id', 'date_unix', 'totalGoalCount', 'stadium_location', 
                         'stadium_name', 'attendance', 'team_a_xg', 'team_b_xg')
                         
    match_df.write.mode("append").format("parquet").save(output_path + "matches/")
    
    # Create timestamp column from original timestamp column
    get_timestamp = udf(lambda z: datetime.fromtimestamp(z).strftime('%Y-%m-%d %H:%M:%S.%f'))
    match_df = match_df.withColumn("ko_ts", get_timestamp(col("date_unix")).cast('timestamp'))
    
    # Extract columns to create time dimension table
    time_table = match_df.select(col("date_unix"), col("ko_ts"),\
                             hour(col("ko_ts")).alias('hour'), \
                        dayofmonth(col("ko_ts")).alias('day'), \
                        weekofyear(col("ko_ts")).alias('week'), \
                        month(col("ko_ts")).alias('month'),
                        year(col("ko_ts")).alias('year'),
                        dayofweek(col("ko_ts")).alias('weekday')
                       )
                       
    time_table = time_table.dropDuplicates(['date_unix'])
    time_table.write.mode("append").format("parquet").save(output_path + "time/")
    
    # Create goals fact table
    # Detailes of goals are nested in struct type and arrarys
    # Use expload function to get it flattened
    df1 = df.select('id', 'competition_id', 'homeID', 'awayID', 'date_unix', 'team_a_goal_details', 'team_b_goal_details')
    df2 = df1.select('id', 'competition_id', 'homeID', 'awayID', 'date_unix', explode(df1.team_a_goal_details).alias('col1'))
    df2 = df2.select('id', 'competition_id', 'homeID', 'awayID', 'date_unix', 'col1.*')
    df3 = df1.select('id', 'competition_id', 'homeID', 'awayID', 'date_unix', explode(df1.team_b_goal_details).alias('col1'))
    df3 = df3.select('id', 'competition_id', 'homeID', 'awayID', 'date_unix', 'col1.*')
    goals_df = df2.union(df3).drop('type').sort('id', 'time').\
        withColumn("goal_id", monotonically_increasing_id()).\
        withColumnRenamed("id","match_id")
        
    goals_df.write.mode("append").format("parquet").save(output_path + "goals/")


def process_teams(spark, input_path, output_path):
    """
    - Combine the input JSON files into a single dataframe.
    - Extract columns to create "teams" dimension tables.
    """
    team_data = input_path + 'teams/*.json'
    df = spark.read.json(team_data)
    
    team_df = df.select(col('id').alias('team_id'), 'name', 'english_name', 'country', 'continent', 
                    'founded').dropDuplicates(['team_id'])
                    
    team_df.write.mode("overwrite").format("parquet").save(output_path + "teams/")


def process_players(spark, input_path, output_path):
    """
    - Combine the input JSON files into a single dataframe.
    - Extract columns to create "players" dimension tables.
    """
    player_data = input_path + 'players/*.json'
    df = spark.read.json(player_data)
    
    player_df = df.select(col('id').alias('player_id'), 'first_name', 'last_name', 'age', 
                      'club_team_id', 'height', 'weight', 'position', 'nationality', 
                      'known_as').dropDuplicates(['player_id'])

    player_df.write.mode("overwrite").format("parquet").save(output_path + "players/")


def main():
    """
    - Create or update a Spark Session.
    - Initialize the input / ouput path to S3 bucket.
    - Call functions to process football datasets.
    """
    spark = create_spark_session()
    input_path = "s3://footystats-capstone/input/"
    output_path = "s3://footystats-capstone/output/"
    
    process_daily_match(spark, input_path, output_path)
    process_teams(spark, input_path, output_path)
    process_players(spark, input_path, output_path)
    

if __name__ == "__main__":
    main()
    
