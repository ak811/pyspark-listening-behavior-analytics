#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Music Listener Behaviour Analysis with Spark Structured APIs

Tasks:
1) User favourite genre
2) Average listen time per song
3) Genre loyalty score (> 0.8)
4) Night owl users (listening between 00:00 and 05:00 frequently)

Run:
  spark-submit main.py --input_dir inputs --output_dir outputs
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, sum as spark_sum, row_number, to_timestamp, hour, when, lit
)
from pyspark.sql.window import Window

def build_spark():
    return (
        SparkSession.builder
        .appName("MusicAnalysis")
        # Helpful configs for CSV handling
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def load_data(spark: SparkSession, input_dir: str):
    logs = (
        spark.read
        .option("header", True)
        .csv(f"{input_dir}/listening_logs.csv")
        .withColumn("duration_sec", col("duration_sec").cast("int"))
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    )

    meta = (
        spark.read
        .option("header", True)
        .csv(f"{input_dir}/songs_metadata.csv")
    )

    return logs, meta

def task1_user_favourite_genre(joined_df, out_dir: str):
    plays_per_genre = (
        joined_df.groupBy("user_id", "genre")
        .agg(count(lit(1)).alias("plays"))
    )

    w = Window.partitionBy("user_id").orderBy(col("plays").desc(), col("genre").asc())
    fav = (
        plays_per_genre
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn")
        .orderBy("user_id")
    )

    (
        fav.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/user_favorite_genres")
    )
    return fav

def task2_avg_listen_time_per_song(joined_df, out_dir: str):
    avg_time = (
        joined_df.groupBy("song_id", "title", "artist", "genre")
        .agg(avg("duration_sec").alias("avg_listen_time_sec"))
        .orderBy(col("avg_listen_time_sec").desc())
    )

    (
        avg_time.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/avg_listen_time_per_song")
    )
    return avg_time

def task3_genre_loyalty(joined_df, fav_genre_df, out_dir: str):
    user_total = joined_df.groupBy("user_id").agg(count(lit(1)).alias("total_plays"))
    user_genre = joined_df.groupBy("user_id", "genre").agg(count(lit(1)).alias("plays"))

    # favourite plays per user
    fav_plays = (
        user_genre.join(fav_genre_df.select("user_id", col("genre").alias("fav_genre")), on="user_id", how="inner")
        .where(col("genre") == col("fav_genre"))
        .select("user_id", col("plays").alias("fav_genre_plays"), "fav_genre")
    )

    loyalty = (
        fav_plays.join(user_total, on="user_id", how="inner")
        .withColumn("loyalty_score", col("fav_genre_plays") / col("total_plays"))
        .orderBy(col("loyalty_score").desc())
    )

    # Filter loyalty > 0.8 as per requirement
    loyal_users = loyalty.where(col("loyalty_score") > lit(0.8))

    (
        loyalty.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/genre_loyalty_scores")
    )
    (
        loyal_users.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/genre_loyalty_scores/above_0_8")
    )

    return loyalty, loyal_users

def task4_night_owls(logs_df, out_dir: str, min_night_plays: int = 5, min_ratio: float = 0.3):
    # Plays between 00:00 (inclusive) and 05:00 (exclusive)
    night = logs_df.withColumn("hour", hour(col("timestamp"))) \
                   .where((col("hour") >= 0) & (col("hour") < 5))

    night_counts = night.groupBy("user_id").agg(count(lit(1)).alias("night_plays"))
    total_counts = logs_df.groupBy("user_id").agg(count(lit(1)).alias("total_plays"))

    night_stats = (
        night_counts.join(total_counts, on="user_id", how="inner")
        .withColumn("night_ratio", col("night_plays") / col("total_plays"))
        .orderBy(col("night_ratio").desc(), col("night_plays").desc())
    )

    # "Frequently" defined as: at least `min_night_plays` AND >= `min_ratio` of total plays
    night_owls = night_stats.where((col("night_plays") >= lit(min_night_plays)) & (col("night_ratio") >= lit(min_ratio)))

    (
        night_stats.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/night_owl_users/all_user_night_stats")
    )
    (
        night_owls.coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{out_dir}/night_owl_users/frequent")
    )

    return night_stats, night_owls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="inputs")
    parser.add_argument("--output_dir", default="outputs")
    args = parser.parse_args()

    spark = build_spark()
    logs_df, meta_df = load_data(spark, args.input_dir)

    # Join metadata for genre/title/artist fields
    joined = logs_df.join(meta_df, on="song_id", how="left")

    # Tasks
    fav_df = task1_user_favourite_genre(joined, args.output_dir)
    avg_df = task2_avg_listen_time_per_song(joined, args.output_dir)
    loyalty_df, loyal_over_0_8 = task3_genre_loyalty(joined, fav_df, args.output_dir)
    night_stats, night_owls = task4_night_owls(logs_df, args.output_dir)

    print("Done. Outputs written to:", args.output_dir)
    spark.stop()

if __name__ == "__main__":
    main()
