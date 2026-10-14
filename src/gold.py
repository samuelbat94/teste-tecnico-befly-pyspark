from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round, when

def run_gold(spark: SparkSession, base_path: str):
    print("Iniciando agregações Gold...")
    
    silver_df = spark.read.parquet(f"{base_path}/silver/bookings_enriched/")
    
    # Visão 1: Receita por Hotel e Mês
    revenue_hotel_month = silver_df.groupBy("hotel", "arrival_date_year", "month_num") \
        .agg(
            count("*").alias("total_bookings"),
            sum(when(col("is_canceled") == 0, 1).otherwise(0)).alias("effective_bookings"),
            sum("is_canceled").alias("cancelled_bookings"),
            round(sum("revenue"), 2).alias("total_revenue"),
            round(avg(when(col("is_canceled") == 0, col("adr"))), 2).alias("avg_adr")
        ) \
        .withColumn("cancellation_rate", round(col("cancelled_bookings") / col("total_bookings"), 4))
        
    revenue_hotel_month.write.mode("overwrite").parquet(f"{base_path}/gold/revenue_by_hotel_month/")
    
    # Visão 2: Top Países por Receita
    top_countries = silver_df.filter(col("is_canceled") == 0) \
        .groupBy("country", "country_name", "continent") \
        .agg(
            count("*").alias("effective_bookings"),
            round(sum("revenue"), 2).alias("total_revenue")
        ).orderBy(col("total_revenue").desc()).limit(20)
        
    top_countries.write.mode("overwrite").parquet(f"{base_path}/gold/top_countries_by_revenue/")
    
    print("Camada Gold finalizada!")
