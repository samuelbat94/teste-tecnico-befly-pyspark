from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, lpad, to_date, concat_ws

def run_silver(spark: SparkSession, base_path: str):
    print("Iniciando processamento Silver...")
    
    bookings = spark.read.parquet(f"{base_path}/bronze/bookings/")
    countries = spark.read.parquet(f"{base_path}/bronze/countries/")
    hotels = spark.read.parquet(f"{base_path}/bronze/hotels/")
    
    # 1. Mapeamento de meses para criar a data
    month_map = expr("""
        case arrival_date_month
            when 'January' then '01' when 'February' then '02' when 'March' then '03'
            when 'April' then '04' when 'May' then '05' when 'June' then '06'
            when 'July' then '07' when 'August' then '08' when 'September' then '09'
            when 'October' then '10' when 'November' then '11' when 'December' then '12'
        end
    """)
    
    # 2. Transformações, Limpezas e Colunas Derivadas
    
    # Contagem antes dos filtros
    count_before = bookings.count()
    
    silver_bookings = bookings \
        .withColumn("month_num", month_map) \
        .withColumn("day_str", lpad(col("arrival_date_day_of_month").cast("string"), 2, "0")) \
        .withColumn("arrival_date", to_date(concat_ws("-", col("arrival_date_year"), col("month_num"), col("day_str")))) \
        .fillna(0, subset=["children", "babies"]) \
        .fillna("UNK", subset=["country"]) \
        .filter((col("adults") + col("children") + col("babies")) > 0) \
        .filter(col("adr") >= 0)
        
    count_after = silver_bookings.count()
    print(f"Registros removidos por regras de negócio (sem hóspedes ou adr negativo): {count_before - count_after}")
        
    silver_bookings = silver_bookings \
        .withColumn("total_nights", col("stays_in_weekend_nights") + col("stays_in_week_nights")) \
        .withColumn("total_guests", col("adults") + col("children") + col("babies")) \
        .withColumn("is_family", when((col("children") > 0) | (col("babies") > 0), 1).otherwise(0)) \
        .withColumn("revenue", when(col("is_canceled") == 0, col("adr") * col("total_nights")).otherwise(0))
        
    # 3. Enriquecimento (Joins)
    silver_enriched = silver_bookings.alias("b") \
        .join(countries.alias("c"), col("b.country") == col("c.country_code"), "left") \
        .join(hotels.alias("h"), col("b.hotel") == col("h.hotel_name"), "left") \
        .select(
            "b.*",
            col("c.country_name"), col("c.continent"),
            col("h.city").alias("hotel_city"), 
            col("h.star_rating").alias("hotel_star_rating")
        )
        
    # 4. Salvando particionado por ano e mes
    silver_enriched.write \
        .mode("overwrite") \
        .partitionBy("arrival_date_year", "month_num") \
        .parquet(f"{base_path}/silver/bookings_enriched/")
        
    print("Camada Silver finalizada!")
