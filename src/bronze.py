from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def run_bronze(spark: SparkSession, base_path: str):
    print("Iniciando ingestão Bronze...")
    
    # Lendo o CSV principal tratando "NULL" literal como nulo real
    bookings_df = spark.read.csv(
        f"{base_path}/raw/hotel_bookings.csv",
        header=True,
        inferSchema=True, 
        nullValue="NULL"  
    )
    
    # Salvando Bookings em Parquet particionado por ano 
    bookings_df.write \
        .mode("overwrite") \
        .partitionBy("arrival_date_year") \
        .parquet(f"{base_path}/bronze/bookings/")
        
    # Lendo metadados
    country_df = spark.read.csv(f"{base_path}/raw/country_metadata.csv", header=True, inferSchema=True)
    hotel_df = spark.read.csv(f"{base_path}/raw/hotel_metadata.csv", header=True, inferSchema=True)
    
    # Salvando metadados
    country_df.write.mode("overwrite").parquet(f"{base_path}/bronze/countries/")
    hotel_df.write.mode("overwrite").parquet(f"{base_path}/bronze/hotels/")
    
    print("Camada Bronze finalizada!")
