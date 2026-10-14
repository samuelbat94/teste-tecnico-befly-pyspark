from pyspark.sql import SparkSession
from bronze import run_bronze
from silver import run_silver
from gold import run_gold
import os

if __name__ == "__main__":
    # Configurando o Spark
    spark = SparkSession.builder \
        .appName("BeFly_Medallion_Pipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        
    # Caminho base relativo a execução do script
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    
    run_bronze(spark, BASE_PATH)
    run_silver(spark, BASE_PATH)
    run_gold(spark, BASE_PATH)
    
    spark.stop()
