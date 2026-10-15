import os
import sys
import logging
from pyspark.sql import SparkSession

# Tenta capturar argumentos do AWS Glue; se rodar localmente, usa a pasta local
try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ['BASE_PATH'])
    BASE_PATH = args['BASE_PATH']
except Exception:
    if os.name == 'nt' and "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

from bronze import run_bronze
from silver import run_silver
from gold import run_gold

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info(f"Iniciando Pipeline Medallion com BASE_PATH: {BASE_PATH}")
    
    spark = SparkSession.builder \
        .appName("BeFly_Medallion_Pipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    try:
        run_bronze(spark, BASE_PATH, logger)
        run_silver(spark, BASE_PATH, logger)
        run_gold(spark, BASE_PATH, logger)
        logger.info("Pipeline executado com SUCESSO!")
    except Exception as e:
        logger.error(f"Erro na execução do pipeline: {e}")
    finally:
        spark.stop()