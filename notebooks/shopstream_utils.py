"""
Utilitaires communs aux notebooks ShopStream (fournis, à ne pas modifier).

    from shopstream_utils import get_spark, JDBC_URL, JDBC_PROPS, KAFKA_BOOTSTRAP, TOPIC, DATA_DIR
"""
import pyspark
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "kafka:9092"          # depuis le conteneur Jupyter
TOPIC = "reviews_stream"
DATA_DIR = "/home/jovyan/work/data"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints"
MODEL_DIR = f"{DATA_DIR}/models"

JDBC_URL = "jdbc:postgresql://postgres:5432/shopstream"
JDBC_PROPS = {"user": "spark", "password": "spark", "driver": "org.postgresql.Driver"}


def _packages():
    """Connecteurs Kafka + driver PostgreSQL, adaptés à la version de Spark de l'image."""
    v = pyspark.__version__
    scala = "2.13" if v.startswith("4") else "2.12"
    return ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{v}",
        "org.postgresql:postgresql:42.7.4",
    ])


def get_spark(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    """Crée (ou récupère) une SparkSession locale configurée pour Kafka et PostgreSQL.
    Le premier appel télécharge les connecteurs (~1 min), les suivants sont instantanés."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", _packages())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} prêt - Spark UI : http://localhost:{spark.sparkContext.uiWebUrl.rsplit(':', 1)[-1]}")
    return spark


def read_table(spark, table: str):
    """Lit une table PostgreSQL en DataFrame."""
    return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)


def write_table(df, table: str, mode: str = "append"):
    """Écrit un DataFrame dans PostgreSQL (mode 'append' ou 'overwrite')."""
    (df.write.mode(mode).option("truncate", "true")
       .jdbc(JDBC_URL, table, properties=JDBC_PROPS))
