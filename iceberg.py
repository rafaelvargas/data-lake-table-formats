
from pyspark.sql import SparkSession
from pyspark import SparkConf


if __name__ == "__main__":
    # add Iceberg dependency
    ICEBERG_VERSION="0.13.1"
    DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:{}".format(ICEBERG_VERSION)

    # initialize sparkConf
    conf = SparkConf()
    ACCESS_KEY_ID = 'minioadmin'
    SECRET_ACCESS_KEY = 'minioadmin'
    
    # get the endpoint ip address, spark doesn't like docker hostnames
    ENDPOINT = 'http://127.0.0.1:9000'
    
    # add dependencies
    DEPENDENCIES+=",org.apache.hadoop:hadoop-aws:3.3.1"
    DEPENDENCIES+=",com.amazonaws:aws-java-sdk-bundle:1.12.195"

    conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY_ID)
    conf.set("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)
    conf.set("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    conf.set("spark.sql.catalog.spark_catalog.type", "hive")
    conf.set("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
    
    conf.set('spark.jars.packages', DEPENDENCIES)
    conf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://dremio/")
    conf.set("spark.sql.execution.pyarrow.enabled", "true")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark = SparkSession\
        .builder\
        .appName("FirstExample")\
        .config(conf=conf)\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("""
        CREATE OR REPLACE TABLE sample (
            id bigint,
            data string,
            category string
        )
        USING iceberg
        PARTITIONED BY (category)"""
    )

    spark.read.table('sample').show()
    spark.sql("""INSERT INTO sample VALUES (1, 'a', 'orders'), (2, 'b', 'product')""")
    spark.read.table('sample').show()
    
    spark.stop()