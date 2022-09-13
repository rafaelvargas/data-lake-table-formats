
from pyspark.sql import SparkSession
from pyspark import SparkConf

AWS_JARS=',org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.300'


class IcebergExperiment:
    def __init__(
        self, 
        iceberg_version: str = '0.14.0', 
        scala_version: str = '2.12',
        scale_in_gb=1
    ):
        self._version = iceberg_version
        self._scale_in_gb = scale_in_gb
        self._dependencies = f"org.apache.iceberg:iceberg-spark-runtime-3.2_{scala_version}:{iceberg_version}"
        self._dependencies += AWS_JARS
        self._confs = [
            ("spark.jars.packages", self._dependencies),
            ("spark.hadoop.fs.s3a.access.key", "minioadmin"),
            ("spark.hadoop.fs.s3a.secret.key", "minioadmin"),
            ("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000"),
            ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
            ("spark.sql.catalog.spark_catalog.type", "hive"),
            ("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083"),
            ("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/"),
            ("spark.sql.execution.pyarrow.enabled", "true"),
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
            ("spark.sql.warehouse.dir", "s3a://iceberg/")
        ]
        self._spark_session = self._create_spark_session()

    def _create_spark_session(self):
        conf = SparkConf()
        for c in self._confs:
            conf.set(c[0], c[1])

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark = SparkSession\
            .builder\
            .appName("IcebergExperiment")\
            .config(conf=conf)\
            .enableHiveSupport()\
            .getOrCreate()
        return spark

    def _load_data(self, table: str):
        self._spark_session.sql(f"DROP TABLE IF EXISTS {table};")
        self._spark_session.sql(
            f"""
            CREATE TABLE {table} (
                id bigint,
                data string,
                category string
            )
            USING iceberg
            PARTITIONED BY (category);
            """
        )
        self._spark_session.sql(
            f"INSERT INTO {table} VALUES (1, 'a', 'c1'), (2, 'b', 'c1'), (3, 'c', 'c2');"
        )

    def _query_data(self, table: str):
        print(f"SELECT SUM(id) FROM {table};")
        self._spark_session.sql(f"SELECT SUM(id) FROM {table};").show()

    def run(self):
        self._load_data(table='iceberg_experiment_test')
        self._query_data(table='iceberg_experiment_test')
