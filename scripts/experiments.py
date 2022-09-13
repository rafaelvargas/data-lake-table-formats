
from pyspark.sql import SparkSession
from pyspark import SparkConf

AWS_JARS=',org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.300'


COLUMNS = {
    'date': 'DATE,',
    'user_id': 'INT,',
    'plan_id':'INT,',
    'software_version_id': 'INT,',
    'platform_id': 'INT,',
    'country_id': 'INT,',
    'duration_in_seconds': 'INT,',
    'number_of_logins': 'INT,',
    'number_of_songs_played': 'INT'
}

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
        column_definitions_string = ""
        for column in COLUMNS.keys():
            column_definitions_string += column + f" {COLUMNS[column]}" + '\n' + "\t\t"
        self._spark_session.sql(
            f"""
            CREATE TABLE {table} (
                {column_definitions_string}
            )
            USING iceberg
            PARTITIONED BY (DATE);
            """
        )
        
        self._spark_session.sql(
            f"INSERT INTO {table} SELECT * FROM parquet.`s3a://datasets/load_{table}.snappy.parquet`"
        )

    def _update_data(self, table: str, percentage_to_update: int = 8):
        self._spark_session.sql(
            f"""
            MERGE INTO {table} t
            USING (SELECT * FROM parquet.`s3a://datasets/update_{percentage_to_update}_{table}.snappy.parquet`) s
                ON t.date = s.date
                AND t.user_id = s.user_id
                AND t.plan_id = s.plan_id
                AND t.software_version_id = s.software_version_id
                AND t.country_id = s.country_id
                AND t.platform_id = s.platform_id
            WHEN MATCHED THEN UPDATE SET 
                t.duration_in_seconds = s.duration_in_seconds,
                t.number_of_logins = s.number_of_logins,
                t.number_of_songs_played = s.number_of_songs_played;
            """
        )

    def _query_data(self, table: str):
        self._spark_session.sql(f"""
            SELECT 
                date, 
                COUNT(DISTINCT user_id) daily_active_users, 
                SUM(duration_in_seconds) duration_sum, 
                SUM(number_of_logins) number_of_logins,
                COUNT(*) number_of_rows 
            FROM {table} 
            GROUP BY 
                date;
        """).show()
        # self._spark_session.sql(f"SELECT * FROM {table} WHERE user_id = 87071 AND date = '2022-08-10'").show()

    def run(self):
        self._load_data(table='fact_daily_usage_by_user')
        self._query_data(table='fact_daily_usage_by_user')
        self._update_data(table='fact_daily_usage_by_user')
        self._query_data(table='fact_daily_usage_by_user')

