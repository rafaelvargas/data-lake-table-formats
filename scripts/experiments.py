
from pyspark.sql import SparkSession
from pyspark import SparkConf
import tables

AWS_JARS=',org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.300'

class Experiment:
    def __init__(self, table_format: str, confs: list):
        self._table_format = table_format
        self._confs = confs
        self._spark_session = self._create_spark_session()
    
    def _create_spark_session(self):
        conf = SparkConf()
        for c in self._confs:
            conf.set(c[0], c[1])
        spark = SparkSession\
            .builder\
            .appName(f"{self._table_format}_experiment")\
            .config(conf=conf)\
            .enableHiveSupport()\
            .getOrCreate()
        return spark
    
    def _load_data(self, table: str):
        partition_columns = tables.DEFINITIONS[table]['partition_columns']
        primary_key = tables.DEFINITIONS[table]['primary_key'] 

        partition_string = f"PARTITIONED BY ({partition_columns})" if partition_columns else ""
        options_string = {
            "hudi": f"""
                OPTIONS (
                    type = 'cow', 
                    primaryKey = '{primary_key}',
                    precombineField = '',
                    'hoodie.datasource.write.hive_style_partitioning' = 'true',
                    'hoodie.parquet.compression.codec' = 'snappy',
                    'hoodie.populate.meta.fields' = 'false'
                )
            """,
            "iceberg": """
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'snappy',
                    'write.merge.mode' = 'copy-on-write',
                    'write.spark.fanout.enabled'= true
                )
            """,
            "delta": ""
        }    
            
        self._spark_session.sql(f"DROP TABLE IF EXISTS {self._table_format}_{table};")
        self._spark_session.sql(
            f"""
            CREATE TABLE {self._table_format}_{table} 
            USING {self._table_format}
            { partition_string }
            { options_string[self._table_format] }
            LOCATION 's3a://{self._table_format}/{self._table_format}_{table}/'
            SELECT * FROM `parquet`.`s3a://datasets/load_{table}.snappy.parquet`;
            """
        )

    def _update_data(self, table: str, percentage_to_update: int = 8):
        self._spark_session.sql(
            f"""
            MERGE INTO {self._table_format}_{table} t
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
            FROM {self._table_format}_{table}
            GROUP BY 
                date;
        """).show()
        self._spark_session.sql(f"""
            SELECT 
                c.name country, 
                COUNT(DISTINCT user_id) number_of_active_users
            FROM {self._table_format}_{table} t
            INNER JOIN {self._table_format}_dim_country c
                on c.id = t.country_id
            GROUP BY 
                c.name
            ORDER BY number_of_active_users DESC
            LIMIT 1;
        """).show()

    def run(self):
        for table_name in tables.DEFINITIONS.keys():
            self._load_data(table=table_name)
        self._query_data(table='fact_daily_usage_by_user')
        self._update_data(table='fact_daily_usage_by_user')
        self._query_data(table='fact_daily_usage_by_user')

class IcebergExperiment(Experiment):
    def __init__(
        self, 
        iceberg_version: str = '0.14.0', 
        scala_version: str = '2.12',
        scale_in_gb=1
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"org.apache.iceberg:iceberg-spark-runtime-3.2_{scala_version}:{iceberg_version}"
        dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ("spark.hadoop.fs.s3a.access.key", "minioadmin"),
            ("spark.hadoop.fs.s3a.secret.key", "minioadmin"),
            ("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000"),
            ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
            ("spark.sql.catalog.spark_catalog.type", "hive"),
            ("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083"),
            ("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/"),
            ("spark.sql.execution.pyarrow.enabled", "true"),
            ("spark.sql.warehouse.dir", "s3a://iceberg/"),
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        ]
        super().__init__(table_format='iceberg', confs=confs)

class DeltaExperiment(Experiment):
    def __init__(
        self, 
        delta_version: str = '2.0.0', 
        scala_version: str = '2.12',
        scale_in_gb=1
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"io.delta:delta-core_{scala_version}:{delta_version},io.delta:delta-contribs_{scala_version}:{delta_version},io.delta:delta-hive_{scala_version}:0.2.0"
        dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ('spark.hadoop.fs.s3a.access.key', 'minioadmin'),
            ('spark.hadoop.fs.s3a.secret.key', 'minioadmin'),
            ('spark.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000'),
            ('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false'),
            ('spark.sql.catalogImplementation', 'hive'),
            ('spark.sql.catalog.spark_catalog.type', 'hive'),
            ('spark.sql.catalog.spark_catalog.uri', 'thrift://localhost:9083'),
            ('spark.sql.catalog.spark_catalog.warehouse', 's3a://delta/'),
            ('spark.sql.warehouse.dir', 's3a://delta/'),
            ('spark.sql.execution.pyarrow.enabled', 'true'),
            ('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
            ('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        ]
        super().__init__(table_format='delta', confs=confs)

class HudiExperiment(Experiment):
    def __init__(
        self, 
        hudi_version: str = '0.11.1', 
        scala_version: str = '2.12',
        scale_in_gb=1
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"org.apache.hudi:hudi-spark3.2-bundle_{scala_version}:{hudi_version}"
        dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ('spark.hadoop.fs.s3a.access.key', 'minioadmin'),
            ('spark.hadoop.fs.s3a.secret.key', 'minioadmin'),
            ('spark.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000'),
            ('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false'),
            ('spark.sql.catalog.spark_catalog.type', 'hive'),
            ('spark.sql.catalog.spark_catalog.uri', 'thrift://localhost:9083'),
            ('spark.sql.catalog.spark_catalog.warehouse', 's3a://hudi/'),
            ('spark.sql.execution.pyarrow.enabled', 'true'),
            ('spark.sql.warehouse.dir', 's3a://hudi/'),
            ('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension'),
            ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        ]
        super().__init__(table_format='hudi', confs=confs)



