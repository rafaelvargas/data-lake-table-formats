
from datetime import datetime
import time
from unittest import result

from pyspark.sql import SparkSession
from pyspark import SparkConf

import tables
import csv

ENV = 'prod'
AWS_JARS=',org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.300'
CSV_HEADER = ["operation", "elapsed_time_in_seconds"]


class Experiment:
    def __init__(self, table_format: str, scale_in_gb: int, confs: list, path: str, experiment_id: str):
        self._table_format = table_format
        self._confs = confs
        if ENV == 'dev':
            self._confs += [
                ('spark.hadoop.fs.s3a.access.key', 'minioadmin'),
                ('spark.hadoop.fs.s3a.secret.key', 'minioadmin'),
                ('spark.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000'),
                ('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false'),
                ('spark.sql.catalogImplementation', 'hive'),
                ('hive.metastore.uris', 'thrift://localhost:9083'), 
                ('spark.sql.warehouse.dir', f'{path}/databases/'),
                ('spark.sql.execution.pyarrow.enabled', 'true')
            ]
        self._spark_session = self._create_spark_session()
        self._scale_in_gb = scale_in_gb
        self._database_name = f"{scale_in_gb}gb_{self._table_format}" 
        if not experiment_id:
            now = datetime.now()
            experiment_id = now.strftime("%Y%m%d_%H%M%S") + "_" + self._database_name
        self._experiment_id = experiment_id
        self._database_path = f"{path}/databases/{self._experiment_id}"
        self._path = path
        self._execution_times = []
    
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
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _load_data(self, table: str):
        partition_columns = tables.DEFINITIONS[table]['partition_columns']
        primary_key = tables.DEFINITIONS[table]['primary_key'] 
        target_location =  f"{self._database_path}/{table}/"

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
                    'write.merge.mode' = 'copy-on-write'
                )
            """,
            "delta": ""
        }    
        
        self._run_sql(f"DROP TABLE IF EXISTS `{self._database_name}`.`{table}`;", f"drop-table-{table}")
        self._run_sql(f"""
            CREATE EXTERNAL TABLE `{self._database_name}`.`{table}` 
            USING {self._table_format}
            { partition_string }
            { options_string[self._table_format] }
            LOCATION '{target_location}'
            SELECT * FROM `parquet`.`{self._path}/datasets/{self._scale_in_gb}gb/load_{table}`;
        """, f"create-table-{table}")

    def _update_data(self, table: str = 'fact_daily_usage_by_user', percentages_to_update: list = [8, 16, 32]):
        for pencentage_to_update in percentages_to_update:
            self._run_sql(f"""
                MERGE INTO `{self._database_name}`.`{table}` t
                USING (SELECT * FROM parquet.`{self._path}/datasets/{self._scale_in_gb}gb/update_{pencentage_to_update}_{table}`) s
                    ON t.date = s.date
                    AND t.user_id = s.user_id
                    AND t.plan_id = s.plan_id
                    AND t.software_version_id = s.software_version_id
                    AND t.country_id = s.country_id
                    AND t.platform_id = s.platform_id
                WHEN MATCHED THEN UPDATE SET
                    t.duration_in_seconds = s.duration_in_seconds,
                    t.number_of_sessions = s.number_of_sessions,
                    t.number_of_songs_played = s.number_of_songs_played;
            """, f"update-table-{table}-{pencentage_to_update}")

    def _run_sql(self, sql: str, operation: str):
        self._spark_session.sparkContext.setJobGroup(operation, operation, interruptOnCancel=True)
        start = time.time()
        df = self._spark_session.sql(sql)
        _ = df.collect()
        end = time.time()
        if ENV == 'dev':
            df.show()
        self._execution_times.append([operation, end - start])
        print(f"Execution time: {end - start} ({operation})")
    
    def _query_data(self, table: str = 'fact_daily_usage_by_user'):
        self._run_sql(f"""
            SELECT 
                date, 
                COUNT(DISTINCT user_id) number_of_active_users
            FROM `{self._database_name}`.`{table}`
            GROUP BY 
                date
            ORDER BY date ASC;
        """, f"q1")
        self._run_sql(f"""
            SELECT 
                COUNT(DISTINCT user_id) number_of_active_users
            FROM `{self._database_name}`.`{table}`;
        """, f"q2")
        self._run_sql(f"""
            SELECT 
                plan_name, 
                AVG(number_of_active_users)
            FROM (
                SELECT 
                    date, 
                    p.name plan_name,
                    COUNT(DISTINCT user_id) number_of_active_users
                FROM `{self._database_name}`.`{table}` f
                INNER JOIN `{self._database_name}`.`dim_plan` p
                    ON f.plan_id = p.id
                GROUP BY 
                    date,
                    p.name
            ) t
            GROUP BY
                plan_name
        """, f"q3")
        self._run_sql(f"""
            SELECT 
                v.version version, 
                SUM(number_of_sessions) number_of_sessions
            FROM `{self._database_name}`.`{table}` f
            INNER JOIN `{self._database_name}`.`dim_software_version` v
                on v.id = f.software_version_id
            GROUP BY 
                v.version
            ORDER BY number_of_sessions DESC
            LIMIT 1;
        """, f"q4")
        self._run_sql(f"""
            SELECT 
                c.name country, 
                COUNT(DISTINCT user_id) number_of_active_users
            FROM `{self._database_name}`.`{table}` f
            INNER JOIN `{self._database_name}`.`dim_country` c
                on c.id = f.country_id
            GROUP BY 
                c.name
            ORDER BY number_of_active_users DESC
            LIMIT 5;
        """, f"q5")
        self._run_sql(f"""
            WITH usage_ranking AS (
                SELECT 
                    plan.name plan,
                    platform.name platform,
                    ROW_NUMBER() OVER (PARTITION BY plan_id ORDER BY number_of_active_users DESC) rn
                FROM (
                    SELECT 
                        plan_id,
                        platform_id,
                        COUNT(DISTINCT user_id) number_of_active_users 
                    FROM `{self._database_name}`.`{table}` f
                    GROUP BY
                        f.plan_id,
                        f.platform_id
                ) t
                INNER JOIN `{self._database_name}`.`dim_plan` plan
                    ON plan.id = t.plan_id
                INNER JOIN `{self._database_name}`.`dim_platform` platform
                    ON platform.id = t.platform_id
            )
            SELECT
                plan,
                platform
            FROM usage_ranking
            WHERE rn = 1;
        """, f"q6")

    def _load_tables(self):
        self._run_sql(f"DROP DATABASE IF EXISTS {self._database_name} CASCADE;", f"drop-database-{self._database_name}")
        self._run_sql(f"CREATE DATABASE IF NOT EXISTS {self._database_name} LOCATION '{self._database_path}';", f"create-database-{self._database_name}")
        for table_name in tables.DEFINITIONS.keys():
            self._load_data(table=table_name)
    
    def _dump_results_to_csv(self, operation: str):
        with open(f"{self._experiment_id}_{operation}_results.csv", "w") as results_file:
            writer = csv.writer(results_file)
            writer.writerow(CSV_HEADER)
            writer.writerows(self._execution_times)

    def run(self, operation: str):
        operation_handlers = {
            "load": self._load_tables,
            "update": self._update_data,
            "query": self._query_data
        }
        operation_handlers[operation]()
        self._dump_results_to_csv(operation=operation)

class IcebergExperiment(Experiment):
    def __init__(
        self, 
        *,
        experiment_id: str,
        iceberg_version: str = '0.14.0', 
        scala_version: str = '2.12',
        scale_in_gb=1,
        path: str = "s3a://experiments"
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"org.apache.iceberg:iceberg-spark-runtime-3.2_{scala_version}:{iceberg_version}"
        if ENV == 'dev': dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        ]
        super().__init__(table_format='iceberg', scale_in_gb=scale_in_gb, confs=confs, path=path, experiment_id=experiment_id)

class DeltaExperiment(Experiment):
    def __init__(
        self, 
        *,
        experiment_id: str,
        delta_version: str = '2.0.0', 
        scala_version: str = '2.12',
        scale_in_gb=1,
        path: str = "s3a://experiments"
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"io.delta:delta-core_{scala_version}:{delta_version},io.delta:delta-contribs_{scala_version}:{delta_version},io.delta:delta-hive_{scala_version}:0.2.0"
        if ENV == 'dev': dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
            ('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        ]
        super().__init__(table_format='delta', scale_in_gb=scale_in_gb, confs=confs, path=path, experiment_id=experiment_id)

class HudiExperiment(Experiment):
    def __init__(
        self, 
        *,
        experiment_id: str,
        hudi_version: str = '0.11.1', 
        scala_version: str = '2.12',
        scale_in_gb=1,
        path: str = "s3a://experiments"
    ):
        self._scale_in_gb = scale_in_gb
        dependencies = f"org.apache.hudi:hudi-spark3.2-bundle_{scala_version}:{hudi_version}"
        if ENV == 'dev': dependencies += AWS_JARS
        confs = [
            ("spark.jars.packages", dependencies),
            ('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension'),
            ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        ]
        super().__init__(table_format='hudi', scale_in_gb=scale_in_gb, confs=confs, path=path, experiment_id=experiment_id)



