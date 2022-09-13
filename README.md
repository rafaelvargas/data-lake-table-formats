
# Data Lake File Formats

Benchmark of Data Lake File Formats

## Setting up the cluster

```bash
mkdir spark
cd spark
wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz
tar -zvxf spark-3.1.3-bin-hadoop3.2.tgz
```


## Setting up the local environment

### Installing PySpark

```
pyenv install 3.9.11
pyenv virtualenv 3.9.11 data-lake-file-formats
pyenv activate data-lake-file-formats
pip install -r requirements.txt
```

```bash
export PYSPARK_PYTHON=/path/to/python/binary/executable
export PYSPARK_DRIVER_PYTHON=/path/to/python/binary/executable

export PYSPARK_PYTHON=~/.pyenv/versions/3.9.11/bin/python3
export PYSPARK_DRIVER_PYTHON=~/.pyenv/versions/3.9.11/bin/python3
export SPARK_HOME=~/.pyenv/versions/data-lake-file-formats/lib/python3.9/site-packages/pyspark
```

## Delta

```bash
spark-sql --properties-file delta.conf
```

```sql
-- create external partitioned table
CREATE TABLE IF NOT EXISTS delta_external_table (
  id INT,
  data STRING,
  category STRING
) USING DELTA;

INSERT INTO delta_external_table VALUES (1, 'a', 'c1'), (2, 'b', 'c1'), (3, 'c', 'c2');
```

### Compatibility

https://docs.delta.io/latest/releases.html#compatibility-with-apache-spark


## Iceberg

```bash
spark-sql --properties-file iceberg.conf
```

```sql
-- create external partitioned table
CREATE TABLE iceberg_external_table (
  id bigint,
  data string,
  category string
)
USING iceberg
PARTITIONED BY (category);

INSERT INTO iceberg_external_table VALUES (1, 'a', 'c1'), (2, 'b', 'c1'), (3, 'c', 'c2');
```


## Hudi

```bash
spark-sql --properties-file hudi.conf
```

### Copy on Write Tables


```sql
-- create an external cow table
create table if not exists hudi_external_cow_table (
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'cow',
  primaryKey = 'id'
);

insert into hudi_external_cow_table select 1 as id, 'test' as name, 20.0 as price;

-- create an external partitioned cow table
create table if not exists hudi_external_partitioned_cow_table (
  id bigint,
  name string,
  dt timestamp
) using hudi
options (
  type = 'cow',
  primaryKey = 'id'
) 
partitioned by (dt);

insert into hudi_external_partitioned_cow_table select 1 as id, 'test' as name, 1000 as ts;
```

### Merge on Read Tables 

```sql
-- create an external mor table
create table if not exists hudi_external_mor_table (
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'mor',
  primaryKey = 'id'
);

insert into hudi_external_mor_table select 1 as id, 'test' as name, 20.0 as price;
```
