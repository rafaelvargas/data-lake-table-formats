
# Data Lake File Formats

Benchmark of Data Lake File Formats


## Setting up the local environment

### Installing PySpark

```
pyenv install 3.9.11
pyenv virtualenv 3.9.11 data-lake-file-formats
pip install -r requirements.txt
```

## Delta

```sql
-- create external partitioned table
CREATE TABLE iceberg_external_table (
  id bigint,
  data string,
  category string
)
USING iceberg
PARTITIONED BY (category)
location 's3a://iceberg/iceberg_external_table';

INSERT INTO iceberg_external_table VALUES (1, 'a', 'c1'), (2, 'b', 'c1'), (3, 'c', 'c2');
```


## Iceberg

```sql
-- create external partitioned table
CREATE TABLE iceberg_external_table (
  id bigint,
  data string,
  category string
)
USING iceberg
PARTITIONED BY (category)
location 's3a://iceberg/iceberg_external_table';

INSERT INTO iceberg_external_table VALUES (1, 'a', 'c1'), (2, 'b', 'c1'), (3, 'c', 'c2');
```


## Hudi

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
)
location 's3a://hudi/hudi_external_cow_table';

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
partitioned by (dt)
location 's3a://hudi/hudi_external_partitioned_cow_table';

insert into hudi_external_partitioned_cow_table select 1 as id, 'test' as name, 1000 as ts;
```


