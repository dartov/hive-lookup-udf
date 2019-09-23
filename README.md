# Lookup Prefix Hive UDF example

0. You might want to use [local Hive installation](https://github.com/big-data-europe/docker-hive) to test that UDF

1. Build the package and copy it to the server
```shell script
mvn package
docker cp target/hive-lookup-udf-1.0-SNAPSHOT.jar docker-hive_hive-server_1:/root/
docker-compose exec hive-server bash
```

2. Upload UDF jar to HDFS, generate example data and example prefix table, login to Hive
```shell script
hdfs dfs -put /root/hive-lookup-udf-1.0-SNAPSHOT.jar /tmp
cat > /root/numbers.txt <<EOF
123456789
987654321
124456789
988654321
EOF
cat > /root/prefixes.txt <<EOF
1234,1
9876,2
12,3
98,4
EOF
hdfs dfs -put /root/prefixes.txt /tmp
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

3. Create function, create example table, call the function
```sql
DROP FUNCTION IF EXISTS longest_prefix;
CREATE FUNCTION longest_prefix AS 'org.dartov.hive.LookupPrefix' USING JAR 'hdfs://namenode:8020/tmp/hive-lookup-udf-1.0-SNAPSHOT.jar';
CREATE TABLE numbers (number STRING);
LOAD DATA LOCAL INPATH '/root/numbers.txt' OVERWRITE INTO TABLE numbers;
CREATE TABLE number_prefix AS SELECT number,longest_prefix(number,'/tmp/prefixes.txt') AS prefix_id FROM numbers;
SELECT * FROM number_prefix;
```

The results should be:
```text
+-----------------------+--------------------------+
| number_prefix.number  | number_prefix.prefix_id  |
+-----------------------+--------------------------+
| 123456789             | 1                        |
| 987654321             | 2                        |
| 124456789             | 3                        |
| 988654321             | 4                        |
+-----------------------+--------------------------+
```