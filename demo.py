"""
Download JAR file https://apache-hudi.slack.com/files/U04U11G3LK0/F05DMRB5KPT/hudi-spark3.3-bundle_2.12-0.14.0-snapshot.jar?origin_team=T4D7BR6T1&origin_channel=C04FP2WJVFZ
"""
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    import os
    import sys
    import uuid
    from datetime import datetime
    from faker import Faker
except Exception as e:
    print("Error: ", e)

global spark


def upsert_hudi_table(
        db_name,
        table_name,
        record_id,
        precomb_key,
        spark_df,
        validator_query,
        table_type='COPY_ON_WRITE',
        method='upsert',
        index_type='BLOOM',
        use_validator=False,
):
    base_path = f"file:///C:/tmp/{db_name}/{table_name}"
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.recordkey.field': record_id,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.precombine.field': precomb_key,
        'hoodie.index.type': index_type
    }

    if use_validator or validator_query == "true":
        hudi_options.update({
            'hoodie.precommit.validators': "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator",
            "hoodie.precommit.validators.equality.sql.queries": validator_query
        })

    try:
        spark_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(base_path)
    except Exception as e:
        error_table_name = f"error_{table_name}"
        error_path = f"file:///C:/tmp/{db_name}/{error_table_name}"
        error_configs = {
            'hoodie.table.name': error_table_name,
            'hoodie.datasource.write.table.type': table_type,
            'hoodie.datasource.write.recordkey.field': "error_id",
            'hoodie.datasource.write.table.name': error_table_name,
            'hoodie.datasource.write.operation': "insert",
            'hoodie.datasource.write.precombine.field': "error_id"
        }

        print(f"UPSERT Items into Error Table {error_table_name}")
        spark_df = spark_df.withColumn("error_id", F.lit(str(uuid.uuid4())))

        spark_df.write.format("hudi") \
            .options(**error_configs) \
            .mode("append") \
            .save(error_path)


def print_hudi_table(db_name, table_name, spark):
    path = f"file:///C:/tmp/{db_name}/{table_name}"
    spark.read.format("hudi").load(path).createOrReplaceTempView("hudi_snapshots")
    spark.sql("select * from hudi_snapshots").show(truncate=False)


def main():
    jar_file = 'hudi-spark3.3-bundle_2.12-0.14.0-SNAPSHOT.jar'
    os.environ['PYSPARK_SUBMIT_ARGS'] = f"--jars {jar_file} pyspark-shell"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.jars', jar_file) \
        .config('spark.sql.hive.convertMetastoreParquet', 'false') \
        .getOrCreate()

    db_name = 'hudidb'
    table_name = 'events'

    spark_df = spark.createDataFrame(data=[
        (1, "This is APPEND 1", 111, "1"),
        (2, "This is APPEND 2", 222, "2"), ],
        schema=["uuid", "message", "precomb", "partition"])
    spark_df.show()

    upsert_hudi_table(
        db_name=db_name,
        table_name=table_name,
        record_id='uuid',
        precomb_key='uuid',
        spark_df=spark_df,
        table_type='COPY_ON_WRITE',
        index_type='BLOOM',
        method='upsert',
        use_validator=True,
        validator_query="""SELECT COUNT(*) FROM <TABLE_NAME> WHERE message IS NULL;"""
    )

    spark_df = spark.createDataFrame(
        data=[
            (4, None, 444, None),
            (5, "This is APPEND 5", 555, "5"),
        ],
        schema=["uuid", "message", "precomb", "partition"])
    spark_df.show()

    upsert_hudi_table(
        db_name=db_name,
        table_name=table_name,
        record_id='uuid',
        precomb_key='uuid',
        spark_df=spark_df,
        table_type='COPY_ON_WRITE',
        index_type='BLOOM',
        method='upsert',
        use_validator=True,
        validator_query="""SELECT COUNT(*) FROM <TABLE_NAME> WHERE message IS NULL;"""
    )

    print("\n")
    print(f"Reading table {table_name}")
    print_hudi_table(db_name=db_name, table_name=table_name, spark=spark)
    print("\n")
    print(f"Reading table error_{table_name}")
    print_hudi_table(db_name=db_name, table_name=f"error_{table_name}", spark=spark)
    print("\n")


main()
