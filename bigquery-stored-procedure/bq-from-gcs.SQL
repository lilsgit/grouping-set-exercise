CREATE OR REPLACE PROCEDURE `dt-lilytian-sandbox-dev.pyspark_data.spark_proc_gcs`()
WITH CONNECTION `dt-lilytian-sandbox-dev.europe-west2.spark-conn`
OPTIONS(engine="SPARK", runtime_version="1.1", main_file_uri="gs://bucket-to-bq-7/f3_pyspark_bq.py")
LANGUAGE PYTHON
