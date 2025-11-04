# file: scripts/run_spark_score.ps1
$venv_python = "$PWD\.venv\Scripts\python.exe"
# Path to your spark-submit (in PATH)
$spark_submit = "spark-submit"

# Use Spark's Kafka package matching Scala 2.12 and Spark 3.5.5
$kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

# Export env for pyspark to use the same python interpreter
$env:PYSPARK_PYTHON = $venv_python
$env:PYSPARK_DRIVER_PYTHON = $venv_python

Write-Output "Using PYSPARK_PYTHON = $env:PYSPARK_PYTHON"
& $spark_submit --packages $kafka_pkg scripts\spark\stream_score.py
