# file: scripts/run_spark_ephemeral.ps1
param(
  [int]$runSeconds = 15
)

# ensure venv python is used by pyspark workers
$proj = "F:\Projects\adaptive-fraud-stream"
$env:PYSPARK_PYTHON = Join-Path $proj ".venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

# Optionally set JAVA_HOME here if needed (uncomment and adjust)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:PATH = "$($env:JAVA_HOME)\bin;$env:PATH"
$env:SPARK_LOCAL_DIRS = "F:\spark-temp"
$env:IVY_HOME = "F:\ivy-cache"				# To avoid repeated downloads for the used packages

# call spark-submit; if spark-submit isn't in PATH, replace with full path to spark-submit.cmd
$sparkSubmit = "C:\spark\bin\spark-submit.cmd"
$pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
$script = Join-Path $proj "scripts\spark\stream_score_with_alerts.py"

# generate a run id
$runId = [int][double]::Parse((Get-Date -UFormat %s)) + "-" + [System.Guid]::NewGuid().ToString("N").Substring(0,6)

# call spark-submit with run-id (so Python picks it up)
# Run and forward exit code
& $sparkSubmit --master local[*] --packages $pkg $script --run-seconds $runSeconds --run-id $runId
#& $sparkSubmit --packages $pkg $script --run-seconds $runSeconds
exit $LASTEXITCODE
