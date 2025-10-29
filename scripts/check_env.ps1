# Windows PowerShell script to collect environment info
Write-Output '--- ENV CHECK: Adaptive Fraud-Scoring Stream ---'
Write-Output 'git version:'
git --version
Write-Output 'java version:'
java -version 2>&1
Write-Output 'spark-submit version:'
spark-submit --version 2>&1
Write-Output 'python version:'
python --version 2>&1
Write-Output 'kafka topics (WSL):'
wsl kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1 || Write-Output 'Could not run kafka-topics via wsl'
