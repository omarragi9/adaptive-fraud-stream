param(
    [string]$tx_id,
    [string]$user_id,
    [string]$card_id,
    [string]$amount,
    [string]$currency,
    [string]$merchant,
    [string]$score
)

# Build one single CSV line
$line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ',' +
        ($tx_id  -as [string]) + ',' +
        ($user_id -as [string]) + ',' +
        ($card_id -as [string]) + ',' +
        ($amount -as [string]) + ',' +
        ($currency -as [string]) + ',' +
        ($merchant -as [string]) + ',' +
        ($score -as [string])

Add-Content -Path 'F:\Projects\adaptive-fraud-stream\label_queue\labels.csv' -Value $line
