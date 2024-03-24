# mysql-audit-log-to-mining-search-csv
A Python script to convert a mysql audit log file to "MiningSearch" style CSV file.

## usage
```
python3 convert_myaudit_to_mscsv.py <AUDIT_LOG> <OUTPUT_CSV_FILE_NAME> 
```

## 制限事項
* 再起動などをしてセッションIDが重複している場合異なるセッションと紐づけらることがあります
