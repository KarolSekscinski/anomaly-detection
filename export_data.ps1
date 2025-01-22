$local_folder_name = "arima"

# Define file paths
$cassandra_5min_csv = "/tmp/$local_folder_name_5min.csv"
$p_cassandra_5min_csv = "/tmp/p_$local_folder_name_5min.csv"
$v_cassandra_5min_csv = "/tmp/v_$local_folder_name_5min.csv"

# Define local destination directories
$local_base_path = "./sandbox/data/$local_folder_name/base_5min.csv"
$local_p_path = "./sandbox/data/$local_folder_name/p_5min.csv"
$local_v_path = "./sandbox/data/$local_folder_name/v_5min.csv"

# Run CQL commands to export data
Write-Output "Exporting data from Cassandra tables..."
docker exec -i cassandra cqlsh -e "COPY market.stocks TO '$cassandra_5min_csv' WITH HEADER = TRUE;"
docker exec -i cassandra cqlsh -e "COPY market.price_anomalies TO '$p_cassandra_5min_csv' WITH HEADER = TRUE;"
docker exec -i cassandra cqlsh -e "COPY market.volume_anomalies TO '$v_cassandra_5min_csv' WITH HEADER = TRUE;"

# Copy files from Docker container to local machine
Write-Output "Copying exported files to local directories..."
docker cp cassandra:$cassandra_5min_csv $local_base_path
docker cp cassandra:$p_cassandra_5min_csv $local_p_path
docker cp cassandra:$v_cassandra_5min_csv $local_v_path

# Check if files exist locally
if (Test-Path $local_base_path) {
    Write-Output "Base 5-minute data copied successfully to $local_base_path"
} else {
    Write-Output "Failed to copy Bese 5-minute data."
}

if (Test-Path $local_p_path) {
    Write-Output "Price anomalies data copied successfully to $local_p_path"
} else {
    Write-Output "Failed to copy Price anomalies data."
}

if (Test-Path $local_v_path) {
    Write-Output "Volume anomalies data copied successfully to $local_v_path"
} else {
    Write-Output "Failed to copy Volume anomalies data."
}

Write-Output "Data export and copy process completed!"
