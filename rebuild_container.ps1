# PowerShell Script for Docker Compose Management
param (
    [string]$ComposeFile = "docker-compose.yml"
)

function Restart-Main-Processor {
    Write-Host "Rebuilding Main Processor service..." -ForegroundColor Cyan
    docker-compose build --no-cache main-processor
    docker-compose up -d main-processor
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}

function Restart-Grafana {
    Write-Host "Rebuilding Grafana service..." -ForegroundColor Cyan
    docker-compose build grafana
    docker-compose up -d grafana
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}

function Restart-Cassandra {
    Write-Host "Rebuilding Cassandra service..." -ForegroundColor Cyan
    docker-compose build cassandra-init
    docker-compose up -d cassandra-init
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}

function Restart-Kafka{
    Write-Host "Rebuilding Kafka service..." -ForegroundColor Cyan
    docker-compose build kafka-broker-init
    docker-compose up -d kafka-broker-init
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}

function Restart-Data-Producer{
    Write-Host "Rebuilding Data Producer service..." -ForegroundColor Cyan
    docker-compose build data-producer
    docker-compose up -d data-producer
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}



# Display menu
Write-Host "Choose an option:" -ForegroundColor Yellow
Write-Host "0. Main Processor"
Write-Host "1. Grafana"
Write-Host "2. Cassandra"
Write-Host "3. Kafka"
Write-Host "4. Data Producer"

# Read user input
$choice = Read-Host "Enter your choice (0-4)"

switch ($choice) {
    0 {
        Restart-Main-Processor
    }
    1 {
        Restart-Grafana
    }
    2 {
        Restart-Cassandra
    }
    3 {
        Restart-Kafka
    }
    4 {
        Restart-Data-Producer
    }
    default {
        Write-Host "Invalid choice. Please select a valid option." -ForegroundColor Red
    }
}
