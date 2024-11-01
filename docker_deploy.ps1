# PowerShell Script for Docker Compose Management
param (
    [string]$ComposeFile = "docker-compose.yml"
)

function Build-DockerCompose {
    Write-Host "Building Docker Compose services..." -ForegroundColor Cyan
    docker-compose -f $ComposeFile build
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed!" -ForegroundColor Red
    }
}

function Up-DockerCompose {
    Write-Host "Starting Docker Compose services..." -ForegroundColor Cyan
    docker-compose -f $ComposeFile up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Services started successfully!" -ForegroundColor Green
    } else {
        Write-Host "Failed to start services!" -ForegroundColor Red
    }
}

function Build-Start-DockerCompose {
    Build-DockerCompose
    Up-DockerCompose
}

function Down-DockerCompose {
    Write-Host "Stopping Docker Compose services..." -ForegroundColor Cyan
    docker-compose -f $ComposeFile down
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Services stopped successfully!" -ForegroundColor Green
    } else {
        Write-Host "Failed to stop services!" -ForegroundColor Red
    }
}

function Clean-Docker {
    Write-Host "Deleting all Docker containers, networks, and volumes..." -ForegroundColor Cyan
    docker container prune -f
    docker volume prune -f
    docker network prune -f
    Write-Host "All containers, volumes, and networks deleted!" -ForegroundColor Green
}

function Delete-All {
    Write-Host "Removing all containers, images, volumes, and networks..." -ForegroundColor Red
    docker system prune -a --volumes -f
    Write-Host "All Docker resources removed!" -ForegroundColor Green
}

# Display menu
Write-Host "Choose an option:" -ForegroundColor Yellow
Write-Host "0. Build and start DockerCompose"
Write-Host "1. Build Docker Compose"
Write-Host "2. Start (up) Docker Compose"
Write-Host "3. Stop (down) Docker Compose"
Write-Host "4. Delete all containers, volumes, and networks"
Write-Host "5. Remove all Docker resources (system prune)"

# Read user input
$choice = Read-Host "Enter your choice (0-5)"

switch ($choice) {
    0 {
        Build-Start-DockerCompose
    }
    1 {
        Build-DockerCompose
    }
    2 {
        Up-DockerCompose
    }
    3 {
        Down-DockerCompose
    }
    4 {
        Clean-Docker
    }
    5 {
        Delete-All
    }
    default {
        Write-Host "Invalid choice. Please select a valid option." -ForegroundColor Red
    }
}
