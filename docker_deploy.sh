#!/bin/bash

# Default Docker Compose file
COMPOSE_FILE="docker-compose.yml"

# Function to build Docker Compose services
function build_docker_compose {
    echo -e "\e[36mBuilding Docker Compose services...\e[0m"
    docker-compose -f $COMPOSE_FILE build
    if [ $? -eq 0 ]; then
        echo -e "\e[32mBuild successful!\e[0m"
    else
        echo -e "\e[31mBuild failed!\e[0m"
    fi
}

# Function to start (up) Docker Compose services
function up_docker_compose {
    echo -e "\e[36mStarting Docker Compose services...\e[0m"
    docker-compose -f $COMPOSE_FILE up -d
    if [ $? -eq 0 ]; then
        echo -e "\e[32mServices started successfully!\e[0m"
    else
        echo -e "\e[31mFailed to start services!\e[0m"
    fi
}

# Function to stop (down) Docker Compose services
function down_docker_compose {
    echo -e "\e[36mStopping Docker Compose services...\e[0m"
    docker-compose -f $COMPOSE_FILE down
    if [ $? -eq 0 ]; then
        echo -e "\e[32mServices stopped successfully!\e[0m"
    else
        echo -e "\e[31mFailed to stop services!\e[0m"
    fi
}

# Function to clean Docker containers, networks, and volumes
function clean_docker {
    echo -e "\e[36mDeleting all stopped containers, networks, and volumes...\e[0m"
    docker container prune -f
    docker volume prune -f
    docker network prune -f
    echo -e "\e[32mAll containers, volumes, and networks deleted!\e[0m"
}

# Function to remove all Docker resources
function delete_all {
    echo -e "\e[31mRemoving all Docker resources (containers, images, volumes, networks)...\e[0m"
    docker system prune -a --volumes -f
    echo -e "\e[32mAll Docker resources removed!\e[0m"
}

# Display menu
echo -e "\e[33mChoose an option:\e[0m"
echo "1. Build Docker Compose"
echo "2. Start (up) Docker Compose"
echo "3. Stop (down) Docker Compose"
echo "4. Delete all containers, volumes, and networks"
echo "5. Remove all Docker resources (system prune)"

# Read user input
read -p "Enter your choice (1-5): " choice

case $choice in
    1)
        build_docker_compose
        ;;
    2)
        up_docker_compose
        ;;
    3)
        down_docker_compose
        ;;
    4)
        clean_docker
        ;;
    5)
        delete_all
        ;;
    *)
        echo -e "\e[31mInvalid choice. Please select a valid option.\e[0m"
        ;;
esac
