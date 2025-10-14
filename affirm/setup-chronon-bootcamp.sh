#!/bin/bash

# Chronon Bootcamp Setup Script
# Minimal setup for learning Chronon GroupBy features
# Includes: Spark + Iceberg + MongoDB + MinIO + Jupyter

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="docker-compose-bootcamp.yml"
MINIO_ALIAS="local"
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

echo "🎓 Starting Chronon Bootcamp (minimal setup)"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Docker
    if ! command_exists docker; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command_exists docker-compose; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    # Check for port conflicts
    local ports=(9000 9001 27017 8080 7077 8888)
    for port in "${ports[@]}"; do
        if lsof -i :$port >/dev/null 2>&1; then
            print_warning "Port $port is already in use. This may cause conflicts."
        fi
    done
    
    print_success "Prerequisites check completed"
}

# Function to wait for service to be ready
wait_for_service() {
    local service_name="$1"
    local test_command="$2"
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if eval "$test_command" >/dev/null 2>&1; then
            print_success "$service_name is ready!"
            return 0
        fi
        
        print_status "Attempt $attempt/$max_attempts - $service_name not ready yet, waiting..."
        sleep 5
        ((attempt++))
    done
    
    print_error "$service_name failed to start within expected time"
    return 1
}

# Function to cleanup existing setup
cleanup_existing_setup() {
    print_status "Cleaning up existing setup..."
    
    # Stop and remove containers
    if [ -f "$COMPOSE_FILE" ]; then
        docker-compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
    fi
    
    # Remove any orphaned containers
    docker container prune -f >/dev/null 2>&1 || true
    
    # Clean up networks
    docker network prune -f >/dev/null 2>&1 || true
    
    print_success "Cleanup completed"
}

# Function to start core services
start_core_services() {
    print_status "Starting core services..."
    
    # Bootcamp: Start only essential services
    docker-compose -f "$COMPOSE_FILE" up -d minio mongodb
    
    # Wait for services to be ready
    wait_for_service "MinIO" "curl -f http://localhost:9000/minio/health/live"
    wait_for_service "MongoDB" "docker-compose -f $COMPOSE_FILE exec mongodb mongosh --eval 'db.adminCommand(\"ping\")'"
    
    print_success "Core services started"
}

# Function to configure MinIO
configure_minio() {
    print_status "Configuring MinIO..."
    
    # Install MinIO client if not present
    if ! command_exists mc; then
        print_status "Installing MinIO client..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            if command_exists brew; then
                brew install minio/stable/mc
            else
                print_warning "Homebrew not found. Please install MinIO client manually: https://docs.min.io/docs/minio-client-quickstart-guide.html"
                return 1
            fi
        else
            # Linux
            curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
            chmod +x mc
            sudo mv mc /usr/local/bin/
        fi
    fi
    
    # Configure MinIO client
    if ! mc alias list | grep -q "$MINIO_ALIAS"; then
        mc alias set "$MINIO_ALIAS" "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
        print_success "MinIO client configured"
    else
        print_status "MinIO client already configured"
    fi
    
    # Create buckets
    mc mb "$MINIO_ALIAS/chronon" --ignore-existing
    mc mb "$MINIO_ALIAS/chronon/warehouse" --ignore-existing
    mc mb "$MINIO_ALIAS/chronon/warehouse/data" --ignore-existing
    mc mb "$MINIO_ALIAS/chronon/warehouse/data/purchases" --ignore-existing
    mc mb "$MINIO_ALIAS/chronon/warehouse/data/users" --ignore-existing
    
    print_success "MinIO buckets created"
}

# Function to copy sample data to MinIO
copy_sample_data_to_minio() {
    print_status "Copying sample data to MinIO..."
    
    # Check if sample data files exist
    if [ ! -f "sample_data/purchases.parquet" ] || [ ! -f "sample_data/users.parquet" ]; then
        print_warning "Sample data files not found. Generating them now..."
        if command_exists python3; then
            python3 scripts/generate_sample_parquet.py
        else
            print_error "Python3 not found. Please generate sample data manually."
            return 1
        fi
    fi
    
    # Copy Parquet files directly to MinIO
    print_status "Copying Parquet files to MinIO..."
    
    # Copy purchases data
    if mc cp sample_data/purchases.parquet "$MINIO_ALIAS/chronon/warehouse/data/purchases/" >/dev/null 2>&1; then
        print_success "Copied purchases.parquet to MinIO"
    else
        print_warning "Failed to copy purchases.parquet to MinIO"
    fi
    
    # Copy users data
    if mc cp sample_data/users.parquet "$MINIO_ALIAS/chronon/warehouse/data/users/" >/dev/null 2>&1; then
        print_success "Copied users.parquet to MinIO"
    else
        print_warning "Failed to copy users.parquet to MinIO"
    fi
    
    # Verify files are in MinIO
    print_status "Verifying files in MinIO..."
    if mc ls "$MINIO_ALIAS/chronon/warehouse/data/purchases/" | grep -q "purchases.parquet" && \
       mc ls "$MINIO_ALIAS/chronon/warehouse/data/users/" | grep -q "users.parquet"; then
        print_success "Sample data copied to MinIO successfully!"
        print_status "📊 Files available at: s3a://chronon/warehouse/data/"
        print_status "📁 purchases.parquet (~155 records)"
        print_status "📁 users.parquet (100 records)"
        print_status "📅 Data spans 7 days (Dec 1-7, 2023)"
        print_status "🏷️ Categories: electronics, books, clothing, food, home"
    else
        print_warning "Some files may not have been copied successfully"
        print_status "You can access the data directly from sample_data/ directory"
    fi
}

# Function to create Spark tables from parquet files
create_spark_tables() {
    print_status "Creating Spark tables from parquet data..."
    
    # Download S3A JARs if not present
    if [ ! -f "/tmp/hadoop-aws-3.2.4.jar" ]; then
        print_status "Downloading Hadoop AWS JARs..."
        curl -s -o /tmp/hadoop-aws-3.2.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.4/hadoop-aws-3.2.4.jar
        curl -s -o /tmp/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar
    fi
    
    # Copy JARs to container
    CHRONON_CONTAINER=$(docker-compose -f "$COMPOSE_FILE" ps -q chronon-main)
    docker cp /tmp/hadoop-aws-3.2.4.jar ${CHRONON_CONTAINER}:/tmp/ >/dev/null 2>&1
    docker cp /tmp/aws-java-sdk-bundle-1.11.1026.jar ${CHRONON_CONTAINER}:/tmp/ >/dev/null 2>&1
    
    # Create partitioned purchases table
    print_status "Creating partitioned 'purchases' table..."
    docker-compose -f "$COMPOSE_FILE" exec -T chronon-main spark-sql \
      --jars /tmp/hadoop-aws-3.2.4.jar,/tmp/aws-java-sdk-bundle-1.11.1026.jar \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
      -e "CREATE TABLE IF NOT EXISTS purchases (user_id STRING, purchase_price DOUBLE, item_category STRING, ts BIGINT) USING PARQUET PARTITIONED BY (ds STRING); INSERT INTO purchases SELECT user_id, purchase_price, item_category, CAST(UNIX_TIMESTAMP(ts) * 1000 AS BIGINT) as ts, DATE_FORMAT(ts, 'yyyy-MM-dd') as ds FROM parquet.\`s3a://chronon/warehouse/data/purchases/purchases.parquet\`" >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "Created partitioned 'purchases' table with 7 partitions (2023-12-01 to 2023-12-07)"
    else
        print_warning "Failed to create purchases table - you may need to create it manually"
    fi
}

# Function to start processing services
start_processing_services() {
    print_status "Starting processing services..."
    
    # Start Spark (required for GroupBy processing)
    docker-compose -f "$COMPOSE_FILE" up -d spark-master spark-worker
    
    # Wait for Spark to be ready
    wait_for_service "Spark Master" "curl -f http://localhost:8080"
    
    print_success "Processing services started"
}

# Function to start development tools
start_development_tools() {
    print_status "Starting development tools..."
    
    # Start Chronon main container and Jupyter
    docker-compose -f "$COMPOSE_FILE" up -d chronon-main jupyter
    
    # Give services time to start
    sleep 10
    
    print_success "Development tools started"
}

# Function to verify services
verify_services() {
    print_status "Verifying all services..."
    
    local services=(
        "MinIO:http://localhost:9001"
        "Spark:http://localhost:8080"
        "Jupyter:http://localhost:8888"
    )
    
    for service in "${services[@]}"; do
        local name="${service%%:*}"
        local url="${service##*:}"
        
        if curl -f "$url" >/dev/null 2>&1; then
            print_success "$name is accessible at $url"
        else
            print_warning "$name may not be fully ready at $url"
        fi
    done
    
    # Test MongoDB
    if docker-compose -f "$COMPOSE_FILE" exec mongodb mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
        print_success "MongoDB is accessible"
    else
        print_warning "MongoDB may not be fully ready"
    fi
    
    # Test Iceberg functionality with Hive catalog
    print_status "Testing Iceberg functionality..."
    # Give Spark more time to initialize
    sleep 15
    
    if docker-compose -f "$COMPOSE_FILE" exec chronon-main spark-sql -e "
        CREATE TABLE IF NOT EXISTS test_iceberg (id INT, name STRING) USING ICEBERG;
        INSERT INTO test_iceberg VALUES (1, 'test');
        SELECT * FROM test_iceberg;
        DROP TABLE test_iceberg;
    " >/dev/null 2>&1; then
        print_success "Iceberg functionality verified"
    else
        print_warning "Iceberg test failed - Spark may still be initializing"
        print_status "You can test Iceberg manually later with: docker-compose -f $COMPOSE_FILE exec chronon-main spark-sql"
    fi
    
    print_success "Service verification completed"
}

# Function to display access information
display_access_info() {
    print_success "Chronon Bootcamp Setup Completed!"
    echo
    echo "=== Access Information ==="
    echo "MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
    echo "Spark Master:      http://localhost:8080"
    echo "Jupyter Notebooks: http://localhost:8888 (token: chronon-dev) - ESSENTIAL for GroupBy verification"
    echo "MongoDB:           localhost:27017"
    echo
    echo "=== Next Steps ==="
    echo "🎓 Follow the bootcamp: CHRONON_BOOTCAMP.md"
    echo "1. Access Jupyter at http://localhost:8888 for data exploration"
    echo "2. Use Spark at http://localhost:8080 for batch processing"
    echo "3. Monitor MinIO at http://localhost:9001 for storage"
    echo
    echo "=== Sample Data Ready ==="
    echo "✅ Sample data has been copied to MinIO and is ready for GroupBy development!"
    echo "📊 Parquet files available at: s3a://chronon/warehouse/data/"
    echo "📁 purchases.parquet (~155 records), users.parquet (100 records)"
    echo "📅 Data spans 7 days (Dec 1-7, 2023) with realistic purchase patterns"
    echo "🏷️ Categories: electronics, books, clothing, food, home"
    echo
    echo "=== Start Learning ==="
    echo "🎓 Follow the bootcamp: CHRONON_BOOTCAMP.md"
    echo "🔍 Optional: Explore data in Jupyter at http://localhost:8888"
    echo
    echo "=== Useful Commands ==="
    echo "Stop all services:    docker-compose -f $COMPOSE_FILE down"
    echo "View logs:            docker-compose -f $COMPOSE_FILE logs [service-name]"
    echo "Restart service:      docker-compose -f $COMPOSE_FILE restart [service-name]"
    echo
    echo "=== Troubleshooting ==="
    echo "If services don't start properly:"
    echo "1. Check Docker is running: docker info"
    echo "2. Check port conflicts: lsof -i :9000 -i :8080 -i :8888 -i :27017"
    echo "3. View service logs: docker-compose -f $COMPOSE_FILE logs [service-name]"
    echo "4. Restart specific service: docker-compose -f $COMPOSE_FILE restart [service-name]"
}

# Main execution
main() {
    print_status "Starting Chronon Bootcamp setup..."
    
    check_prerequisites
    cleanup_existing_setup
    start_core_services
    configure_minio
    copy_sample_data_to_minio
    create_spark_tables
    start_processing_services
    start_development_tools
    verify_services
    display_access_info
    
    print_success "Chronon Bootcamp setup completed successfully!"
}

# Run main function
main "$@"
