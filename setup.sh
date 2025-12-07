#!/usr/bin/env bash
# LordFilm Parser - System Setup Script
# Generates docker-compose.yml and systemd service files from templates
# User interaction with defaults and environment variable detection

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored message
log_info() { echo -e "${BLUE}ℹ ${NC}$1"; }
log_success() { echo -e "${GREEN}✅ ${NC}$1"; }
log_warning() { echo -e "${YELLOW}⚠ ${NC}$1"; }
log_error() { echo -e "${RED}❌ ${NC}$1"; }

# Get current directory (project root)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER="$(whoami)"
PYTHON_PATH="$(which python3)"

# Default values
DEFAULT_POSTGRES_DB="lordfilm_db"
DEFAULT_POSTGRES_USER="lordfilm_user"
DEFAULT_POSTGRES_PASSWORD=""

# Function to prompt user with current value detection
prompt_with_default() {
    local var_name="$1"
    local prompt_text="$2"
    local default_value="$3"

    # Check if variable is already set in environment
    local current_value="${!var_name:-}"

    if [[ -n "$current_value" ]]; then
        echo -e "Current ${BLUE}$var_name${NC}=[${GREEN}$current_value${NC}]"
        read -p "$prompt_text [press Enter to keep]: " user_input
        if [[ -z "$user_input" ]]; then
            echo "$current_value"
        else
            echo "$user_input"
        fi
    else
        read -p "$prompt_text [$default_value]: " user_input
        echo "${user_input:-$default_value}"
    fi
}

# Function to check requirements
check_requirements() {
    log_info "Checking system requirements..."

    # Check for podman
    if ! command -v podman &> /dev/null; then
        log_error "podman is not installed. Please install podman first."
        exit 1
    fi

    # Check for podman-compose
    if ! command -v podman-compose &> /dev/null; then
        log_error "podman-compose is not installed. Please install podman-compose first."
        exit 1
    fi

    # Check for python3
    if ! command -v python3 &> /dev/null; then
        log_error "python3 is not installed. Please install python3 first."
        exit 1
    fi

    # Check template files
    local missing_templates=()

    if [[ ! -f "docker-compose.yml.template" ]]; then
        missing_templates+=("docker-compose.yml.template")
    fi

    for template in systemd/*.template; do
        if [[ ! -f "$template" ]]; then
            missing_templates+=("$template")
        fi
    done

    if [[ ${#missing_templates[@]} -gt 0 ]]; then
        log_error "Missing template files:"
        for template in "${missing_templates[@]}"; do
            log_error "  - $template"
        done
        exit 1
    fi

    log_success "All requirements satisfied"
}

# Function to gather database configuration
get_database_config() {
    log_info "📦 PostgreSQL Database Configuration"
    echo "========================================"

    # Export variables for prompt function
    export POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD

    POSTGRES_DB=$(prompt_with_default "POSTGRES_DB" "Database name" "$DEFAULT_POSTGRES_DB")
    POSTGRES_USER=$(prompt_with_default "POSTGRES_USER" "Database user" "$DEFAULT_POSTGRES_USER")

    # Password requires special handling
    if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
        echo -e "Current ${BLUE}POSTGRES_PASSWORD${NC}=[${GREEN}******${NC}]"
        read -p "Database password [press Enter to keep, or type new]: " db_pass_input
        if [[ -n "$db_pass_input" ]]; then
            POSTGRES_PASSWORD="$db_pass_input"
        fi
    else
        while [[ -z "$POSTGRES_PASSWORD" ]]; do
            read -sp "Database password (required): " POSTGRES_PASSWORD
            echo
            if [[ -z "$POSTGRES_PASSWORD" ]]; then
                log_warning "Password cannot be empty"
            fi
        done
    fi

    echo
}

# Function to generate docker-compose.yml
generate_docker_compose() {
    log_info "Generating docker-compose.yml from template..."

    # Use sed to replace placeholders
    sed \
        -e "s|{{POSTGRES_DB}}|$POSTGRES_DB|g" \
        -e "s|{{POSTGRES_USER}}|$POSTGRES_USER|g" \
        -e "s|{{POSTGRES_PASSWORD}}|$POSTGRES_PASSWORD|g" \
        docker-compose.yml.template > docker-compose.yml

    # Set proper permissions
    chmod 644 docker-compose.yml

    log_success "Generated docker-compose.yml"
}

# Function to start containers
start_containers() {
    log_info "Starting containers with podman-compose..."

    if ! podman-compose up -d; then
        log_error "Failed to start containers"
        exit 1
    fi

    log_success "Containers started successfully"
    echo
    log_info "Waiting for containers to become healthy..."

    # Wait for PostgreSQL
    local postgres_healthy=false
    for i in {1..30}; do
        if podman exec movie_postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" &>/dev/null; then
            postgres_healthy=true
            break
        fi
        echo -n "."
        sleep 2
    done

    if [[ "$postgres_healthy" == true ]]; then
        log_success "PostgreSQL container is healthy"
    else
        log_warning "PostgreSQL container health check timeout - continuing anyway"
    fi

    # Wait for Selenium
    local selenium_healthy=false
    for i in {1..30}; do
        if curl -s http://localhost:4444/wd/hub/status &>/dev/null; then
            selenium_healthy=true
            break
        fi
        echo -n "."
        sleep 2
    done

    if [[ "$selenium_healthy" == true ]]; then
        log_success "Selenium container is healthy"
    else
        log_warning "Selenium container health check timeout - continuing anyway"
    fi

    echo
}

# Function to generate systemd files
generate_systemd_files() {
    log_info "Generating systemd service files..."

    # Create temporary directory
    local temp_dir="/tmp/lordfilm-setup-$(date +%s)"
    mkdir -p "$temp_dir"

    # Generate each systemd file from template
    for template in systemd/*.template; do
        local filename=$(basename "$template" .template)
        local output_file="$temp_dir/$filename"

        log_info "  Processing: $filename"

        sed \
            -e "s|{{USER}}|$USER|g" \
            -e "s|{{GROUP}}|$USER|g" \
            -e "s|{{PROJECT_DIR}}|$PROJECT_DIR|g" \
            -e "s|{{PYTHON_PATH}}|$PYTHON_PATH|g" \
            "$template" > "$output_file"

        log_success "    Generated: $output_file"
    done

    echo "$temp_dir"  # Return temp directory path
}

# Function to copy systemd files with confirmation
copy_systemd_files() {
    local temp_dir="$1"

    log_info "📁 Generated systemd files are in: $temp_dir"
    echo
    ls -la "$temp_dir/"
    echo

    read -p "❓ Copy these files to /etc/systemd/system/? [y/N]: " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Copying systemd files..."

        if ! sudo cp "$temp_dir"/* /etc/systemd/system/; then
            log_error "Failed to copy systemd files. You may need sudo privileges."
            return 1
        fi

        log_success "Systemd files copied to /etc/systemd/system/"
        return 0
    else
        log_warning "Skipping systemd file copy"
        return 2
    fi
}

# Function to print next steps instructions
print_instructions() {
    log_info "📋 NEXT STEPS:"
    echo "=============="
    echo
    echo "1. Reload systemd daemon:"
    echo "   ${GREEN}sudo systemctl daemon-reload${NC}"
    echo
    echo "2. Enable and start the timer (for automatic parsing):"
    echo "   ${GREEN}sudo systemctl enable lordfilm-parser.timer${NC}"
    echo "   ${GREEN}sudo systemctl start lordfilm-parser.timer${NC}"
    echo
    echo "3. Enable and start the bot service:"
    echo "   ${GREEN}sudo systemctl enable lordfilm-bot.service${NC}"
    echo "   ${GREEN}sudo systemctl start lordfilm-bot.service${NC}"
    echo
    echo "4. Check service status:"
    echo "   ${GREEN}systemctl status lordfilm-parser.timer${NC}"
    echo "   ${GREEN}systemctl status lordfilm-bot.service${NC}"
    echo
    echo "5. View logs:"
    echo "   ${GREEN}journalctl -u lordfilm-parser -f${NC}"
    echo "   ${GREEN}journalctl -u lordfilm-bot -f${NC}"
    echo
    echo "6. Manual parser test (optional):"
    echo "   ${GREEN}cd $PROJECT_DIR${NC}"
    echo "   ${GREEN}python3 scripts/main_runner.py --mode daily --limit 10${NC}"
    echo
    echo "7. Container management:"
    echo "   ${GREEN}cd $PROJECT_DIR${NC}"
    echo "   ${GREEN}podman-compose ps          # List containers${NC}"
    echo "   ${GREEN}podman-compose logs        # View logs${NC}"
    echo "   ${GREEN}podman-compose restart     # Restart containers${NC}"
    echo
    echo "8. To stop everything:"
    echo "   ${GREEN}sudo systemctl stop lordfilm-parser.timer lordfilm-bot.service${NC}"
    echo "   ${GREEN}sudo systemctl disable lordfilm-parser.timer lordfilm-bot.service${NC}"
    echo "   ${GREEN}cd $PROJECT_DIR && podman-compose down${NC}"
    echo
}

# Main function
main() {
    echo -e "${BLUE}🚀 LordFilm Parser - System Setup${NC}"
    echo "======================================"
    echo

    # Check requirements
    check_requirements

    # Get database configuration
    get_database_config

    # Generate docker-compose.yml
    generate_docker_compose

    # Start containers
    start_containers

    # Generate systemd files
    local temp_dir
    temp_dir=$(generate_systemd_files)

    # Copy systemd files with confirmation
    copy_systemd_files "$temp_dir"
    local copy_status=$?

    echo
    echo "======================================"

    if [[ $copy_status -eq 0 ]]; then
        log_success "✅ Setup completed successfully!"
    elif [[ $copy_status -eq 2 ]]; then
        log_warning "⚠️  Setup partially completed (systemd files not copied)"
    else
        log_error "❌ Setup completed with errors"
    fi

    echo
    print_instructions

    # Cleanup temp directory
    if [[ -d "$temp_dir" ]]; then
        rm -rf "$temp_dir"
    fi
}

# Run main function
main