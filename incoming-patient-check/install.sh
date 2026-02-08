#!/bin/bash
#
# Incoming Patient Check Service - Installation Script
# This script installs the service and activates the cron job
#

set -e  # Stop on error

# =============================================================================
# CONFIGURATION - You can change these values as needed
# =============================================================================
INSTALL_DIR="/opt/incoming-patient-check"
CRON_FILE="/etc/cron.d/incoming-patient-check"  
LOG_FILE="/var/log/incoming-patient-check.log"
SERVICE_USER="root"
CRON_HOUR="2"
CRON_MINUTE="0"
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Log functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check required privileges
check_privileges() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run with root privileges"
        echo "Usage: sudo $0"
        exit 1
    fi
}

# Check required dependencies
check_dependencies() {
    log_info "Checking required dependencies..."
    
    # Python 3.7+ check
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 not found. Please install Python 3.7+"
        exit 1
    fi
    
    # Python version check (without bc)
    python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    python_major=$(python3 -c 'import sys; print(sys.version_info.major)')
    python_minor=$(python3 -c 'import sys; print(sys.version_info.minor)')
    
    if [[ $python_major -lt 3 ]] || [[ $python_major -eq 3 && $python_minor -lt 7 ]]; then
        log_error "Python 3.7+ required. Current version: $python_version"
        exit 1
    fi
    
    log_success "Python $python_version - OK"
    
    # pip3 check
    if ! command -v pip3 &> /dev/null; then
        log_error "pip3 not found. Please install python3-pip"
        exit 1
    fi
    
    log_success "pip3 - OK"
    
    # Cron service check (systemctl if available)
    if command -v systemctl &> /dev/null; then
        # Try both cron and crond services
        if systemctl is-active --quiet cron; then
            log_success "Cron service (cron) - OK"
        elif systemctl is-active --quiet crond; then
            log_success "Cron service (crond) - OK"
        else
            log_warning "Cron service not running, attempting to start..."
            # Try to start cron first, then crond
            if systemctl start cron 2>/dev/null; then
                systemctl enable cron
                log_success "Cron service (cron) started - OK"
            elif systemctl start crond 2>/dev/null; then
                systemctl enable crond
                log_success "Cron service (crond) started - OK"
            else
                log_error "Failed to start cron service. Please start manually."
                exit 1
            fi
        fi
    else
        # Old systems with service command
        if command -v service &> /dev/null; then
            if service cron status &> /dev/null || service crond status &> /dev/null; then
                log_success "Cron service (service) - OK"
            else
                log_warning "Cron service not running, attempting to start..."
                if service cron start 2>/dev/null || service crond start 2>/dev/null; then
                    log_success "Cron service started - OK"
                else
                    log_error "Failed to start cron service. Please start manually."
                    exit 1
                fi
            fi
        else
            log_warning "Cannot check cron service status. Please verify manually."
        fi
    fi
}

# Project installation
install_project() {
    local current_dir=$(pwd)
    
    log_info "Installing project to $INSTALL_DIR directory..."
    
    # Create target directory
    mkdir -p "$INSTALL_DIR"
    
    # Copy current files
    log_info "Copying files..."
    
    # Python files
    cp -r "$current_dir"/*.py "$INSTALL_DIR/" 2>/dev/null || true
    cp -r "$current_dir"/config "$INSTALL_DIR/" 2>/dev/null || true
    cp -r "$current_dir"/services "$INSTALL_DIR/" 2>/dev/null || true
    cp -r "$current_dir"/models "$INSTALL_DIR/" 2>/dev/null || true
    cp -r "$current_dir"/utils "$INSTALL_DIR/" 2>/dev/null || true
    
    # Config file check
    if [[ -f "$INSTALL_DIR/config/constants.py" ]]; then
        log_success "Configuration file copied: config/constants.py"
    else
        log_error "Configuration file not found!"
        return 1
    fi
    
    # Copy requirements file if exists
    if [[ -f "$current_dir/requirements.txt" ]]; then
        cp "$current_dir/requirements.txt" "$INSTALL_DIR/"
        log_success "requirements.txt copied"
    fi
    
    # Create logs directory
    mkdir -p "$INSTALL_DIR/logs"
    
    # Set ownership and permissions
    chown -R root:root "$INSTALL_DIR"
    chmod -R 755 "$INSTALL_DIR"
    chmod +x "$INSTALL_DIR"/*.py
    
    log_success "Project files installed: $INSTALL_DIR"
}

# Install Python dependencies
install_python_dependencies() {
    if [[ -f "$INSTALL_DIR/requirements.txt" ]]; then
        log_info "Installing Python dependencies..."
        cd "$INSTALL_DIR"
        
        # If you want to use virtual environment
        # python3 -m venv venv
        # source venv/bin/activate
        
        pip3 install -r requirements.txt
        log_success "Python dependencies installed"
    else
        log_warning "requirements.txt not found, dependencies must be installed manually"
    fi
}

# Cron job installation
install_cron_job() {
    log_info "Installing cron job..."
    
    # Detect Python executable path
    local python_path=$(which python3)
    if [[ -z "$python_path" ]]; then
        python_path="/usr/bin/python3"
        log_warning "Python path not detected, using default: $python_path"
    else
        log_success "Python path detected: $python_path"
    fi
    
    # Remove old cron job if exists
    if [[ -f "$CRON_FILE" ]]; then
        log_warning "Removing old cron job..."
        rm -f "$CRON_FILE"
    fi
    
    # Create new cron job
    cat > "$CRON_FILE" << EOF
# $CRON_FILE
# Incoming Patient Check Cron Service - Run daily at $CRON_HOUR:$CRON_MINUTE
# This service performs 2-phase operations:
# 1. Data collection and HIYS enrichment
# 2. Old data cleanup

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Environment variables
PYTHONDONTWRITEBYTECODE=1
PYTHONUNBUFFERED=1

# Run coordinator service daily at $CRON_HOUR:$CRON_MINUTE
$CRON_MINUTE $CRON_HOUR * * * $SERVICE_USER cd $INSTALL_DIR && $python_path cron_service.py >> $LOG_FILE 2>&1
EOF
    
    # Set cron file permissions
    chmod 644 "$CRON_FILE"
    
    # Create log file
    touch "$LOG_FILE"
    chmod 644 "$LOG_FILE"
    
    # Reload cron
    if command -v systemctl &> /dev/null; then
        systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || true
    else
        service cron reload 2>/dev/null || service crond reload 2>/dev/null || true
    fi
    
    log_success "Cron job installed: $CRON_FILE"
}

# Service test
test_service() {
    log_info "Testing service..."
    
    cd "$INSTALL_DIR"
    
    # Configuration test
    if [[ ! -f "config/constants.py" ]]; then
        log_error "Configuration file not found: config/constants.py"
        return 1
    else
        log_success "Configuration file - OK"
    fi
    
    # Python syntax check
    python3 -m py_compile cron_service.py
    log_success "cron_service.py syntax - OK"
    
    # Check all Python files syntax
    for py_file in $(find . -name "*.py" -type f); do
        if ! python3 -m py_compile "$py_file" 2>/dev/null; then
            log_error "Syntax error: $py_file"
            return 1
        fi
    done
    log_success "All Python files syntax - OK"
    
    # Data cleanup test
    if python3 services/data_cleanup_service.py --help > /dev/null 2>&1; then
        log_success "Data cleanup service - OK"
    else
        log_warning "Data cleanup service test failed (may be normal)"
    fi
    
    # Log file write test
    if touch "$LOG_FILE" 2>/dev/null; then
        log_success "Log file write permission - OK"
    else
        log_error "No write permission for log file: $LOG_FILE"
        return 1
    fi
    
    log_success "Service tests completed"
}

# Log rotation setup
install_logrotate() {
    log_info "Installing log rotation configuration..."
    
    local current_dir=$(pwd)
    local logrotate_file="/etc/logrotate.d/incoming-patient-check"
    
    if [[ -f "$current_dir/logrotate.d/incoming-patient-check" ]]; then
        cp "$current_dir/logrotate.d/incoming-patient-check" "$logrotate_file"
        chmod 644 "$logrotate_file"
        log_success "Log rotation configuration installed: $logrotate_file"
    else
        # Create manually
        cat > "$logrotate_file" << EOF
$LOG_FILE {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 root root
}
EOF
        chmod 644 "$logrotate_file"
        log_success "Log rotation configuration created: $logrotate_file"
    fi
}

# Installation summary
show_installation_summary() {
    echo
    log_success "🎉 Installation completed!"
    echo
    echo "📁 Installation directory: $INSTALL_DIR"
    echo "⏰ Cron schedule: Daily at $CRON_HOUR:$CRON_MINUTE"
    echo "📄 Cron file: $CRON_FILE"
    echo "📋 Log file: $LOG_FILE"
    echo
    echo "🔧 Configuration:"
    echo "   1. Edit $INSTALL_DIR/config/constants.py"
    echo "   2. Update database connection information (DEFAULT_DB_*)"
    echo "   3. Configure HIYS API settings (DEFAULT_HIYS_*)"
    echo "   4. Check WhatsApp settings (WHATSAPP_*)"
    echo
    echo "🔍 Test commands:"
    echo "   # Cron service test"
    echo "   cd $INSTALL_DIR && python3 cron_service.py"
    echo
    echo "   # Data cleanup dry run"
    echo "   cd $INSTALL_DIR && python3 services/data_cleanup_service.py --dry-run"
    echo
    echo "   # Log monitoring"
    echo "   tail -f $LOG_FILE"
    echo
    echo "🗑️  Uninstall: ./uninstall.sh"
    echo
}

# Main installation function
main() {
    echo "🚀 Incoming Patient Check Service - Installation Starting"
    echo "========================================================"
    
    check_privileges
    check_dependencies
    install_project
    install_python_dependencies
    install_cron_job
    install_logrotate
    test_service
    show_installation_summary
    
    log_success "Installation completed successfully! 🎉"
}

# Run the script
main "$@"