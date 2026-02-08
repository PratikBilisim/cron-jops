#!/bin/bash
#
# Incoming Patient Check Service - Uninstallation Script
# This script completely removes the service from the system
#

set -e  # Stop on error

# =============================================================================
# CONFIGURATION - Should match install.sh values
# =============================================================================
INSTALL_DIR="/opt/incoming-patient-check"
CRON_FILE="/etc/cron.d/incoming-patient-check"  
LOG_FILE="/var/log/incoming-patient-check.log"
SERVICE_USER="root"
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

# Get confirmation
confirm_uninstall() {
    echo "⚠️  Incoming Patient Check Service will be removed:"
    echo "   - Cron job will be deleted"
    echo "   - $INSTALL_DIR directory will be deleted"
    echo "   - Log files will be preserved"
    echo
    
    read -p "Are you sure you want to continue? (y/N): " -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Uninstallation cancelled"
        exit 0
    fi
}

# Stop running services
stop_running_services() {
    log_info "Checking for running services..."
    
    # Find and stop cron job processes - more comprehensive search
    local pids=$(pgrep -f "cron_service.py|incoming-patient-check" || true)
    
    if [[ -n "$pids" ]]; then
        log_warning "Stopping running service processes..."
        echo "$pids" | xargs kill -TERM 2>/dev/null || true
        sleep 3
        
        # Force kill if still running
        pids=$(pgrep -f "cron_service.py|incoming-patient-check" || true)
        if [[ -n "$pids" ]]; then
            echo "$pids" | xargs kill -9 2>/dev/null || true
            log_warning "Some processes were force killed"
        fi
        
        log_success "Running processes stopped"
    else
        log_success "No running processes found"
    fi
}

# Remove cron job
remove_cron_job() {
    log_info "Removing cron job..."
    
    if [[ -f "$CRON_FILE" ]]; then
        rm -f "$CRON_FILE"
        log_success "Cron job deleted: $CRON_FILE"
    else
        log_warning "Cron job file not found: $CRON_FILE"
    fi
    
    # Reload cron
    if command -v systemctl &> /dev/null; then
        systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || true
    else
        service cron reload 2>/dev/null || service crond reload 2>/dev/null || true
    fi
    log_success "Cron service reloaded"
}

# Remove project directory
remove_project_directory() {
    log_info "Removing project directory: $INSTALL_DIR"
    
    if [[ -d "$INSTALL_DIR" ]]; then
        # Backup important files
        local backup_dir="/tmp/incoming-patient-check-backup-$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$backup_dir"
        
        # Backup config files
        if [[ -f "$INSTALL_DIR/config/constants.py" ]]; then
            log_info "Backing up configuration files: $backup_dir"
            cp -r "$INSTALL_DIR/config" "$backup_dir/" 2>/dev/null || true
        fi
        
        # Backup logs if exists
        if [[ -d "$INSTALL_DIR/logs" ]]; then
            cp -r "$INSTALL_DIR/logs" "$backup_dir/" 2>/dev/null || true
        fi
        
        # Remove directory
        rm -rf "$INSTALL_DIR"
        log_success "Project directory deleted: $INSTALL_DIR"
        
        if [[ -d "$backup_dir" ]] && [[ -n "$(ls -A "$backup_dir" 2>/dev/null)" ]]; then
            log_success "Backups saved: $backup_dir"
        fi
    else
        log_warning "Project directory not found: $INSTALL_DIR"
    fi
}

# Remove Python packages (optional)
remove_python_packages() {
    log_info "Checking Python packages..."
    
    # We don't automatically remove globally installed packages
    # because other projects might be using them
    
    log_warning "Python packages not automatically removed"
    log_info "You can manually remove them if needed:"
    echo "   pip3 uninstall mysql-connector-python python-dotenv requests"
}

# Clean up logs (optional)
cleanup_logs() {
    log_info "Checking log files..."
    
    if [[ -f "$LOG_FILE" ]]; then
        read -p "Do you want to delete the log file as well? ($LOG_FILE) (y/N): " -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -f "$LOG_FILE"
            log_success "Log file deleted: $LOG_FILE"
        else
            log_info "Log file preserved: $LOG_FILE"
        fi
    else
        log_info "Log file not found: $LOG_FILE"
    fi
}

# System cleanup
system_cleanup() {
    log_info "Performing system cleanup..."
    
    # Clean temporary files
    rm -rf /tmp/incoming-patient-check* 2>/dev/null || true
    
    # Cache cleanup
    rm -rf /root/.cache/pip/incoming-patient-check* 2>/dev/null || true
    
    # Remove logrotate configuration
    if [[ -f "/etc/logrotate.d/incoming-patient-check" ]]; then
        rm -f "/etc/logrotate.d/incoming-patient-check"
        log_success "Logrotate configuration removed"
    fi
    
    log_success "System cleanup completed"
}

# Verify removal
verify_removal() {
    log_info "Verifying removal..."
    
    local issues=0
    
    # Check cron job
    if [[ -f "$CRON_FILE" ]]; then
        log_error "Cron job still exists!"
        ((issues++))
    fi
    
    # Check project directory
    if [[ -d "$INSTALL_DIR" ]]; then
        log_error "Project directory still exists!"
        ((issues++))
    fi
    
    # Check running processes
    if pgrep -f "cron_service.py" >/dev/null 2>&1; then
        log_error "Service still running!"
        ((issues++))
    fi
    
    if [[ $issues -eq 0 ]]; then
        log_success "Removal verified - clean!"
    else
        log_error "$issues issues detected in removal"
        return 1
    fi
}

# Removal summary
show_removal_summary() {
    echo
    log_success "🗑️  Removal completed!"
    echo
    echo "✅ Removed items:"
    echo "   ✓ Cron job: $CRON_FILE"
    echo "   ✓ Project directory: $INSTALL_DIR"
    echo "   ✓ Running processes"
    echo "   ✓ Logrotate configuration"
    echo
    echo "📁 Preserved items:"
    echo "   • Log file: $LOG_FILE (optional)"
    echo "   • Config backups: /tmp/incoming-patient-check-backup-*"
    echo "   • Python packages (global)"
    echo
    echo "🧹 Manual cleanup (if needed):"
    echo "   # Delete remaining log files"
    echo "   sudo rm -f $LOG_FILE*"
    echo
    echo "   # Remove Python packages"
    echo "   pip3 uninstall mysql-connector-python python-dotenv requests"
    echo
    echo "   # Delete config backups"
    echo "   sudo rm -rf /tmp/incoming-patient-check-backup-*"
    echo
}

# Main removal function
main() {
    echo "🗑️  Incoming Patient Check Service - Removal Starting"
    echo "===================================================="
    
    check_privileges
    confirm_uninstall
    stop_running_services
    remove_cron_job
    remove_project_directory
    remove_python_packages
    cleanup_logs
    system_cleanup
    verify_removal
    show_removal_summary
    
    log_success "Removal completed successfully! 🎉"
}

# Run the script
main "$@"