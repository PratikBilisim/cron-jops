#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

INSTALL_DIR="/opt/mysql-backup-service"
CONFIG_DIR="/etc/mysql-backup"
ENV_DIR="/etc/mysql-backup/env"
LOG_DIR="/var/log/mysql-backup"
BACKUP_DIR="/var/backups/mysql"
BIN_PATH="/usr/local/bin/mysql-backup-service"
CRON_FILE="/etc/cron.d/mysql-backup"

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_root() {
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

check_dependencies() {
    print_info "Checking dependencies..."
    
    # Check if Python 3 is installed
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required but not installed"
        exit 1
    fi
    
    # Check if mysqldump is installed
    if ! command -v mysqldump &> /dev/null; then
        print_error "mysqldump is required but not installed. Please install MySQL client tools."
        exit 1
    fi
    
    # Check if cron is installed and running
    if ! command -v crontab &> /dev/null; then
        print_error "cron is required but not installed"
        exit 1
    fi
    
    print_success "All dependencies are satisfied"
}

create_directories() {
    print_info "Creating directories..."
    
    mkdir -p "$INSTALL_DIR"
    mkdir -p "$CONFIG_DIR"
    mkdir -p "$ENV_DIR"
    mkdir -p "$LOG_DIR"
    mkdir -p "$BACKUP_DIR"
    
    print_success "Directories created"
}

install_files() {
    print_info "Installing service files..."
    
    # Copy source files
    cp -r src/ "$INSTALL_DIR/"
    cp mysql-backup-service.py "$INSTALL_DIR/"
    
    # Copy configuration files
    cp config/config.json "$CONFIG_DIR/"
    
    # Copy all environment files for the databases
    if [ -f "config/cayirova.env" ]; then
        cp config/cayirova.env "$ENV_DIR/"
        print_success "Cayırova Pratik CRM env dosyası kopyalandı"
    fi
    
    if [ -f "config/dnz.env" ]; then
        cp config/dnz.env "$ENV_DIR/"
        print_success "DNZ Pratik CRM env dosyası kopyalandı"
    fi
    
    if [ -f "config/drm.env" ]; then
        cp config/drm.env "$ENV_DIR/"
        print_success "DRM Pratik CRM env dosyası kopyalandı"
    fi
    
    if [ -f "config/moodist.env" ]; then
        cp config/moodist.env "$ENV_DIR/"
        print_success "Moodist Pratik CRM env dosyası kopyalandı"
    fi
    
    # Create executable symlink
    ln -sf "$INSTALL_DIR/mysql-backup-service.py" "$BIN_PATH"
    chmod +x "$INSTALL_DIR/mysql-backup-service.py"
    
    # Fix line endings for cross-platform compatibility
    if command -v dos2unix &> /dev/null; then
        dos2unix "$INSTALL_DIR/mysql-backup-service.py" 2>/dev/null || true
    else
        # Alternative method to remove \r characters
        sed -i 's/\r$//' "$INSTALL_DIR/mysql-backup-service.py" 2>/dev/null || true
    fi
    
    print_success "Service files installed"
}

setup_permissions() {
    print_info "Setting up permissions..."
    
    # Set ownership and permissions
    chown -R root:root "$INSTALL_DIR"
    chown -R root:root "$CONFIG_DIR"
    chmod 755 "$INSTALL_DIR"
    chmod 755 "$CONFIG_DIR"
    chmod 700 "$ENV_DIR"  # Restrict access to env files (contain passwords)
    chmod 755 "$LOG_DIR"
    chmod 755 "$BACKUP_DIR"
    
    print_success "Permissions configured"
}

install_cron() {
    print_info "Installing cron job..."
    
    # Install cron file
    cp config/mysql-backup.cron "$CRON_FILE"
    chmod 644 "$CRON_FILE"
    
    # Restart cron service
    if systemctl is-active --quiet cron; then
        systemctl reload cron
    elif systemctl is-active --quiet crond; then
        systemctl reload crond
    else
        print_warning "Could not reload cron service. Please restart it manually."
    fi
    
    print_success "Cron job installed"
}

create_user_guide() {
    print_info "Creating user guide..."
    
    cat > "$CONFIG_DIR/SETUP_GUIDE.txt" << 'EOF'
MySQL Backup Service Setup Guide
=================================

1. Configure Database Connections:
   - Create .env files in /etc/mysql-backup/env/ directory
   - Each .env file should contain one database configuration
   - Use the example.env file as a template
   - File naming example: app1.env, app2.env, production.env

2. Example .env file content:
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=backup_user
   DB_PASSWORD=secure_password
   DB_NAME=database_name
   BACKUP_NAME=custom_backup_name

3. Test the service:
   - Run: mysql-backup-service status
   - Run: mysql-backup-service backup
   - Check logs in: /var/log/mysql-backup/

4. Backup Schedule:
   - Automatic backups run at 12:30 and 21:00 daily
   - Old backups are automatically cleaned up (3 day retention)

5. Manual Operations:
   - Create backup: mysql-backup-service backup
   - Check status: mysql-backup-service status  
   - Clean old backups: mysql-backup-service cleanup

6. Configuration:
   - Edit /etc/mysql-backup/config.json to modify settings
   - Restart cron after configuration changes

7. Logs:
   - Service logs: /var/log/mysql-backup/mysql-backup.log
   - Cron logs: /var/log/mysql-backup/cron.log

8. Backup Location:
   - Backups stored in: /var/backups/mysql/
   - Format: {backup_name}_{timestamp}.sql.gz
EOF

    print_success "User guide created at $CONFIG_DIR/SETUP_GUIDE.txt"
}

validate_installation() {
    print_info "Validating installation..."
    
    # Check if service command works
    if "$BIN_PATH" status --json > /dev/null 2>&1; then
        print_success "Service command validation passed"
    else
        print_warning "Service command validation failed. Check Python dependencies."
    fi
    
    # Check if cron job is properly installed
    if crontab -l 2>/dev/null | grep -q mysql-backup-service || [ -f "$CRON_FILE" ]; then
        print_success "Cron job validation passed"
    else
        print_warning "Cron job validation failed"
    fi
    
    # Check if environment files are properly installed
    env_count=$(find "$ENV_DIR" -name "*.env" -not -name "example.env" | wc -l)
    if [ $env_count -gt 0 ]; then
        print_success "Environment files found: $env_count database configurations"
        print_info "Environment files:"
        find "$ENV_DIR" -name "*.env" -not -name "example.env" | while read env_file; do
            db_name=$(grep "^BACKUP_NAME=" "$env_file" 2>/dev/null | cut -d'=' -f2 || echo "$(basename "$env_file" .env)")
            echo "  - $(basename "$env_file"): $db_name"
        done
    else
        print_warning "No database environment files found!"
        echo "Please configure your databases in: $ENV_DIR"
    fi
}

print_final_instructions() {
    echo
    echo "=========================================="
    echo "  MySQL Backup Service Installation Complete"
    echo "=========================================="
    echo
    
    # Count configured databases
    env_count=$(find "$ENV_DIR" -name "*.env" -not -name "example.env" 2>/dev/null | wc -l)
    
    if [ $env_count -gt 0 ]; then
        echo "✅ $env_count veritabanı yapılandırması kuruldu:"
        find "$ENV_DIR" -name "*.env" -not -name "example.env" 2>/dev/null | while read env_file; do
            db_name=$(grep "^BACKUP_NAME=" "$env_file" 2>/dev/null | cut -d'=' -f2 || echo "$(basename "$env_file" .env)")
            echo "   - $(basename "$env_file"): $db_name"
        done
        echo
        echo "🚀 Sistem kullanıma hazır!"
        echo
        echo "Hemen test edin:"
        echo "   mysql-backup-service status"
        echo "   mysql-backup-service backup"
        echo
    else
        echo "⚠️  Veritabanı yapılandırması bulunamadı!"
        echo
        echo "Next Steps:"
        echo "1. Configure your database connections:"
        echo "   - Edit files in: $ENV_DIR"
        echo "   - Use $ENV_DIR/example.env as template"
        echo
        echo "2. Test the service:"
        echo "   mysql-backup-service status"
        echo "   mysql-backup-service backup"
        echo
    fi
    
    echo "3. Check the setup guide:"
    echo "   cat $CONFIG_DIR/SETUP_GUIDE.txt"
    echo
    echo "4. Monitor logs:"
    echo "   tail -f $LOG_DIR/mysql-backup.log"
    echo
    echo "📅 Otomatik yedekleme zamanı:"
    echo "   - Sabah: 09:00"
    echo "   - Akşam: 21:00"
    echo "   - Saklama süresi: 3 gün"
    echo
    echo "=========================================="
}

uninstall() {
    print_info "Uninstalling MySQL Backup Service..."
    
    # Remove cron job
    rm -f "$CRON_FILE"
    
    # Remove symlink
    rm -f "$BIN_PATH"
    
    # Remove installation directory
    rm -rf "$INSTALL_DIR"
    
    # Note: We don't remove config, logs, or backups for safety
    print_warning "Configuration, logs, and backups preserved in:"
    print_warning "  - $CONFIG_DIR"
    print_warning "  - $LOG_DIR"
    print_warning "  - $BACKUP_DIR"
    
    print_success "Service uninstalled"
}

# Main installation function
main() {
    case "${1:-install}" in
        "install")
            print_info "Starting MySQL Backup Service installation..."
            check_root
            check_dependencies
            create_directories
            install_files
            setup_permissions
            install_cron
            create_user_guide
            validate_installation
            print_final_instructions
            ;;
        "uninstall")
            print_info "Starting MySQL Backup Service uninstallation..."
            check_root
            uninstall
            ;;
        "update")
            print_info "Updating MySQL Backup Service..."
            check_root
            install_files
            setup_permissions
            print_success "Service updated"
            ;;
        *)
            echo "Usage: $0 {install|uninstall|update}"
            echo "  install   - Install the service (default)"
            echo "  uninstall - Remove the service"
            echo "  update    - Update service files only"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"