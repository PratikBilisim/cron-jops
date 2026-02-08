#!/bin/bash
# MySQL Backup Service - Sunucu Hazırlık ve Kontrol Scripti
# Bu script sunucunuzun MySQL backup servisi için hazır olup olmadığını kontrol eder

set -e

# Renkler
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Fonksiyonlar
print_info() {
    echo -e "${BLUE}[BİLGİ]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[BAŞARILI]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[UYARI]${NC} $1"
}

print_error() {
    echo -e "${RED}[HATA]${NC} $1"
}

print_header() {
    echo "=================================================="
    echo "  MySQL Backup Service - Sunucu Hazırlık Kontrolü"
    echo "=================================================="
    echo
}

check_system_info() {
    print_info "Sistem bilgileri kontrol ediliyor..."
    
    echo "İşletim Sistemi: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)"
    echo "Kernel Sürümü: $(uname -r)"
    echo "Sistem Zamanı: $(date)"
    echo "Disk Kullanımı:"
    df -h | grep -E '(Filesystem|/dev/)'
    echo
}

check_mysql_service() {
    print_info "MySQL servisi kontrol ediliyor..."
    
    if systemctl is-active --quiet mysql || systemctl is-active --quiet mysqld; then
        print_success "MySQL servisi çalışıyor"
        
        # MySQL versiyonu
        if command -v mysql &> /dev/null; then
            MYSQL_VERSION=$(mysql --version | head -1)
            echo "MySQL Versiyonu: $MYSQL_VERSION"
        fi
        
        # MySQL durumu
        if systemctl is-active --quiet mysql; then
            systemctl status mysql --no-pager -l
        elif systemctl is-active --quiet mysqld; then
            systemctl status mysqld --no-pager -l
        fi
        
    else
        print_error "MySQL servisi çalışmıyor!"
        echo "MySQL servisini başlatmak için:"
        echo "  sudo systemctl start mysql"
        echo "  # veya"
        echo "  sudo systemctl start mysqld"
        return 1
    fi
    echo
}

check_mysql_client() {
    print_info "MySQL client araçları kontrol ediliyor..."
    
    if command -v mysql &> /dev/null; then
        print_success "mysql komutu mevcut"
    else
        print_error "mysql komutu bulunamadı!"
        echo "MySQL client kurmak için:"
        echo "  # Ubuntu/Debian:"
        echo "  sudo apt-get install mysql-client"
        echo "  # CentOS/RHEL:"
        echo "  sudo yum install mysql"
        return 1
    fi
    
    if command -v mysqldump &> /dev/null; then
        print_success "mysqldump komutu mevcut"
        MYSQLDUMP_VERSION=$(mysqldump --version | head -1)
        echo "mysqldump Versiyonu: $MYSQLDUMP_VERSION"
    else
        print_error "mysqldump komutu bulunamadı!"
        echo "mysqldump gerekli! MySQL client paketini yükleyin."
        return 1
    fi
    echo
}

test_database_connections() {
    print_info "Veritabanı bağlantıları test ediliyor..."
    
    # Test veritabanları listesi
    declare -A databases=(
        ["cayirova"]="cayirovapratikcr:pratik-zxc025*:cayirovapratikcr_db"
        ["dnz"]="dnzpratikcrm:pratik-zxc025*:dnzpratikcrm_db"
        ["drm"]="drmpratikcrm:pratik-zxc025*:drmpratikcrm_db"
        ["moodist"]="moodistpratikcrm:pratik-zxc025*:moodistpratikcrm_db"
    )
    
    success_count=0
    total_count=${#databases[@]}
    
    for db_name in "${!databases[@]}"; do
        IFS=':' read -r user password database <<< "${databases[$db_name]}"
        
        print_info "Test ediliyor: $db_name ($database)"
        
        # Bağlantı testi
        connection_result=$(mysql -h localhost -u "$user" -p"$password" -e "USE $database; SELECT 1;" 2>&1)
        
        if [ $? -eq 0 ]; then
            print_success "$db_name bağlantısı başarılı"
            
            # Tablo sayısını kontrol et
            table_count=$(mysql -h localhost -u "$user" -p"$password" -e "USE $database; SHOW TABLES;" 2>/dev/null | wc -l)
            echo "  Tablo sayısı: $((table_count-1))"
            
            # Veritabanı boyutunu kontrol et
            db_size=$(mysql -h localhost -u "$user" -p"$password" -e "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) AS 'DB Size in MB' FROM information_schema.tables WHERE table_schema='$database';" 2>/dev/null | tail -1)
            echo "  Veritabanı boyutu: ${db_size} MB"
            
            ((success_count++))
        else
            print_error "$db_name bağlantısı başarısız!"
            echo "  Kullanıcı: $user"
            echo "  Veritabanı: $database"
            echo "  Hata detayı: $connection_result"
            echo "  Manuel test: mysql -h localhost -u $user -p'$password' $database"
        fi
        echo
    done
    
    echo "Bağlantı test sonucu: $success_count/$total_count başarılı"
    
    if [ $success_count -eq $total_count ]; then
        print_success "Tüm veritabanı bağlantıları başarılı!"
    else
        print_warning "Bazı veritabanı bağlantıları başarısız!"
        return 1
    fi
    echo
}

test_mysqldump() {
    print_info "mysqldump test ediliyor..."
    
    # Test için ilk veritabanını kullan
    if mysql -h localhost -u cayirovapratikcr -p'pratik-zxc025*' -e "USE cayirovapratikcr_db; SELECT 1;" &> /dev/null; then
        
        print_info "Test mysqldump komutu çalıştırılıyor..."
        
        # Geçici test dosyası
        test_backup="/tmp/test_backup_$(date +%s).sql"
        
        # mysqldump testini en basit şekilde yapalım
        print_info "mysqldump testi başlatılıyor..."
        
        if mysqldump --host=localhost --port=3306 --user=cayirovapratikcr --password='pratik-zxc025*' \
                     --single-transaction --lock-tables=false \
                     --add-drop-database --databases cayirovapratikcr_db > "$test_backup"; then
            
            # Dosya oluştu mu ve içinde veri var mı?
            if [ -f "$test_backup" ] && [ -s "$test_backup" ]; then
                backup_size=$(du -h "$test_backup" | cut -f1)
                line_count=$(wc -l < "$test_backup")
                print_success "Test backup başarılı! (Boyut: $backup_size, Satır: $line_count)"
                rm -f "$test_backup"
            else
                print_error "Backup dosyası oluşmadı veya boş!"
                rm -f "$test_backup"
                return 1
            fi
            
        else
            exit_code=$?
            print_error "Test backup başarısız! (Exit code: $exit_code)"
            
            # Gerçek hata mesajını stderr'dan alalım
            echo "Gerçek hata detayları:"
            mysqldump --host=localhost --port=3306 --user=cayirovapratikcr --password='pratik-zxc025*' \
                     --single-transaction --triggers --events --lock-tables=false \
                     --add-drop-database --databases cayirovapratikcr_db >/dev/null
            
            echo
            echo "Olası nedenler:"
            echo "1. Kullanıcı adı veya şifre yanlış"
            echo "2. Veritabanı mevcut değil"  
            echo "3. Kullanıcının backup yetkisi yok"
            echo "4. MySQL server bağlantı sorunu"
            echo "5. Timeout sorunu"
            echo
            echo "Manuel test için:"
            echo "mysqldump --host=localhost --user=cayirovapratikcr --password='pratik-zxc025*' --databases cayirovapratikcr_db > test.sql"
            
            rm -f "$test_backup"
            return 1
        fi
    else
        print_warning "Test için veritabanı bağlantısı kurulamadı"
        print_info "Veritabanı bağlantısı test ediliyor..."
        
        # Bağlantı hatasının detayını göster
        connection_error=$(mysql -h localhost -u cayirovapratikcr -p'pratik-zxc025*' -e "USE cayirovapratikcr_db; SELECT 1;" 2>&1)
        echo "Bağlantı hatası detayları:"
        echo "$connection_error"
        echo
        echo "mysqldump testi atlanıyor"
    fi
    echo
}

check_disk_space() {
    print_info "Disk alanı kontrol ediliyor..."
    
    # Root partition kontrolü
    root_usage=$(df / | awk 'NR==2 {print $5}' | sed 's/%//')
    root_available=$(df -h / | awk 'NR==2 {print $4}')
    
    echo "Root (/) partition kullanımı: ${root_usage}%"
    echo "Kullanılabilir alan: $root_available"
    
    if [ $root_usage -gt 90 ]; then
        print_error "Disk alanı kritik seviyede! (%${root_usage})"
        return 1
    elif [ $root_usage -gt 80 ]; then
        print_warning "Disk alanı düşük! (%${root_usage})"
    else
        print_success "Disk alanı yeterli (%${root_usage})"
    fi
    
    # /var kontrolü (backup dizini için)
    if mountpoint -q /var; then
        var_usage=$(df /var | awk 'NR==2 {print $5}' | sed 's/%//')
        var_available=$(df -h /var | awk 'NR==2 {print $4}')
        
        echo "/var partition kullanımı: ${var_usage}%"
        echo "Kullanılabilir alan: $var_available"
        
        if [ $var_usage -gt 85 ]; then
            print_warning "/var partition alanı düşük! (%${var_usage})"
        fi
    fi
    echo
}

check_python() {
    print_info "Python kontrol ediliyor..."
    
    if command -v python3 &> /dev/null; then
        python_version=$(python3 --version)
        print_success "Python3 mevcut: $python_version"
        
        # Python versiyonu kontrolü
        python_ver=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
        if python3 -c "import sys; exit(0 if sys.version_info >= (3, 6) else 1)"; then
            print_success "Python versiyonu uygun (≥3.6)"
        else
            print_error "Python versiyonu çok eski! Python 3.6+ gerekli (mevcut: $python_ver)"
            return 1
        fi
    else
        print_error "Python3 bulunamadı!"
        echo "Python3 kurmak için:"
        echo "  # Ubuntu/Debian:"
        echo "  sudo apt-get install python3"
        echo "  # CentOS/RHEL:"
        echo "  sudo yum install python3"
        return 1
    fi
    echo
}

check_cron() {
    print_info "Cron servisi kontrol ediliyor..."
    
    if systemctl is-active --quiet cron || systemctl is-active --quiet crond; then
        print_success "Cron servisi çalışıyor"
        
        if systemctl is-active --quiet cron; then
            systemctl status cron --no-pager -l | head -5
        elif systemctl is-active --quiet crond; then
            systemctl status crond --no-pager -l | head -5
        fi
    else
        print_error "Cron servisi çalışmıyor!"
        echo "Cron servisini başlatmak için:"
        echo "  sudo systemctl start cron"
        echo "  # veya"
        echo "  sudo systemctl start crond"
        return 1
    fi
    echo
}

check_permissions() {
    print_info "Dizin izinleri kontrol ediliyor..."
    
    directories=(
        "/etc"
        "/var/log"
        "/var/backups"
        "/opt"
        "/usr/local/bin"
    )
    
    for dir in "${directories[@]}"; do
        if [ -d "$dir" ] && [ -w "$dir" ]; then
            print_success "$dir yazılabilir"
        elif [ -d "$dir" ]; then
            print_warning "$dir yazma izni yok (root gerekebilir)"
        else
            print_warning "$dir mevcut değil"
        fi
    done
    echo
}

generate_recommendations() {
    print_info "Öneriler oluşturuluyor..."
    
    echo "=== SİSTEM ÖNERİLERİ ==="
    echo
    echo "1. BACKUP DİZİNİ:"
    echo "   mkdir -p /var/backups/mysql"
    echo "   chmod 755 /var/backups/mysql"
    echo
    echo "2. LOG DİZİNİ:"
    echo "   mkdir -p /var/log/mysql-backup"
    echo "   chmod 755 /var/log/mysql-backup"
    echo
    echo "3. LOG ROTATION (opsiyonel):"
    echo "   cat > /etc/logrotate.d/mysql-backup << 'EOF'"
    echo "   /var/log/mysql-backup/*.log {"
    echo "       daily"
    echo "       missingok"
    echo "       rotate 30"
    echo "       compress"
    echo "       notifempty"
    echo "       create 644 root root"
    echo "   }"
    echo "   EOF"
    echo
    echo "4. MySQL BACKUP KULLANICISI (güvenlik için):"
    echo "   mysql -u root -p << 'EOF'"
    echo "   CREATE USER 'backup_user'@'localhost' IDENTIFIED BY 'güçlü_şifre';"
    echo "   GRANT SELECT, LOCK TABLES, SHOW VIEW, TRIGGER, RELOAD ON *.* TO 'backup_user'@'localhost';"
    echo "   FLUSH PRIVILEGES;"
    echo "   EOF"
    echo
    echo "5. FİREWALL (eğer uzak backup alınacaksa):"
    echo "   ufw allow from trusted_ip to any port 3306"
    echo
    echo "6. MONİTÖRİNG:"
    echo "   # Backup başarısızlık bildirimi için webhook veya email kurabilirsiniz"
    echo
}

create_test_script() {
    print_info "Test scripti oluşturuluyor..."
    
    cat > /tmp/mysql_backup_test.sh << 'EOF'
#!/bin/bash
# MySQL Backup Test Script

echo "=== MYSQL BACKUP SERVİSİ TEST ==="
echo

# Test 1: Tüm veritabanlarına bağlantı
echo "1. Veritabanı bağlantı testi..."
databases=("cayirovapratikcr:pratik-zxc025*:cayirovapratikcr_db" 
           "dnzpratikcrm:pratik-zxc025*:dnzpratikcrm_db"
           "drmpratikcrm:pratik-zxc025*:drmpratikcrm_db"
           "moodistpratikcrm:pratik-zxc025*:moodistpratikcrm_db")

for db_info in "${databases[@]}"; do
    IFS=':' read -r user password database <<< "$db_info"
    if mysql -h localhost -u "$user" -p"$password" -e "SELECT 1;" &>/dev/null; then
        echo "  ✓ $database bağlantısı başarılı"
    else
        echo "  ✗ $database bağlantısı başarısız"
    fi
done

echo
echo "2. mysqldump testi..."
# Test backup
if mysqldump --host=localhost --user=cayirovapratikcr --password='pratik-zxc025*' \
             --single-transaction cayirovapratikcr_db > /tmp/test.sql 2>/dev/null; then
    echo "  ✓ mysqldump çalışıyor"
    echo "  Backup boyutu: $(ls -lh /tmp/test.sql | awk '{print $5}')"
    rm -f /tmp/test.sql
else
    echo "  ✗ mysqldump başarısız"
fi

echo
echo "Test tamamlandı!"
EOF

    chmod +x /tmp/mysql_backup_test.sh
    print_success "Test scripti oluşturuldu: /tmp/mysql_backup_test.sh"
    echo "Çalıştırmak için: bash /tmp/mysql_backup_test.sh"
    echo
}

# Ana kontrol fonksiyonu
main() {
    print_header
    
    local overall_status=0
    
    check_system_info || ((overall_status++))
    check_python || ((overall_status++))
    check_mysql_service || ((overall_status++))
    check_mysql_client || ((overall_status++))
    test_database_connections || ((overall_status++))
    test_mysqldump || ((overall_status++))
    check_disk_space || ((overall_status++))
    check_cron || ((overall_status++))
    check_permissions || ((overall_status++))
    
    echo "=================================================="
    echo "  KONTROL SONUCU"
    echo "=================================================="
    
    if [ $overall_status -eq 0 ]; then
        print_success "Sunucunuz MySQL Backup Service için hazır!"
        echo
        echo "Kurulum için çalıştırın:"
        echo "  sudo ./install.sh"
    else
        print_warning "Bazı sorunlar tespit edildi ($overall_status hata)"
        echo
        echo "Sorunları çözdükten sonra tekrar test edin:"
        echo "  bash $0"
    fi
    
    echo
    generate_recommendations
    create_test_script
    
    return $overall_status
}

# Parametreler
case "${1:-check}" in
    "check"|"")
        main
        ;;
    "test")
        test_database_connections
        test_mysqldump
        ;;
    "quick")
        check_mysql_service
        check_mysql_client
        test_database_connections
        ;;
    *)
        echo "Kullanım: $0 [check|test|quick]"
        echo "  check - Tam sistem kontrolü (varsayılan)"
        echo "  test  - Sadece veritabanı testleri"
        echo "  quick - Hızlı kontrol"
        ;;
esac