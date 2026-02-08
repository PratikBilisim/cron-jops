#!/bin/bash
# MySQL Backup Debug Script - Sorun tespiti için detaylı kontrol

set -e

echo "=== MySQL BACKUP DEBUG ==="
echo "Tarih: $(date)"
echo

# MySQL servis kontrolü
echo "1. MySQL Servis Durumu:"
if systemctl is-active --quiet mysql; then
    echo "✓ mysql servisi aktif"
    systemctl status mysql --no-pager | head -3
elif systemctl is-active --quiet mysqld; then
    echo "✓ mysqld servisi aktif"
    systemctl status mysqld --no-pager | head -3
else
    echo "✗ MySQL servisi çalışmıyor!"
fi
echo

# MySQL client kontrol
echo "2. MySQL Client Kontrol:"
if command -v mysql &> /dev/null; then
    echo "✓ mysql komutu mevcut"
    mysql --version
else
    echo "✗ mysql komutu bulunamadı!"
fi

if command -v mysqldump &> /dev/null; then
    echo "✓ mysqldump komutu mevcut"
    mysqldump --version
else
    echo "✗ mysqldump komutu bulunamadı!"
fi
echo

# Veritabanı bağlantı testleri
echo "3. Detaylı Veritabanı Bağlantı Testleri:"
echo

databases=(
    "cayirovapratikcr:pratik-zxc025*:cayirovapratikcr_db"
    "dnzpratikcrm:pratik-zxc025*:dnzpratikcrm_db"
    "drmpratikcrm:pratik-zxc025*:drmpratikcrm_db"
    "moodistpratikcrm:pratik-zxc025*:moodistpratikcrm_db"
)

for db_info in "${databases[@]}"; do
    IFS=':' read -r user password database <<< "$db_info"
    
    echo "--- Test: $database ---"
    echo "Kullanıcı: $user"
    echo "Veritabanı: $database"
    
    # Temel bağlantı testi
    echo -n "Bağlantı testi: "
    if timeout 10 mysql -h localhost -u "$user" -p"$password" -e "SELECT 1;" 2>/dev/null >/dev/null; then
        echo "✓ BAŞARILI"
        
        # Veritabanı varlığı
        echo -n "Veritabanı varlığı: "
        if timeout 10 mysql -h localhost -u "$user" -p"$password" -e "USE $database; SELECT 1;" 2>/dev/null >/dev/null; then
            echo "✓ MEVCUT"
            
            # Tablolar
            table_count=$(timeout 10 mysql -h localhost -u "$user" -p"$password" -e "USE $database; SHOW TABLES;" 2>/dev/null | tail -n +2 | wc -l)
            echo "Tablo sayısı: $table_count"
            
            # Boyut
            size_query="SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb FROM information_schema.tables WHERE table_schema='$database';"
            db_size=$(timeout 10 mysql -h localhost -u "$user" -p"$password" -e "$size_query" 2>/dev/null | tail -1)
            echo "Veritabanı boyutu: ${db_size} MB"
            
            # mysqldump testi
            echo -n "mysqldump testi: "
            test_file="/tmp/debug_backup_${database}_$(date +%s).sql"
            
            if timeout 30 mysqldump --host=localhost --port=3306 --user="$user" --password="$password" \
                --single-transaction --triggers --events --lock-tables=false \
                --add-drop-database --databases "$database" > "$test_file" 2>/dev/null; then
                
                if [ -f "$test_file" ] && [ -s "$test_file" ]; then
                    backup_size=$(ls -lh "$test_file" | awk '{print $5}')
                    echo "✓ BAŞARILI (Boyut: $backup_size)"
                    rm -f "$test_file"
                else
                    echo "✗ BAŞARISIZ (Boş dosya)"
                    rm -f "$test_file"
                fi
            else
                echo "✗ BAŞARISIZ (Komut hatası)"
                rm -f "$test_file"
            fi
            
        else
            echo "✗ MEVCUT DEĞİL"
            echo "Hata:"
            timeout 10 mysql -h localhost -u "$user" -p"$password" -e "USE $database;" 2>&1 | head -3
        fi
        
    else
        echo "✗ BAŞARISIZ"
        echo "Hata:"
        timeout 10 mysql -h localhost -u "$user" -p"$password" -e "SELECT 1;" 2>&1 | head -3
    fi
    
    echo
done

# Sistem kaynakları
echo "4. Sistem Kaynakları:"
echo "Disk kullanımı:"
df -h | grep -E '(Filesystem|/$|/var)'
echo
echo "Bellek kullanımı:"
free -h
echo

# MySQL konfigürasyonu
echo "5. MySQL Konfigürasyon:"
if command -v mysql &> /dev/null; then
    echo "MySQL değişkenleri (önemli olanlar):"
    mysql -e "SHOW VARIABLES LIKE 'max_connections';" 2>/dev/null || echo "MySQL konfigürasyonuna erişim yok"
    mysql -e "SHOW VARIABLES LIKE 'innodb_buffer_pool_size';" 2>/dev/null || true
    mysql -e "SHOW VARIABLES LIKE 'wait_timeout';" 2>/dev/null || true
fi
echo

# Ağ bağlantısı
echo "6. MySQL Port Kontrolü:"
if ss -tlnp | grep -q :3306; then
    echo "✓ MySQL portu (3306) dinleniyor"
    ss -tlnp | grep :3306
else
    echo "✗ MySQL portu (3306) dinlenmiyor"
fi
echo

echo "=== DEBUG TAMAMLANDI ==="
echo "Bu çıktıyı paylaşarak sorunu daha iyi analiz edebiliriz."