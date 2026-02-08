# Incoming Patient Check Service

Hastane veritabanlarından hasta bilgilerini otomatik olarak alıp işleyen bir servis sistemidir.

## Sistem Gereksinimleri

- **Python**: 3.7+
- **İşletim Sistemi**: Linux
- **Veritabanı**: MySQL/MariaDB
- **Bağımlılıklar**: requirements.txt dosyasında belirtilen paketler

## Kurulum

### Otomatik Kurulum
```bash
git clone https://github.com/brfkorucu/incoming-patient-check.git
cd incoming-patient-check
sudo ./install.sh
```

### Manuel Kurulum
```bash
# Bağımlılıkları yükle
pip3 install -r requirements.txt

# Konfigürasyonu düzenle
nano config/constants.py

# Cron job ekle
sudo cp logrotate.d/incoming-patient-check /etc/cron.d/
```

### Kaldırma
```bash
sudo ./uninstall.sh
```

## Proje Yapısı

```
incoming-patient-check/
├── config/
│   ├── __init__.py
│   └── constants.py          # Konfigürasyon sabitleri
├── logrotate.d/
│   └── incoming-patient-check # Log rotasyon konfigürasyonu
├── models/
│   ├── __init__.py
│   └── data_models.py        # Veri modelleri
├── services/
│   ├── __init__.py
│   ├── data_cleanup_service.py    # Veri temizleme servisi
│   ├── data_processor.py          # Ana veri işleme servisi
│   ├── database_save_service.py   # Veritabanı kaydetme
│   ├── database_service.py        # Veritabanı operasyonları
│   ├── hiys_api_service.py        # HIYS API entegrasyonu
│   └── whatsapp_service.py        # WhatsApp mesajlaşma
├── utils/
│   ├── __init__.py
│   └── logger.py             # Loglama utilities
├── cron_service.py           # Ana cron service koordinatörü
├── install.sh                # Kurulum scripti
├── uninstall.sh              # Kaldırma scripti
├── requirements.txt          # Python bağımlılıkları
├── LICENSE
└── README.md
```

## Özellikler

- Birden fazla hastane veritabanını destekler
- HIYS API entegrasyonu ile hasta verilerini zenginleştirir
- 90 günden eski verileri otomatik temizler
- Batch processing ile performans optimizasyonu
- Detaylı loglama sistemi
- WhatsApp entegrasyonu

## Kullanım

### Ana Servis
```bash
# Cron service çalıştır
python3 cron_service.py

# Test modu ile çalıştır
TEST_MODE=true python3 cron_service.py
```

### Veri Temizleme
```bash
# Dry run (sadece görüntüle)
python3 services/data_cleanup_service.py --dry-run

# Gerçek temizleme
python3 services/data_cleanup_service.py

# Belirli veritabanı için
python3 services/data_cleanup_service.py --db-name database_name
```

## Konfigürasyon

### Temel Ayarlar (config/constants.py)
```python
# Veritabanı
DEFAULT_DB_USER = 'cronrunner'
DEFAULT_DB_PASSWORD = 'your_password'
DEFAULT_DB_HOST = 'localhost'
DEFAULT_DB_PORT = 3306

# HIYS API
ENABLE_HIYS_ENRICHMENT_DEFAULT = True
HIYS_BATCH_SIZE = 10
DEFAULT_HIYS_REQUEST_DELAY = 0.2
DEFAULT_HIYS_TIMEOUT = 30

# Servis Kontrolü
ENABLE_DATA_PROCESSING_DEFAULT = True
ENABLE_DATA_CLEANUP_DEFAULT = True
TEST_MODE_DEFAULT = False
```

## Cron Zamanlaması

Servis her gün saat 02:00'da otomatik çalışır:
```bash
# /etc/cron.d/incoming-patient-check
0 2 * * * root cd /opt/incoming-patient-check && python3 cron_service.py >> /var/log/incoming-patient-check.log 2>&1
```

## Loglar

- Sistem logları: `/var/log/incoming-patient-check.log`
- Uygulama logları: `logs/cron_service.log`
- İşlenmiş veriler: `logs/processed_data_YYYYMMDD_HHMMSS.json`
- Zenginleştirilmiş veriler: `logs/final_enriched_data_YYYYMMDD_HHMMSS.json`

