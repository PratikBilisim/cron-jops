# MySQL Backup Service

Linux sunucularında MySQL veritabanlarının otomatik yedekleri.

## Özellikler

- Günde 2 kez otomatik yedek (09:00 ve 21:00)
- Çoklu veritabanı desteği (.env dosyalarından)
- 3 günlük saklama ile otomatik temizlik
- SQL dump dosyaları (.sql)
- Kapsamlı loglama

## Kurulum

```bash
sudo ./install.sh
```

## Kullanım

```bash
mysql-backup-service status
mysql-backup-service backup
mysql-backup-service cleanup
```

## Yapılandırma

Environment dosyaları `/etc/mysql-backup/env/` dizininde:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=kullanici_adi
DB_PASSWORD=sifre
DB_NAME=veritabani_adi
BACKUP_NAME=yedek_adi
```

## Dizinler

- Yapılandırma: `/etc/mysql-backup/`
- Yedekler: `/var/backups/mysql/`
- Loglar: `/var/log/mysql-backup/`
