"""Data Cleanup Service"""

import json
import logging
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys
import os
from config.constants import (
    CLEANUP_DAYS, TEST_CLEANUP_DAYS_DEFAULT, TEST_MODE_DEFAULT,
    DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_CHARSET, DEFAULT_DB_COLLATION,
    MYSQL_ACCESS_DENIED, MYSQL_CANNOT_CONNECT, MYSQL_UNKNOWN_DATABASE,
    DEFAULT_DB_USER, DEFAULT_DB_PASSWORD, HOSPITAL_CONFIGS
)

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataCleanupService:
    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger(__name__)

        self.db_config = {
            'user': DEFAULT_DB_USER,
            'password': DEFAULT_DB_PASSWORD,
            'host': DEFAULT_DB_HOST,
            'port': DEFAULT_DB_PORT,
            'charset': DEFAULT_DB_CHARSET,
            'collation': DEFAULT_DB_COLLATION
        }

        test_mode = TEST_MODE_DEFAULT
        if test_mode:
            cleanup_days = TEST_CLEANUP_DAYS_DEFAULT
            self.logger.warning(
                f"TEST MODE: Using {cleanup_days} days cutoff instead of {CLEANUP_DAYS} days")
        else:
            cleanup_days = CLEANUP_DAYS

        self.cutoff_date = datetime.now() - timedelta(days=cleanup_days)
        self.logger.info(
            f"Data cleanup cutoff date: {self.cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")

        if test_mode:
            self.logger.warning(
                "TEST MODE ACTIVE - Data cleanup will work on limited dataset!")
        else:
            self.logger.info("Production mode - Full cleanup operation")

    def _get_hospitals_from_config(self) -> List[str]:
        try:
            hospitals = HOSPITAL_CONFIGS

            db_names = [hospital['dbName']
                        for hospital in hospitals if 'dbName' in hospital]

            if not db_names:
                self.logger.warning(
                    "No database names found in hospital configs, using default")
                return ['vatanpratikcrm_db']

            self.logger.info(
                f"Found {len(db_names)} databases in config: {', '.join(db_names)}")
            return db_names

        except Exception as e:
            self.logger.error(f"Error reading hospital configs: {e}")
            return ['vatanpratikcrm_db']

    def _get_db_connection(self, db_name: str):
        try:
            config = self.db_config.copy()
            config['database'] = db_name

            if not config['user'] or not config['password']:
                raise ValueError(
                    f"Database credentials not configured properly. User: {bool(config['user'])}, Password: {bool(config['password'])}")

            connection = mysql.connector.connect(**config)

            if not connection.is_connected():
                raise mysql.connector.Error(
                    "Failed to establish database connection")

            self.logger.debug(f"Successfully connected to database: {db_name}")
            return connection

        except mysql.connector.Error as e:
            error_code = e.errno if hasattr(e, 'errno') else 'Unknown'
            if error_code == MYSQL_ACCESS_DENIED:  # Access denied
                self.logger.error(
                    f"Access denied for database {db_name}. Check credentials.")
            elif error_code == MYSQL_CANNOT_CONNECT:  # Can't connect
                self.logger.error(
                    f"Cannot connect to database server for {db_name}. Check host and port.")
            elif error_code == MYSQL_UNKNOWN_DATABASE:  # Unknown database
                self.logger.error(f"Database {db_name} does not exist.")
            else:
                self.logger.error(
                    f"MySQL error connecting to {db_name}: [{error_code}] {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Configuration error: {e}")
            raise
        except Exception as e:
            self.logger.error(
                f"Unexpected error connecting to database {db_name}: {e}")
            raise
            config['database'] = db_name

            connection = mysql.connector.connect(**config)
            self.logger.debug(f"Database connection established: {db_name}")
            return connection
        except Error as e:
            self.logger.error(f"Veritabanı bağlantı hatası: {e}")
            raise

    def cleanup_all_databases(self) -> Dict[str, Any]:
        db_names = self._get_hospitals_from_config()

        total_stats = {
            'deleted_records': 0,
            'cleaned_records': 0,
            'cleaned_transactions': 0,
            'processed_databases': 0,
            'failed_databases': 0
        }

        db_results = {}

        self.logger.info(f"Starting cleanup for {len(db_names)} databases...")

        for db_name in db_names:
            try:
                self.logger.info(f"🔄 Processing database: {db_name}")
                result = self.cleanup_old_data(db_name)

                # Sonuçları topla
                total_stats['deleted_records'] += result['deleted_records']
                total_stats['cleaned_records'] += result['cleaned_records']
                total_stats['cleaned_transactions'] += result['cleaned_transactions']
                total_stats['processed_databases'] += 1

                db_results[db_name] = {
                    'status': 'success',
                    'result': result
                }

                self.logger.info(f"✅ {db_name} cleanup completed successfully")

            except Exception as e:
                self.logger.error(f"❌ {db_name} cleanup failed: {e}")
                total_stats['failed_databases'] += 1

                db_results[db_name] = {
                    'status': 'failed',
                    'error': str(e)
                }

        self.logger.info("All databases processed!")
        self.logger.info(f"Total Summary: "
                         f"Processed {total_stats['processed_databases']} DBs, "
                         f"Failed {total_stats['failed_databases']} DBs, "
                         f"Deleted {total_stats['deleted_records']} records, "
                         f"Cleaned {total_stats['cleaned_records']} records, "
                         f"Removed {total_stats['cleaned_transactions']} transactions")

        return {
            'total_stats': total_stats,
            'db_results': db_results
        }

    def get_all_databases_statistics(self) -> Dict[str, Any]:
        db_names = self._get_hospitals_from_config()

        all_stats = {
            'databases': {},
            'total_summary': {
                'total_records': 0,
                'single_transactions_to_delete': 0,
                'multi_transaction_records': 0,
                'accessible_databases': 0,
                'failed_databases': 0
            }
        }

        self.logger.info(
            f"Getting statistics for {len(db_names)} databases...")

        for db_name in db_names:
            try:
                self.logger.debug(f"📊 Getting stats for: {db_name}")
                stats = self.get_cleanup_statistics(db_name)

                all_stats['databases'][db_name] = {
                    'status': 'success',
                    'stats': stats
                }

                # Toplam istatistiklere ekle
                all_stats['total_summary']['total_records'] += stats['total_records']
                all_stats['total_summary']['single_transactions_to_delete'] += stats['single_transactions_to_delete']
                all_stats['total_summary']['multi_transaction_records'] += stats['multi_transaction_records']
                all_stats['total_summary']['accessible_databases'] += 1

            except Exception as e:
                self.logger.error(f"❌ Failed to get stats for {db_name}: {e}")
                all_stats['databases'][db_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                all_stats['total_summary']['failed_databases'] += 1

        return all_stats

    def cleanup_old_data(self, db_name: str = 'vatanpratikcrm_db') -> Dict[str, int]:
        connection = None
        stats = {
            'deleted_records': 0,
            'cleaned_records': 0,
            'cleaned_transactions': 0
        }

        try:
            connection = self._get_db_connection(db_name)
            cursor = connection.cursor(dictionary=True)

            self.logger.info(f"Data cleanup process started for {db_name}...")

            # Mevcut kayıt sayısını kontrol et
            cursor.execute(
                "SELECT COUNT(*) as total FROM incomingpatientshiys")
            total_before = cursor.fetchone()['total']
            self.logger.info(f"Records before cleanup: {total_before}")

            # Son eklenen kayıtları göster (NAZLICAN kontrol için)
            cursor.execute("""
                SELECT userPatientId, hastaAdi, hastaSoyadi, processTimestamp, sonIslemTarihi, toplamIslemSayisi
                FROM incomingpatientshiys 
                ORDER BY processTimestamp DESC 
                LIMIT 5
            """)
            recent_records = cursor.fetchall()
            self.logger.info("Most recent records in database:")
            for record in recent_records:
                patient_name = f"{record.get('hastaAdi', '')} {record.get('hastaSoyadi', '')}".strip(
                ) or "Unknown"
                self.logger.info(f"   📝 userPatientId: {record['userPatientId']}, "
                                 f"Name: {patient_name}, "
                                 f"Transactions: {record.get('toplamIslemSayisi', 0)}, "
                                 f"Last Transaction: {record.get('sonIslemTarihi', 'N/A')}")

            # 1. Önce silinecek kayıtları bul ve sil
            deleted_count = self._delete_old_single_transaction_records(cursor)
            stats['deleted_records'] = deleted_count

            # 2. Sonra multi-transaction kayıtları temizle
            cleaned_stats = self._cleanup_old_transactions_in_records(cursor)
            stats['cleaned_records'] = cleaned_stats['cleaned_records']
            stats['cleaned_transactions'] = cleaned_stats['cleaned_transactions']

            # Değişiklikleri kaydet
            connection.commit()

            # Cleanup sonrası durum
            cursor.execute(
                "SELECT COUNT(*) as total FROM incomingpatientshiys")
            total_after = cursor.fetchone()['total']
            self.logger.info(
                f"Records after cleanup: {total_after} (was {total_before})")

            # Kalan kayıtları göster
            if total_after > 0:
                cursor.execute("""
                    SELECT userPatientId, hastaAdi, hastaSoyadi, processTimestamp, sonIslemTarihi
                    FROM incomingpatientshiys 
                    ORDER BY processTimestamp DESC 
                    LIMIT 5
                """)
                remaining_records = cursor.fetchall()
                self.logger.info("📋 Remaining records after cleanup:")
                for record in remaining_records:
                    patient_name = f"{record.get('hastaAdi', '')} {record.get('hastaSoyadi', '')}".strip(
                    ) or "Unknown"
                    self.logger.info(f"   ✅ userPatientId: {record['userPatientId']}, "
                                     f"Name: {patient_name}, "
                                     f"Last Transaction: {record.get('sonIslemTarihi', 'N/A')}")
            else:
                self.logger.warning("🚨 No records remaining after cleanup!")

            self.logger.info("Data cleanup completed successfully!")
            self.logger.info(f"Summary: Deleted {stats['deleted_records']} records, "
                             f"Cleaned {stats['cleaned_records']} records, "
                             f"Removed {stats['cleaned_transactions']} old transactions")

            return stats

        except Exception as e:
            self.logger.error(f"Data cleanup error: {str(e)}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                self.logger.debug("Database connection closed")

    def _delete_old_single_transaction_records(self, cursor) -> int:
        try:
            # Önce silinecek kayıtları say
            count_query = """
                SELECT COUNT(*) as count 
                FROM incomingpatientshiys 
                WHERE toplamIslemSayisi = 1 
                AND sonIslemTarihi < %s
            """

            cursor.execute(count_query, (self.cutoff_date,))
            count_result = cursor.fetchone()
            records_to_delete = count_result['count'] if count_result else 0

            if records_to_delete == 0:
                self.logger.info("🗑️  No single-transaction records to delete")
                return 0

            # Debug: Silinecek kayıtları logla
            self.logger.info(
                f"🗑️  Found {records_to_delete} single-transaction records to delete (cutoff: {self.cutoff_date})")

            # Silinecek kayıtların detaylarını logla
            sample_records = []
            if records_to_delete > 0:
                detail_query = """
                    SELECT userPatientId, hastaAdi, hastaSoyadi, sonIslemTarihi, processTimestamp
                    FROM incomingpatientshiys 
                    WHERE toplamIslemSayisi = 1 
                    AND sonIslemTarihi < %s
                    ORDER BY sonIslemTarihi DESC
                    LIMIT 10
                """
                cursor.execute(detail_query, (self.cutoff_date,))
                sample_records = cursor.fetchall()

                self.logger.info("🗑️  Sample records to be deleted:")
                for record in sample_records:
                    patient_name = f"{record.get('hastaAdi', '')} {record.get('hastaSoyadi', '')}".strip(
                    ) or "Unknown"
                    self.logger.info(f"     ❌ userPatientId: {record['userPatientId']}, "
                                     f"Name: {patient_name}, "
                                     f"Last Transaction: {record.get('sonIslemTarihi', 'N/A')}, "
                                     f"Processed: {record.get('processTimestamp', 'N/A')}")

                if records_to_delete > 10:
                    self.logger.info(
                        f"     ... and {records_to_delete - 10} more records")

            if records_to_delete > 100:
                self.logger.warning(
                    f"⚠️  Large deletion: {records_to_delete} records will be deleted")

            # Kayıtları sil
            delete_query = """
                DELETE FROM incomingpatientshiys 
                WHERE toplamIslemSayisi = 1 
                AND sonIslemTarihi < %s
            """

            cursor.execute(delete_query, (self.cutoff_date,))
            deleted_count = cursor.rowcount

            if deleted_count > 0:
                self.logger.info(
                    f"🗑️  Successfully deleted {deleted_count} old single-transaction records")

                # NAZLICAN kontrolü için özel log
                if any('NAZLICAN' in (record.get('hastaAdi', '') + ' ' + record.get('hastaSoyadi', '')) for record in sample_records):
                    self.logger.warning(
                        "🚨 NAZLICAN record was among the deleted records!")

            else:
                self.logger.info("🗑️  No records were actually deleted")

            return deleted_count

        except Exception as e:
            self.logger.error(f"Error deleting old records: {e}")
            raise

    def _cleanup_old_transactions_in_records(self, cursor) -> Dict[str, int]:
        stats = {'cleaned_records': 0, 'cleaned_transactions': 0}

        try:
            # Multi-transaction kayıtları getir
            select_query = """
                SELECT id, userPatientId, toplamIslemSayisi, transactionDetails 
                FROM incomingpatientshiys 
                WHERE toplamIslemSayisi > 1 
                AND transactionDetails IS NOT NULL 
                AND transactionDetails != '{}'
            """

            cursor.execute(select_query)
            records = cursor.fetchall()

            if not records:
                self.logger.info("🧽 No multi-transaction records to clean")
                return stats

            self.logger.info(
                f"🧽 Processing {len(records)} multi-transaction records...")

            for record in records:
                cleaned_data = self._clean_transactions_for_record(record)

                if cleaned_data['has_changes']:
                    # Kayıdı güncelle
                    update_query = """
                        UPDATE incomingpatientshiys 
                        SET toplamIslemSayisi = %s,
                            transactionDetails = %s,
                            sonIslemTarihi = %s,
                            sonDoktorAdi = %s,
                            sonPoliklinik = %s,
                            sonBrans = %s
                        WHERE id = %s
                    """

                    cursor.execute(update_query, (
                        cleaned_data['new_transaction_count'],
                        cleaned_data['new_transaction_json'],
                        cleaned_data['latest_transaction_date'],
                        cleaned_data['latest_doctor'],
                        cleaned_data['latest_dept'],
                        cleaned_data['latest_branch'],
                        record['id']
                    ))

                    stats['cleaned_records'] += 1
                    stats['cleaned_transactions'] += cleaned_data['removed_count']

                    self.logger.debug(f"  ✅ Cleaned record {record['userPatientId']}: "
                                      f"Removed {cleaned_data['removed_count']} old transactions, "
                                      f"Remaining: {cleaned_data['new_transaction_count']}")

            self.logger.info(f"🧽 Cleaned {stats['cleaned_records']} records, "
                             f"removed {stats['cleaned_transactions']} old transactions")

            return stats

        except Exception as e:
            self.logger.error(f"Error cleaning transactions: {e}")
            raise

    def _clean_transactions_for_record(self, record: Dict) -> Dict[str, Any]:
        result = {
            'has_changes': False,
            'new_transaction_count': record['toplamIslemSayisi'],
            'new_transaction_json': record['transactionDetails'],
            'removed_count': 0,
            'latest_transaction_date': None,
            'latest_doctor': None,
            'latest_dept': None,
            'latest_branch': None
        }

        try:
            # Transaction JSON'ını parse et
            transactions_dict = json.loads(record['transactionDetails'])

            if not isinstance(transactions_dict, dict):
                self.logger.debug(
                    f"  ⏭️  Skipping {record['userPatientId']}: Invalid transaction format")
                return result

            # Eski transaction'ları filtrele
            new_transactions_dict = {}
            latest_date = None
            latest_transaction = None

            for trans_id, trans_data in transactions_dict.items():
                try:
                    trans_date_str = trans_data.get('TransactionDate', '')
                    if trans_date_str:
                        # Parse transaction date
                        trans_date = datetime.fromisoformat(
                            trans_date_str.replace('T', ' ').split('.')[0])

                        # 90 günden yeni mi kontrol et
                        if trans_date >= self.cutoff_date:
                            new_transactions_dict[trans_id] = trans_data

                            # En son transaction'ı bul
                            if latest_date is None or trans_date > latest_date:
                                latest_date = trans_date
                                latest_transaction = trans_data
                        else:
                            result['removed_count'] += 1
                    else:
                        # Tarih yoksa koru (güvenlik için)
                        new_transactions_dict[trans_id] = trans_data

                except Exception as e:
                    self.logger.debug(
                        f"  ⚠️  Error parsing transaction {trans_id}: {e}")
                    # Parse hatası varsa koru
                    new_transactions_dict[trans_id] = trans_data

            # Değişiklik var mı kontrol et
            if len(new_transactions_dict) != len(transactions_dict):
                result['has_changes'] = True
                result['new_transaction_count'] = len(new_transactions_dict)
                result['new_transaction_json'] = json.dumps(
                    new_transactions_dict, ensure_ascii=False)

                # En son transaction bilgilerini güncelle
                if latest_transaction:
                    result['latest_transaction_date'] = latest_date
                    result['latest_doctor'] = latest_transaction.get('DrName')
                    result['latest_dept'] = latest_transaction.get('DeptName')
                    result['latest_branch'] = latest_transaction.get(
                        'BranchName')

            return result

        except Exception as e:
            self.logger.error(
                f"Error cleaning transactions for record {record['userPatientId']}: {e}")
            return result

    def get_cleanup_statistics(self, db_name: str = 'vatanpratikcrm_db') -> Dict[str, Any]:
        connection = None
        try:
            connection = self._get_db_connection(db_name)
            cursor = connection.cursor(dictionary=True)

            stats = {}

            # Toplam kayıt sayısı
            cursor.execute(
                "SELECT COUNT(*) as total FROM incomingpatientshiys")
            stats['total_records'] = cursor.fetchone()['total']

            # Silinecek tek işlemli kayıtlar
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM incomingpatientshiys 
                WHERE toplamIslemSayisi = 1 AND sonIslemTarihi < %s
            """, (self.cutoff_date,))
            stats['single_transactions_to_delete'] = cursor.fetchone()['count']

            # Multi-transaction kayıtlar
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM incomingpatientshiys 
                WHERE toplamIslemSayisi > 1
            """, )
            stats['multi_transaction_records'] = cursor.fetchone()['count']

            # En eski kayıt
            cursor.execute(
                "SELECT MIN(sonIslemTarihi) as oldest FROM incomingpatientshiys")
            oldest = cursor.fetchone()['oldest']
            stats['oldest_record_date'] = oldest.strftime(
                '%Y-%m-%d %H:%M:%S') if oldest else None

            # En yeni kayıt
            cursor.execute(
                "SELECT MAX(sonIslemTarihi) as newest FROM incomingpatientshiys")
            newest = cursor.fetchone()['newest']
            stats['newest_record_date'] = newest.strftime(
                '%Y-%m-%d %H:%M:%S') if newest else None

            stats['cutoff_date'] = self.cutoff_date.strftime(
                '%Y-%m-%d %H:%M:%S')

            return stats

        except Exception as e:
            self.logger.error(f"Error getting cleanup statistics: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()


def main():
    """Main function for cron job execution"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Data Cleanup Service - 90 day old data cleanup')
    parser.add_argument(
        '--db-name', help='Specific database name to clean (if not provided, all databases from config will be processed)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show statistics only, do not perform cleanup')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--all', action='store_true',
                        help='Process all databases from hospitals.json (default behavior)')

    args = parser.parse_args()

    # Logging seviyesi
    log_level = logging.DEBUG if args.verbose else logging.INFO
    service = DataCleanupService(log_level=log_level)

    try:
        if args.dry_run:
            # Sadece istatistikleri göster
            print("📊 Cleanup Statistics (DRY RUN)")
            print("=" * 50)

            if args.db_name:
                # Belirli bir database için
                stats = service.get_cleanup_statistics(args.db_name)
                print(f"Database: {args.db_name}")
                print(f"Total records: {stats['total_records']}")
                print(
                    f"Records to delete (single transaction): {stats['single_transactions_to_delete']}")
                print(
                    f"Multi-transaction records: {stats['multi_transaction_records']}")
                print(f"Cutoff date: {stats['cutoff_date']}")
                print(f"Oldest record: {stats['oldest_record_date']}")
                print(f"Newest record: {stats['newest_record_date']}")
            else:
                # Tüm database'ler için
                all_stats = service.get_all_databases_statistics()

                # Database başına detaylar
                for db_name, db_data in all_stats['databases'].items():
                    print(f"\n🏥 Database: {db_name}")
                    if db_data['status'] == 'success':
                        stats = db_data['stats']
                        print(f"  Total records: {stats['total_records']}")
                        print(
                            f"  Records to delete: {stats['single_transactions_to_delete']}")
                        print(
                            f"  Multi-transaction records: {stats['multi_transaction_records']}")
                        print(
                            f"  Oldest record: {stats['oldest_record_date']}")
                        print(
                            f"  Newest record: {stats['newest_record_date']}")
                    else:
                        print(f"  ❌ Error: {db_data['error']}")

                # Toplam özet
                total = all_stats['total_summary']
                print(f"\n📊 TOTAL SUMMARY:")
                print(
                    f"  Accessible databases: {total['accessible_databases']}")
                print(f"  Failed databases: {total['failed_databases']}")
                print(f"  Total records: {total['total_records']}")
                print(
                    f"  Total records to delete: {total['single_transactions_to_delete']}")
                print(
                    f"  Total multi-transaction records: {total['multi_transaction_records']}")

        else:
            # Gerçek temizleme yap
            if args.db_name:
                # Belirli bir database için
                result = service.cleanup_old_data(args.db_name)
                print(f"✅ Cleanup completed for {args.db_name}: {result}")
            else:
                # Tüm database'ler için
                result = service.cleanup_all_databases()
                print(f"✅ Multi-database cleanup completed:")
                print(f"  Total stats: {result['total_stats']}")

                # Database başına sonuçlar
                for db_name, db_result in result['db_results'].items():
                    if db_result['status'] == 'success':
                        print(f"  ✅ {db_name}: {db_result['result']}")
                    else:
                        print(f"  ❌ {db_name}: {db_result['error']}")

    except Exception as e:
        print(f"Cleanup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
