#!/usr/bin/env python3
"""
Incoming Patient Check Cron Service Coordinator
"""

from config.constants import (
    DEFAULT_LOG_LEVEL, DEFAULT_LOG_FILE, DEFAULT_BATCH_SIZE,
    ENABLE_DATA_PROCESSING_DEFAULT, ENABLE_DATA_CLEANUP_DEFAULT,
    CLEANUP_DELAY_MINUTES_DEFAULT, DEFAULT_TEST_RECORD_LIMIT,
    TEST_MODE_DEFAULT, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD,
    DEFAULT_DB_HOST, ENABLE_HIYS_ENRICHMENT_DEFAULT
)
from services.data_cleanup_service import DataCleanupService
from services.database_save_service import DatabaseSaveService
from services.data_processor import DataProcessor
from services.whatsapp_service import WhatsAppService
from utils.logger import setup_logging
from typing import Optional, List
import os
import sys
import logging
import time
import subprocess
from datetime import datetime
from pathlib import Path

if sys.version_info < (3, 7):
    print("Error: Python 3.7 or higher is required")
    sys.exit(1)
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))


class CronServiceCoordinator:
    def __init__(self):
        log_level = DEFAULT_LOG_LEVEL
        log_file = DEFAULT_LOG_FILE
        if not os.path.isabs(log_file):
            log_file = str(project_root / log_file)

        setup_logging(log_level, log_file)

        self.logger = logging.getLogger(__name__)
        self.logger.info("Cron Service Coordinator initialized")

        batch_size = DEFAULT_BATCH_SIZE
        self.data_processor = DataProcessor(batch_size=batch_size)
        self.db_save_service = DatabaseSaveService()
        self.whatsapp_service = WhatsAppService()
        self.cleanup_service_script = str(
            project_root / 'services' / 'data_cleanup_service.py')
        self.python_executable = sys.executable or '/usr/bin/python3'
        self.enable_data_processing = ENABLE_DATA_PROCESSING_DEFAULT
        self.enable_data_cleanup = ENABLE_DATA_CLEANUP_DEFAULT
        self.cleanup_delay = CLEANUP_DELAY_MINUTES_DEFAULT

        if TEST_MODE_DEFAULT:
            self.logger.warning(f"TEST MODE ACTIVE - Limited to {DEFAULT_TEST_RECORD_LIMIT} records per hospital")
        else:
            self.logger.info("Production mode - Processing all records")

    def run_data_processing(self) -> dict:
        try:
            self.logger.info("Starting data processing...")
            db_user = DEFAULT_DB_USER
            db_password = DEFAULT_DB_PASSWORD
            db_host = DEFAULT_DB_HOST
            enable_hiys = ENABLE_HIYS_ENRICHMENT_DEFAULT

            if not db_password:
                self.logger.error(
                    "Database password not configured in environment")
                return {'success': False, 'stats': {}}

            # Reset statistics at the beginning of each run
            self.db_save_service.reset_run_statistics()

            self.logger.info("Processing data for all hospitals...")
            processed_data = self.data_processor.process_all_hospitals(
                db_user,
                db_password,
                db_host
            )

            if not processed_data.hospitals:
                self.logger.warning("No hospital data processed")
                return {'success': False, 'stats': {}}

            if enable_hiys:
                self.logger.info("HIYS enrichment enabled - Starting batch processing...")
                result = self.data_processor.enrich_data_with_hiys_batch(
                    processed_data)
                if isinstance(result, tuple):
                    success, hiys_stats = result
                    if success:
                        self.logger.info("HIYS enrichment and database save completed successfully")
                        # Use real statistics from HIYS enrichment
                        actual_added = hiys_stats.get('new_records', 0)
                        actual_updated = hiys_stats.get('updated_records', 0)
                    else:
                        self.logger.error("HIYS enrichment or database save failed")
                        return {'success': False, 'stats': {}}
                else:
                    # Backward compatibility - old return format
                    success = result
                    if success:
                        self.logger.info("HIYS enrichment and database save completed successfully")
                        # Fallback to global statistics
                        global_stats = self.db_save_service.get_global_run_statistics()
                        actual_added = global_stats.get('new_records', 0)
                        actual_updated = global_stats.get('updated_records', 0)
                    else:
                        self.logger.error("HIYS enrichment or database save failed")
                        return {'success': False, 'stats': {}}

            else:
                self.logger.info("HIYS enrichment disabled - Saving basic data...")
                result = self.data_processor.save_processed_data_to_db(
                    processed_data)
                
                if isinstance(result, tuple):
                    success, basic_stats = result
                    if success:
                        self.logger.info("Basic data saved to database successfully")
                        # Use statistics from basic data save
                        actual_added = basic_stats.get('new_records', 0)
                        actual_updated = basic_stats.get('updated_records', 0)
                    else:
                        self.logger.error("Failed to save basic data to database")
                        return {'success': False, 'stats': {}}
                else:
                    # Backward compatibility - old return format
                    success = result
                    if success:
                        self.logger.info("Basic data saved to database successfully")
                        # Fallback to global statistics
                        global_stats = self.db_save_service.get_global_run_statistics()
                        actual_added = global_stats.get('new_records', 0)
                        actual_updated = global_stats.get('updated_records', 0)
                    else:
                        self.logger.error("Failed to save basic data to database")
                        return {'success': False, 'stats': {}}

            total_patients = sum(len(hospital.patients)
                                 for hospital in processed_data.hospitals)
            self.logger.info(f"Data processing summary: Processed {len(processed_data.hospitals)} hospitals, {total_patients} patients")

            # Get actual database statistics from the global tracker
            configs = self.data_processor.load_hospital_configs()
            db_name = configs[0].get('dbName', 'vatanpratikcrm_db') if configs else 'vatanpratikcrm_db'
            
            # Use the actual statistics from HIYS enrichment if available
            if 'actual_added' not in locals():
                # Fallback to global statistics if not set from HIYS enrichment
                global_stats = self.db_save_service.get_global_run_statistics()
                actual_added = global_stats.get('new_records', 0)
                actual_updated = global_stats.get('updated_records', 0)
            
            # Ensure we have valid statistics (never None)
            actual_added = actual_added or 0
            actual_updated = actual_updated or 0

            self.logger.info(f"📊 Final Statistics - Added: {actual_added}, Updated: {actual_updated}, Total Processed: {total_patients}")

            return {
                'success': True,
                'stats': {
                    'hospitals': len(processed_data.hospitals),
                    'patients': total_patients,
                    'added': actual_added,
                    'updated': actual_updated,
                    'database': db_name
                }
            }

        except Exception as e:
            self.logger.error(f"Data processing error: {e}", exc_info=True)
            return {'success': False, 'stats': {}}

    def _extract_db_stats_from_logs(self) -> tuple[int, int]:
        """Extract the actual number of NEW and UPDATED records from recent logs"""
        try:
            import re
            
            # Read recent log entries (last 100 lines should be enough)
            log_handler = None
            for handler in self.logger.handlers:
                if hasattr(handler, 'baseFilename'):
                    log_handler = handler
                    break
            
            if log_handler and hasattr(log_handler, 'baseFilename'):
                try:
                    with open(log_handler.baseFilename, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        # Look for recent "NEW records" and "updated" entries
                        new_pattern = r'Batch \d+: (\d+) NEW records'
                        total_new = 0
                        total_updated = 0
                        
                        for line in reversed(lines[-100:]):  # Check last 100 lines
                            # Look for NEW records
                            new_match = re.search(new_pattern, line)
                            if new_match:
                                total_new += int(new_match.group(1))
                            
                            # Look for UPDATE patterns (if any exist in logs)
                            # This might need adjustment based on actual log format
                            if 'UPDATE' in line or 'updated' in line.lower():
                                update_match = re.search(r'(\d+)\s*(?:updated|UPDATE)', line)
                                if update_match:
                                    total_updated += int(update_match.group(1))
                        
                        return total_new, total_updated
                except Exception:
                    pass
            return 0, 0
        except Exception:
            return 0, 0

    def run_data_cleanup(self) -> dict:
        try:
            self.logger.info("Starting data cleanup...")
            cleanup_service = DataCleanupService(log_level=logging.INFO)
            result = cleanup_service.cleanup_all_databases()

            total_stats = result['total_stats']
            self.logger.info("Data cleanup completed:")
            self.logger.info(f"Processed {total_stats['processed_databases']} databases")
            self.logger.info(f"Deleted {total_stats['deleted_records']} old records")
            self.logger.info(f"Cleaned {total_stats['cleaned_records']} records")
            self.logger.info(f"Removed {total_stats['cleaned_transactions']} old transactions")

            if total_stats['failed_databases'] > 0:
                self.logger.warning(f"Failed to process {total_stats['failed_databases']} databases")

            success = total_stats['failed_databases'] == 0
            return {
                'success': success,
                'stats': {
                    'deleted_records': total_stats['deleted_records'],
                    'cleaned_records': total_stats['cleaned_records'],
                    'cleaned_transactions': total_stats['cleaned_transactions'],
                    'processed_databases': total_stats['processed_databases']
                }
            }

        except Exception as e:
            self.logger.error(f"Data cleanup error: {e}", exc_info=True)
            return {'success': False, 'stats': {}}

    def run_service(self, script_path: str, service_name: str, args: Optional[List[str]] = None) -> bool:
        try:
            cmd = [self.python_executable, script_path]
            if args:
                cmd.extend(args)

            self.logger.info(f"Starting {service_name}...")
            self.logger.debug(f"Command: {' '.join(cmd)}")

            start_time = time.time()
            result = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                timeout=3600  # 1 saat timeout
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                self.logger.info(f"{service_name} completed successfully in {duration:.2f} seconds")

                if result.stdout.strip():
                    self.logger.debug(f"{service_name} stdout:\n{result.stdout}")

                return True
            else:
                self.logger.error(f"{service_name} failed with return code {result.returncode}")
                if result.stderr.strip():
                    self.logger.error(
                        f"{service_name} stderr:\n{result.stderr}")

                return False

        except subprocess.TimeoutExpired:
            self.logger.error(f"{service_name} timed out after 1 hour")
            return False
        except Exception as e:
            self.logger.error(f"{service_name} error: {e}")
            return False

    def run(self):
        overall_start_time = time.time()
        self.logger.info("Cron Service Coordinator started")

        results = {
            'data_cleanup': {'enabled': self.enable_data_cleanup, 'success': False, 'stats': {}},
            'data_processing': {'enabled': self.enable_data_processing, 'success': False, 'stats': {}}
        }

        try:
            if self.enable_data_cleanup:
                self.logger.info("Phase 1: Data Cleanup Service")
                cleanup_result = self.run_data_cleanup()
                results['data_cleanup']['success'] = cleanup_result['success']
                results['data_cleanup']['stats'] = cleanup_result['stats']
            else:
                self.logger.info("Phase 1: Data Cleanup Service - DISABLED")

            if self.enable_data_processing:
                self.logger.info("Phase 2: Data Processing Service")
                processing_result = self.run_data_processing()
                results['data_processing']['success'] = processing_result['success']
                results['data_processing']['stats'] = processing_result['stats']

                if not results['data_processing']['success']:
                    self.logger.warning("Data processing failed, but continuing with cleanup...")

                if self.enable_data_cleanup and self.cleanup_delay > 0:
                    self.logger.info(f"Waiting {self.cleanup_delay} minutes before cleanup...")
                    time.sleep(self.cleanup_delay * 60)
                elif self.enable_data_cleanup:
                    self.logger.info("Proceeding directly to cleanup (no delay configured)")
            else:
                self.logger.info("Phase 2: Data Processing Service - DISABLED")

            overall_success = True

            for service_name, result in results.items():
                if result['enabled']:
                    if result['success']:
                        self.logger.info(f"{service_name.replace('_', ' ').title()}: SUCCESS")
                    else:
                        self.logger.error(f"{service_name.replace('_', ' ').title()}: FAILED")
                        overall_success = False
                else:
                    self.logger.info(f"{service_name.replace('_', ' ').title()}: DISABLED")

            overall_duration = time.time() - overall_start_time

            if overall_success:
                self.logger.info(f"All enabled services completed successfully in {overall_duration:.2f} seconds")
            else:
                self.logger.error(f"Some services failed. Total duration: {overall_duration:.2f} seconds")

            # Send WhatsApp notification about cron completion
            try:
                self.whatsapp_service.send_cron_completion_notification(results, overall_duration)
            except Exception as e:
                self.logger.error(f"Failed to send WhatsApp notification: {e}")

            return overall_success

        except Exception as e:
            self.logger.error(f"Critical error in cron coordinator: {e}", exc_info=True)
            
            # Send error notification
            try:
                self.whatsapp_service.send_error_notification(str(e), "Cron Coordinator")
            except Exception as notification_error:
                self.logger.error(f"Failed to send error notification: {notification_error}")
            
            return False

    def get_status_summary(self):
        return {
            'timestamp': datetime.now().isoformat(),
            'data_processing_enabled': self.enable_data_processing,
            'data_cleanup_enabled': self.enable_data_cleanup,
            'cleanup_delay_minutes': self.cleanup_delay,
            'python_executable': self.python_executable,
            'project_root': str(project_root)
        }


def main():
    coordinator = CronServiceCoordinator()

    status = coordinator.get_status_summary()
    coordinator.logger.info(f"Service configuration: {status}")
    success = coordinator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
