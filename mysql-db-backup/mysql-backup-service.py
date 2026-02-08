#!/usr/bin/env python3

import sys
import os
import logging
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Add source directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
if os.path.exists(src_dir):
    sys.path.insert(0, src_dir)
else:
    # For installed version, try the installation directory
    install_dirs = ['/opt/mysql-backup-service/src', '/opt/mysql-backup-service']
    for install_dir in install_dirs:
        if os.path.exists(install_dir):
            sys.path.insert(0, install_dir)
            break

from backup_cleaner import BackupCleaner
from mysql_backup import MySQLBackup
from env_parser import EnvParser


class MySQLBackupService:
    def __init__(self, config_file: str = "/etc/mysql-backup/config.json"):
        self.config = self._load_config(config_file)
        self.logger = self._setup_logging()

        self.env_parser = EnvParser(self.config['env_directory'])
        self.backup_service = MySQLBackup(self.config['backup_directory'])
        self.cleaner = BackupCleaner(
            self.config['backup_directory'],
            self.config['retention_days']
        )

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        default_config = {
            'env_directory': '/etc/mysql-backup/env',
            'backup_directory': '/var/backups/mysql',
            'log_directory': '/var/log/mysql-backup',
            'log_level': 'INFO',
            'retention_days': 3,
            'notification': {
                'enabled': False,
                'email': None,
                'webhook': None
            }
        }

        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
        except Exception as e:
            print(f"Warning: Could not load config file {config_file}: {e}")
            print("Using default configuration")

        return default_config

    def _setup_logging(self) -> logging.Logger:
        log_dir = Path(self.config['log_directory'])
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / 'mysql-backup.log'

        logging.basicConfig(
            level=getattr(logging, self.config['log_level']),
            format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

        logger = logging.getLogger(__name__)
        logger.info("MySQL Backup Service initialized")
        return logger

    def run_backup(self) -> Dict[str, Any]:
        results = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_databases': 0,
            'successful_backups': 0,
            'failed_backups': 0,
            'backup_details': [],
            'errors': []
        }

        try:
            db_configs = self.env_parser.get_database_configs()
            results['total_databases'] = len(db_configs)

            if not db_configs:
                return results

            for db_config in db_configs:
                backup_result = self._backup_single_database(db_config)
                results['backup_details'].append(backup_result)

                if backup_result['success']:
                    results['successful_backups'] += 1
                else:
                    results['failed_backups'] += 1
                    results['errors'].append(backup_result['error'])

            cleanup_stats = self.cleaner.cleanup_old_backups()
            results['cleanup_stats'] = cleanup_stats

        except Exception as e:
            self.logger.error(f"Error during backup operation: {str(e)}")
            results['errors'].append(str(e))

        finally:
            results['end_time'] = datetime.now()
            duration = results['end_time'] - results['start_time']
            self.logger.info(f"Backup operation completed in {duration}")

        return results

    def _backup_single_database(self, db_config: Dict[str, Any]) -> Dict[str, Any]:
        backup_result = {
            'database_name': db_config['backup_name'],
            'host': db_config['host'],
            'database': db_config['database'],
            'success': False,
            'backup_path': None,
            'backup_size': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'error': None
        }

        try:
            backup_path = self.backup_service.create_backup(db_config)

            if backup_path:
                backup_info = self.backup_service.get_backup_info(backup_path)
                if backup_info and backup_info['is_valid']:
                    backup_result['success'] = True
                    backup_result['backup_path'] = backup_path
                    backup_result['backup_size'] = backup_info['size']
                else:
                    backup_result['error'] = "Backup verification failed"
            else:
                backup_result['error'] = "Backup creation failed"

        except Exception as e:
            backup_result['error'] = str(e)

        finally:
            backup_result['end_time'] = datetime.now()

        return backup_result

    def status(self) -> Dict[str, Any]:
        status = {
            'timestamp': datetime.now(),
            'config': self.config,
            'database_configs': [],
            'backup_summary': {},
            'retention_verification': {}
        }

        try:
            db_configs = self.env_parser.get_database_configs()
            status['database_configs'] = [
                {
                    'backup_name': config['backup_name'],
                    'host': config['host'],
                    'database': config['database'],
                    'source_file': config['source_file']
                }
                for config in db_configs
            ]

            status['backup_summary'] = self.cleaner.get_backup_summary()
            status['retention_verification'] = self.cleaner.verify_retention_policy()

        except Exception as e:
            status['error'] = str(e)

        return status

    def cleanup(self) -> Dict[str, Any]:
        try:
            return self.cleaner.cleanup_old_backups()
        except Exception as e:
            return {'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description='MySQL Backup Service')
    parser.add_argument('action', choices=[
                        'backup', 'status', 'cleanup'], help='Action to perform')
    parser.add_argument(
        '--config', '-c', default='/etc/mysql-backup/config.json', help='Configuration file path')
    parser.add_argument('--json', action='store_true',
                        help='Output results in JSON format')

    args = parser.parse_args()

    try:
        service = MySQLBackupService(args.config)

        if args.action == 'backup':
            results = service.run_backup()
        elif args.action == 'status':
            results = service.status()
        elif args.action == 'cleanup':
            results = service.cleanup()

        if args.json:
            print(json.dumps(results, default=str, indent=2))
        else:
            if args.action == 'backup':
                print(
                    f"Backup completed: {results['successful_backups']}/{results['total_databases']} successful")
                if results['failed_backups'] > 0:
                    print(f"Failed backups: {results['failed_backups']}")
            elif args.action == 'status':
                print(
                    f"Database configurations: {len(results['database_configs'])}")
                print(
                    f"Total backups: {results['backup_summary']['total_backups']}")
                print(
                    f"Policy compliant: {results['retention_verification']['policy_compliant']}")
            elif args.action == 'cleanup':
                if 'error' in results:
                    print(f"Cleanup failed: {results['error']}")
                else:
                    print(
                        f"Cleanup completed: {results['files_removed']} files removed")

        if args.action == 'backup' and results['failed_backups'] > 0:
            sys.exit(1)
        elif 'error' in results:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"Service error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
