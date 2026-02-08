#!/usr/bin/env python3

import os
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


class MySQLBackup:
    def __init__(self, backup_directory: str = "/var/backups/mysql"):
        self.backup_directory = Path(backup_directory)
        self.logger = logging.getLogger(__name__)
        self.backup_directory.mkdir(parents=True, exist_ok=True)

    def create_backup(self, db_config: Dict[str, Any]) -> Optional[str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = db_config['backup_name']
        backup_filename = f"{backup_name}_{timestamp}.sql"
        backup_path = self.backup_directory / backup_filename

        try:
            cmd = self._build_mysqldump_command(db_config)

            with open(backup_path, 'w', encoding='utf-8') as sql_file:
                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, env=self._get_mysqldump_env()
                )

                try:
                    stdout, stderr = process.communicate(timeout=3600)
                    sql_file.write(stdout)

                    if process.returncode != 0:
                        self.logger.error(f"mysqldump failed: {stderr}")
                        if backup_path.exists():
                            backup_path.unlink()
                        return None

                except subprocess.TimeoutExpired:
                    process.kill()
                    self.logger.error(f"Backup timeout for {backup_name}")
                    if backup_path.exists():
                        backup_path.unlink()
                    return None

            if backup_path.exists() and backup_path.stat().st_size > 0:
                self.logger.info(f"Backup completed: {backup_filename}")
                return str(backup_path)
            else:
                self.logger.error(f"Backup file empty: {backup_filename}")
                return None

        except Exception as e:
            self.logger.error(
                f"Error creating backup for {backup_name}: {str(e)}")
            if backup_path.exists():
                backup_path.unlink()
            return None

    def _build_mysqldump_command(self, db_config: Dict[str, Any]) -> list:
        return [
            'mysqldump',
            f'--host={db_config["host"]}',
            f'--port={db_config["port"]}',
            f'--user={db_config["user"]}',
            f'--password={db_config["password"]}',
            '--single-transaction',
            '--triggers',
            '--events',
            '--lock-tables=false',
            '--add-drop-database',
            '--databases',
            db_config['database']
        ]

    def _get_mysqldump_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env['LC_ALL'] = 'C.UTF-8'
        env['LANG'] = 'C.UTF-8'
        return env

    def verify_backup(self, backup_path: str) -> bool:
        try:
            backup_file = Path(backup_path)
            if not backup_file.exists() or backup_file.stat().st_size == 0:
                return False

            with open(backup_path, 'r', encoding='utf-8') as sql_file:
                first_lines = []
                for i, line in enumerate(sql_file):
                    first_lines.append(line.strip())
                    if i >= 10:
                        break

                sql_content = '\n'.join(first_lines)
                return any(keyword in sql_content for keyword in [
                    'mysqldump', 'MySQL dump', 'MariaDB dump', 'Database:'
                ])

        except Exception:
            return False

    def get_backup_info(self, backup_path: str) -> Optional[Dict[str, Any]]:
        try:
            backup_file = Path(backup_path)
            if not backup_file.exists():
                return None

            stat = backup_file.stat()
            return {
                'path': str(backup_file),
                'filename': backup_file.name,
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_ctime),
                'modified': datetime.fromtimestamp(stat.st_mtime),
                'is_valid': self.verify_backup(backup_path)
            }
        except Exception:
            return None
