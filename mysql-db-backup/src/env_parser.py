#!/usr/bin/env python3

import os
import glob
import logging
from typing import List, Dict, Any
from pathlib import Path


class EnvParser:
    def __init__(self, env_directory: str = "/etc/mysql-backup/env"):
        self.env_directory = Path(env_directory)
        self.logger = logging.getLogger(__name__)

    def parse_env_file(self, file_path: str) -> Dict[str, str]:
        env_vars = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line_number, line in enumerate(file, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        env_vars[key] = value
                    else:
                        self.logger.warning(
                            f"Invalid line in {file_path}:{line_number}: {line}")
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {str(e)}")
        return env_vars

    def get_database_configs(self) -> List[Dict[str, Any]]:
        db_configs = []
        if not self.env_directory.exists():
            self.logger.error(
                f"Environment directory does not exist: {self.env_directory}")
            return db_configs

        env_files = glob.glob(str(self.env_directory / "*.env"))
        if not env_files:
            self.logger.warning(f"No .env files found in {self.env_directory}")
            return db_configs

        for env_file in env_files:
            env_vars = self.parse_env_file(env_file)
            if self._validate_db_config(env_vars):
                db_config = self._extract_db_config(env_vars, env_file)
                db_configs.append(db_config)
            else:
                self.logger.warning(
                    f"Invalid database configuration in {env_file}")

        return db_configs

    def _validate_db_config(self, env_vars: Dict[str, str]) -> bool:
        required_fields = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        for field in required_fields:
            if field not in env_vars or not env_vars[field]:
                return False
        return True

    def _extract_db_config(self, env_vars: Dict[str, str], source_file: str) -> Dict[str, Any]:
        return {
            'host': env_vars.get('DB_HOST', 'localhost'),
            'port': int(env_vars.get('DB_PORT', '3306')),
            'user': env_vars.get('DB_USER'),
            'password': env_vars.get('DB_PASSWORD'),
            'database': env_vars.get('DB_NAME'),
            'source_file': source_file,
            'backup_name': self._generate_backup_name(env_vars, source_file)
        }

    def _generate_backup_name(self, env_vars: Dict[str, str], source_file: str) -> str:
        if 'BACKUP_NAME' in env_vars:
            return env_vars['BACKUP_NAME']
        db_name = env_vars.get('DB_NAME', 'unknown')
        host = env_vars.get('DB_HOST', 'localhost')
        file_name = Path(source_file).stem
        return f"{file_name}_{host}_{db_name}"
