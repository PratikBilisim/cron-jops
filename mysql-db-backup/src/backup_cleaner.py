#!/usr/bin/env python3

import os
import logging
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any


class BackupCleaner:
    def __init__(self, backup_directory: str = "/var/backups/mysql", retention_days: int = 3):
        self.backup_directory = Path(backup_directory)
        self.retention_days = retention_days
        self.logger = logging.getLogger(__name__)

    def cleanup_old_backups(self) -> Dict[str, Any]:
        stats = {
            'total_files_checked': 0,
            'files_removed': 0,
            'space_freed': 0,
            'errors': 0,
            'removed_files': [],
            'error_files': []
        }

        if not self.backup_directory.exists():
            return stats

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        backup_pattern = str(self.backup_directory / "*.sql")
        backup_files = glob.glob(backup_pattern)
        stats['total_files_checked'] = len(backup_files)

        for backup_file in backup_files:
            try:
                file_path = Path(backup_file)
                file_ctime = datetime.fromtimestamp(file_path.stat().st_ctime)

                if file_ctime < cutoff_date:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    stats['files_removed'] += 1
                    stats['space_freed'] += file_size
                    stats['removed_files'].append({
                        'filename': file_path.name,
                        'size': file_size,
                        'created': file_ctime
                    })
            except Exception as e:
                stats['errors'] += 1
                stats['error_files'].append({
                    'filename': backup_file,
                    'error': str(e)
                })

        return stats

    def get_backup_summary(self) -> Dict[str, Any]:
        summary = {
            'total_backups': 0,
            'total_size': 0,
            'oldest_backup': None,
            'newest_backup': None,
            'backups_by_age': {
                'today': 0,
                'yesterday': 0,
                'this_week': 0,
                'older': 0
            },
            'backup_files': []
        }

        if not self.backup_directory.exists():
            return summary

        backup_pattern = str(self.backup_directory / "*.sql")
        backup_files = glob.glob(backup_pattern)

        if not backup_files:
            return summary

        now = datetime.now()
        today = now.date()
        yesterday = (now - timedelta(days=1)).date()
        week_ago = now - timedelta(days=7)

        for backup_file in backup_files:
            try:
                file_path = Path(backup_file)
                stat = file_path.stat()

                file_info = {
                    'filename': file_path.name,
                    'size': stat.st_size,
                    'created': datetime.fromtimestamp(stat.st_ctime),
                    'modified': datetime.fromtimestamp(stat.st_mtime)
                }

                summary['backup_files'].append(file_info)
                summary['total_size'] += stat.st_size

                if summary['oldest_backup'] is None or file_info['created'] < summary['oldest_backup']:
                    summary['oldest_backup'] = file_info['created']

                if summary['newest_backup'] is None or file_info['created'] > summary['newest_backup']:
                    summary['newest_backup'] = file_info['created']

                file_date = file_info['created'].date()
                if file_date == today:
                    summary['backups_by_age']['today'] += 1
                elif file_date == yesterday:
                    summary['backups_by_age']['yesterday'] += 1
                elif file_info['created'] >= week_ago:
                    summary['backups_by_age']['this_week'] += 1
                else:
                    summary['backups_by_age']['older'] += 1

            except Exception:
                continue

        summary['total_backups'] = len(summary['backup_files'])
        summary['backup_files'].sort(key=lambda x: x['created'], reverse=True)
        return summary

    def verify_retention_policy(self) -> Dict[str, Any]:
        verification = {
            'policy_compliant': True,
            'retention_days': self.retention_days,
            'violations': [],
            'recommendations': []
        }

        summary = self.get_backup_summary()

        if summary['total_backups'] == 0:
            verification['recommendations'].append("No backup files found")
            return verification

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)

        for backup in summary['backup_files']:
            if backup['created'] < cutoff_date:
                verification['policy_compliant'] = False
                verification['violations'].append({
                    'filename': backup['filename'],
                    'age_days': (datetime.now() - backup['created']).days,
                    'created': backup['created']
                })

        if verification['violations']:
            verification['recommendations'].append(
                f"Run cleanup to remove {len(verification['violations'])} old backup files"
            )

        if summary['total_backups'] > (self.retention_days * 4):
            verification['recommendations'].append(
                "Consider running cleanup more frequently"
            )

        return verification

    def _format_bytes(self, bytes_size: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"
