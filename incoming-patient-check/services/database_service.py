from __future__ import annotations

import os
import mysql.connector
from mysql.connector import Error
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from models.data_models import ChatListEntry, UserPatient, CrmHospital
from config.constants import (
    TEST_MODE_DEFAULT, DEFAULT_TEST_RECORD_LIMIT, RECENT_TRANSACTIONS_DAYS
)


class DatabaseService:
    def __init__(self, database: str, user: str, password: str, host: str = 'localhost', port: int = 3306):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self._check_test_mode()

    def __enter__(self):
        """Context manager enter"""
        if self.connect():
            return self
        raise Exception(f"Failed to connect to database {self.database}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

    def _check_test_mode(self):
        self.test_mode = TEST_MODE_DEFAULT
        self.test_limit = DEFAULT_TEST_RECORD_LIMIT if self.test_mode else None

        if self.test_mode:
            self.logger.warning(
                f"DATABASE SERVICE TEST MODE: Active with limit {self.test_limit}")
        else:
            self.logger.debug("DATABASE SERVICE: Production mode")

    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4'
            )
            if self.connection.is_connected():
                self.logger.info(
                    f"Successfully connected to database: {self.database}")
                return True
            return False
        except Error as e:
            self.logger.error(
                f"Error connecting to database {self.database}: {e}")
            return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info(
                f"Database connection closed for: {self.database}")

    def get_chatlist_data(self, days_back: int = RECENT_TRANSACTIONS_DAYS) -> List[Dict[str, Any]]:
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Database connection not established")
            return []

        try:
            cursor = self.connection.cursor(dictionary=True)
            start_date = datetime.now() - timedelta(days=days_back)
            start_date_str = start_date.strftime('%Y-%m-%d')
            query = """
            SELECT c1.* 
            FROM chatlist c1
            INNER JOIN (
                SELECT userPatientId, MAX(dateTime) as max_date
                FROM chatlist 
                WHERE dateTime >= %s
                GROUP BY userPatientId
            ) c2 ON c1.userPatientId = c2.userPatientId 
                 AND c1.dateTime = c2.max_date
            ORDER BY c1.dateTime DESC
            """

            if self.test_mode:
                test_limit = DEFAULT_TEST_RECORD_LIMIT
                query += f" LIMIT {test_limit}"
                self.logger.warning(
                    f"TEST MODE: Limiting results to {test_limit} records")

            cursor.execute(query, (start_date_str,))
            results = cursor.fetchall()

            if self.test_mode:
                self.logger.warning(
                    f"TEST MODE: Retrieved {len(results)} records (limited)")
            else:
                self.logger.info(
                    f"Retrieved {len(results)} unique chat records from {self.database}")

            cursor.close()

            return results

        except Error as e:
            self.logger.error(
                f"Error fetching chatlist data from {self.database}: {e}")
            return []

    def get_user_patient_info(self, user_patient_ids: List[int]) -> Dict[int, Dict[str, Optional[str]]]:
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Database connection not established")
            return {}

        if not user_patient_ids:
            return {}

        try:
            cursor = self.connection.cursor(dictionary=True)

            # IN clause için placeholder oluştur
            placeholders = ','.join(['%s'] * len(user_patient_ids))
            query = f"""
            SELECT id, language, phoneNumber 
            FROM userpatient 
            WHERE id IN ({placeholders})
            """

            cursor.execute(query, user_patient_ids)
            results = cursor.fetchall()

            patient_info_map = {
                row['id']: {
                    'language': row['language'],
                    'phoneNumber': row['phoneNumber']
                }
                for row in results
            }

            self.logger.info(
                f"Retrieved patient info for {len(patient_info_map)} patients from {self.database}")
            cursor.close()

            return patient_info_map

        except Error as e:
            self.logger.error(
                f"Error fetching user patient info from {self.database}: {e}")
            return {}

    def get_hospital_app_id(self, db_name: str) -> Optional[str]:
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Database connection not established")
            return None

        try:
            cursor = self.connection.cursor(dictionary=True)

            query = "SELECT appId FROM crmhospitals WHERE dbName = %s LIMIT 1"
            cursor.execute(query, (db_name,))
            result = cursor.fetchone()

            cursor.close()

            if result:
                self.logger.info(f"Retrieved appId for database {db_name}")
                return result['appId']
            else:
                self.logger.warning(f"No appId found for database {db_name}")
                return None

        except Error as e:
            self.logger.error(
                f"Error fetching hospital appId for {db_name}: {e}")
            return None

    def get_first_message_date_for_patient(self, user_patient_id: int) -> Optional[datetime]:
        """Belirli bir userPatientId için chatlist'ten ilk mesaj tarihini getir"""
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Database connection not established")
            return None

        try:
            cursor = self.connection.cursor(dictionary=True)

            query = """
            SELECT MIN(dateTime) as first_message_date 
            FROM chatlist 
            WHERE userPatientId = %s AND dateTime IS NOT NULL
            """
            cursor.execute(query, (user_patient_id,))
            result = cursor.fetchone()

            cursor.close()

            if result and result['first_message_date']:
                self.logger.debug(f"First message date for patient {user_patient_id}: {result['first_message_date']}")
                return result['first_message_date']
            else:
                self.logger.debug(f"No message date found for patient {user_patient_id}")
                return None

        except Error as e:
            self.logger.error(
                f"Error fetching first message date for patient {user_patient_id}: {e}")
            return None
