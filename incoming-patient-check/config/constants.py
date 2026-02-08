# -*- coding: utf-8 -*-
"""Project configuration constants"""

from typing import Final, List, Dict, Any

# Database Configuration
DEFAULT_DB_USER: Final[str] = 'cronrunner'
DEFAULT_DB_PASSWORD: Final[str] = 'pratik-zxc025*'
DEFAULT_DB_HOST: Final[str] = 'localhost'
DEFAULT_DB_PORT: Final[int] = 3306
DEFAULT_DB_CHARSET: Final[str] = 'utf8mb4'
DEFAULT_DB_COLLATION: Final[str] = 'utf8mb4_turkish_ci'

# HIYS API Configuration
DEFAULT_HIYS_BASE_URL: Final[str] = 'https://prtk.gen.tr/HIYS/crm'
DEFAULT_HIYS_TIMEOUT: Final[int] = 30
DEFAULT_HIYS_MAX_RETRIES: Final[int] = 3
DEFAULT_HIYS_REQUEST_DELAY: Final[float] = 0
HIYS_BATCH_SIZE: Final[int] = 10
ENABLE_HIYS_ENRICHMENT_DEFAULT: Final[bool] = True

# Service Configuration
ENABLE_DATA_PROCESSING_DEFAULT: Final[bool] = True
ENABLE_DATA_CLEANUP_DEFAULT: Final[bool] = True
CLEANUP_DELAY_MINUTES_DEFAULT: Final[int] = 0
ENABLE_HIYS_DEFAULT: Final[bool] = True

# Data Processing Constants
DEFAULT_BATCH_SIZE: Final[int] = 1000
DEFAULT_TEST_RECORD_LIMIT: Final[int] = 200
TEST_BATCH_SIZE: Final[int] = 10
RECENT_TRANSACTIONS_DAYS: Final[int] = 90
CLEANUP_DAYS: Final[int] = 90

# Test Mode Configuration
TEST_MODE_DEFAULT: Final[bool] = False
TEST_CLEANUP_DAYS_DEFAULT: Final[int] = 90

# Batch Processing
DEFAULT_SAVE_BATCH_SIZE: Final[int] = 100

# Logging Configuration
DEFAULT_LOG_LEVEL: Final[str] = 'DEBUG'
DEFAULT_LOG_FILE: Final[str] = 'logs/cron_service.log'
LOG_DATE_FORMAT: Final[str] = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT: Final[str] = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Field Length Limits
MAX_CHAT_TYPE_LENGTH: Final[int] = 50
MAX_LANGUAGE_LENGTH: Final[int] = 10
MAX_PHONE_NUMBER_LENGTH: Final[int] = 20
MAX_UPN_LENGTH: Final[int] = 50
MAX_TCKN_LENGTH: Final[int] = 11
MAX_NAME_LENGTH: Final[int] = 100
MAX_DOCTOR_NAME_LENGTH: Final[int] = 100
MAX_CLINIC_NAME_LENGTH: Final[int] = 100
MAX_SPECIALTY_LENGTH: Final[int] = 100
MAX_GENDER_LENGTH: Final[int] = 10
MAX_PATIENT_TYPE_LENGTH: Final[int] = 50
MAX_TAG_LENGTH: Final[int] = 100

# HTTP Status Codes
HTTP_CLIENT_ERROR_START: Final[int] = 400
HTTP_CLIENT_ERROR_END: Final[int] = 500

# MySQL Error Codes
MYSQL_ACCESS_DENIED: Final[int] = 1045
MYSQL_CANNOT_CONNECT: Final[int] = 2003
MYSQL_UNKNOWN_DATABASE: Final[int] = 1049

# Process Timeouts
SUBPROCESS_TIMEOUT_SECONDS: Final[int] = 3600  # 1 hour

# Logging
LOG_DATE_FORMAT: Final[str] = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT: Final[str] = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# File Paths
DEFAULT_CONFIG_PATH: Final[str] = 'config/hospitals.json'
DEFAULT_LOG_FILE: Final[str] = 'logs/cron_service.log'

# Test Configuration
TEST_CLEANUP_DAYS_DEFAULT: Final[int] = 90

# WhatsApp Messaging Configuration
WHATSAPP_API_URL: Final[str] = 'https://dev.pratikcrm.tr:5009/api/channels/f55e7d46-ebf2-456b-aeb8-e8fd66b21723/send'
WHATSAPP_PHONE_NUMBER: Final[str] = '905418341781'
ENABLE_WHATSAPP_NOTIFICATIONS: Final[bool] = True
WHATSAPP_REQUEST_TIMEOUT: Final[int] = 30
WHATSAPP_VERIFY_SSL: Final[bool] = False


HOSPITAL_CONFIGS: Final[List[Dict[str, Any]]] = [
    {
        "appId": "14",
        "dbName": "sanapratikcrm_db",
        "username": "sanapratikcrm", 
        "password": "pratik-zxc025*",
        "host": "localhost"
    }
]

DEFAULT_CONFIG_PATH: Final[str] = 'config/hospitals.json'
