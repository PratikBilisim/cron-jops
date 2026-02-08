from __future__ import annotations

import requests
import logging
import os
from typing import Dict, Any, List, Optional
import time
from datetime import datetime, timedelta
from models.data_models import (
    PatientData, PatientDetailResponse, TransactionResponse,
    EnrichedPatientData, Transaction
)
from config.constants import (
    DEFAULT_HIYS_BASE_URL, DEFAULT_HIYS_TIMEOUT, DEFAULT_HIYS_MAX_RETRIES,
    DEFAULT_HIYS_REQUEST_DELAY, HTTP_CLIENT_ERROR_START, HTTP_CLIENT_ERROR_END,
    RECENT_TRANSACTIONS_DAYS, HIYS_BATCH_SIZE
)


class HIYSAPIService:

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[int] = None, max_retries: Optional[int] = None, request_delay: Optional[float] = None, database_service=None):
        self.base_url = base_url or DEFAULT_HIYS_BASE_URL
        self.patient_detail_url = f"{self.base_url}/userDetailWithPhoneNumber.php"
        self.transactions_url = f"{self.base_url}/findPatientTransactions.php"
        self.logger = logging.getLogger(__name__)
        self.database_service = database_service

        self.timeout = timeout or DEFAULT_HIYS_TIMEOUT
        self.max_retries = max_retries or DEFAULT_HIYS_MAX_RETRIES
        self.request_delay = request_delay or DEFAULT_HIYS_REQUEST_DELAY

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

        self.logger.info(
            f"HIYS API Service initialized - timeout: {self.timeout}s, retries: {self.max_retries}, delay: {self.request_delay}s")

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.session.close()

    def _make_request(self, url: str, payload: Dict[str, Any], operation: str) -> Optional[Dict[str, Any]]:
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.debug(
                    f"{operation} - Attempt {attempt}/{self.max_retries}")

                response = self.session.post(
                    url,
                    json=payload,
                    timeout=self.timeout
                )

                response.raise_for_status()
                result = response.json()
                time.sleep(self.request_delay)

                return result

            except requests.exceptions.Timeout as e:
                self.logger.warning(
                    f"{operation} - Timeout on attempt {attempt}/{self.max_retries} (timeout: {self.timeout}s)")
                if attempt == self.max_retries:
                    self.logger.error(
                        f"{operation} - All retry attempts failed due to timeout after {self.timeout}s")
                    return None
                time.sleep(self.request_delay * attempt)

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else "Unknown"
                self.logger.warning(
                    f"{operation} - HTTP error on attempt {attempt}/{self.max_retries}: [{status_code}] {e}")

                if e.response and HTTP_CLIENT_ERROR_START <= e.response.status_code < HTTP_CLIENT_ERROR_END:
                    self.logger.error(
                        f"{operation} - Client error, not retrying: {e}")
                    return None

                if attempt == self.max_retries:
                    self.logger.error(
                        f"{operation} - All retry attempts failed: {e}")
                    return None
                time.sleep(self.request_delay * attempt)

            except requests.exceptions.ConnectionError as e:
                self.logger.warning(
                    f"{operation} - Connection error on attempt {attempt}/{self.max_retries}: {e}")
                if attempt == self.max_retries:
                    self.logger.error(
                        f"{operation} - All retry attempts failed due to connection issues: {e}")
                    return None
                time.sleep(self.request_delay * attempt)

            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"{operation} - Request error on attempt {attempt}/{self.max_retries}: {e}")
                if attempt == self.max_retries:
                    self.logger.error(
                        f"{operation} - All retry attempts failed: {e}")
                    return None
                time.sleep(self.request_delay * attempt)

            except Exception as e:
                self.logger.error(
                    f"{operation} - Unexpected error on attempt {attempt}: {e}", exc_info=True)
                return None

        return None

    def _filter_recent_transactions(self, transactions: List[Transaction], days_back: int = RECENT_TRANSACTIONS_DAYS, first_message_date: Optional[datetime] = None) -> List[Transaction]:
        if not transactions:
            return []

        # Tarih aralığı hesapla
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        filtered_transactions = []
        skipped_count = 0
        skipped_by_message_date = 0

        for transaction in transactions:
            transaction_date_str = transaction.TransactionDate

            if not transaction_date_str:
                skipped_count += 1
                continue

            try:
                # ISO format parse et (2025-09-25T13:15:14)
                if 'T' in transaction_date_str:
                    transaction_date = datetime.fromisoformat(
                        transaction_date_str.replace('T', ' '))
                else:
                    transaction_date = datetime.strptime(
                        transaction_date_str, '%Y-%m-%d %H:%M:%S')

                # İlk mesaj tarihinden önce ise atla
                if first_message_date and transaction_date < first_message_date:
                    skipped_by_message_date += 1
                    self.logger.debug(
                        f"Skipped transaction dated {transaction_date.strftime('%Y-%m-%d')} (before first message date {first_message_date.strftime('%Y-%m-%d')})")
                    continue

                # Son n gün içinde mi kontrol et
                if start_date <= transaction_date <= end_date:
                    filtered_transactions.append(transaction)
                else:
                    skipped_count += 1
                    self.logger.debug(
                        f"Skipped transaction dated {transaction_date.strftime('%Y-%m-%d')} (outside {days_back} days range)")

            except Exception as e:
                skipped_count += 1
                self.logger.warning(
                    f"Error parsing transaction date '{transaction_date_str}': {e}")

        if skipped_count > 0 or skipped_by_message_date > 0:
            self.logger.info(
                f"Filtered transactions: {len(filtered_transactions)} kept, {skipped_count} skipped (outside date range), {skipped_by_message_date} skipped (before first message)")

        return filtered_transactions

    def get_patient_details(self, app_id: str, phone_number: str, country_code: str = "TR") -> Optional[PatientDetailResponse]:
        try:
            payload = {
                "appId": app_id,
                "country_code": country_code,
                "phone_number": phone_number
            }

            self.logger.debug(
                f"Getting patient details for phone: {phone_number}, appId: {app_id}")

            result = self._make_request(
                self.patient_detail_url,
                payload,
                f"Patient Details (phone: {phone_number})"
            )

            if result is None:
                return None

            # Hata yanıtı kontrolü
            if 'error' in result and result.get('error') is True:
                error_message = result.get('message', 'Unknown error')
                self.logger.info(
                    f"Patient not found for phone {phone_number}: {error_message}")
                return None

            # Başarılı yanıt kontrolü
            if result and 'patients' in result and result['patients']:
                return PatientDetailResponse(**result)
            else:
                self.logger.warning(
                    f"No patients found for phone: {phone_number}")
                return None

        except Exception as e:
            self.logger.error(
                f"Error parsing patient details response for {phone_number}: {e}")
            return None

    def get_patient_transactions(self, app_id: str, upn: str) -> Optional[TransactionResponse]:
        try:
            payload = {
                "appId": int(app_id),
                "upn": int(upn)
            }

            self.logger.debug(
                f"Getting transactions for UPN: {upn}, appId: {app_id}")

            result = self._make_request(
                self.transactions_url,
                payload,
                f"Transactions (UPN: {upn})"
            )

            if result and 'transactions' in result:
                return TransactionResponse(**result)
            else:
                self.logger.warning(f"No transactions found for UPN: {upn}")
                return None

        except Exception as e:
            self.logger.error(
                f"Error parsing transactions response for UPN {upn}: {e}")
            return None

    def enrich_patient_data(self, patient_data: PatientData, app_id: str) -> Optional[EnrichedPatientData]:

        # Chatlist'ten ilk mesaj tarihini al (eğer database service mevcut ise)
        first_message_date = None
        if self.database_service:
            try:
                first_message_date = self.database_service.get_first_message_date_for_patient(patient_data.userPatientId)
                if first_message_date:
                    self.logger.debug(f"Patient {patient_data.userPatientId}: First message date found: {first_message_date}")
                else:
                    self.logger.debug(f"Patient {patient_data.userPatientId}: No first message date found in chatlist")
            except Exception as e:
                self.logger.warning(f"Error getting first message date for patient {patient_data.userPatientId}: {e}")
        else:
            self.logger.warning(f"Patient {patient_data.userPatientId}: Database service not available for first message date")

        # 1. Hasta detaylarını getir
        patient_details_response = self.get_patient_details(
            app_id, patient_data.phoneNumber)

        # İlk API başarısız ise None döndür (bu hasta işlenmeyecek)
        if not patient_details_response or not patient_details_response.patients:
            self.logger.debug(
                f"Patient {patient_data.userPatientId}: Not found in HIYS system - skipping")
            return None

        # İlk hastayı al (telefon numarası unique olmalı)
        patient_detail = patient_details_response.patients[0]

        # 2. UPN ile işlemleri getir
        transactions_response = self.get_patient_transactions(
            app_id, patient_detail.UPN)

        # İkinci API de başarısız ise None döndür (bu hasta işlenmeyecek)
        if not transactions_response or not transactions_response.transactions:
            self.logger.debug(
                f"Patient {patient_data.userPatientId}: No transactions found in HIYS - skipping")
            return None

        # Transaction'ları ilk mesaj tarihi ve gün filtresi ile filtrele
        recent_transactions = self._filter_recent_transactions(
            transactions_response.transactions, days_back=RECENT_TRANSACTIONS_DAYS, first_message_date=first_message_date)

        # Filtreleme sonrası transaction kalmadıysa hasta eklenmez
        if not recent_transactions:
            self.logger.debug(
                f"Patient {patient_data.userPatientId}: No recent transactions (within {RECENT_TRANSACTIONS_DAYS} days and after first message) - skipping")
            return None

        # Her iki API de başarılı - enriched patient oluştur
        enriched_patient = EnrichedPatientData(
            userPatientId=patient_data.userPatientId,
            chatType=patient_data.chatType,
            language=patient_data.language,
            phoneNumber=patient_data.phoneNumber,
            ilkMesajTarihi=first_message_date
        )
        
        self.logger.debug(f"Patient {patient_data.userPatientId}: EnrichedPatientData created with ilkMesajTarihi: {first_message_date}")

        # Patient details ekle
        enriched_patient.patientDetails = patient_detail
        enriched_patient.patientFound = True

        # Filtrelenmiş transactions ekle (sadece son mesaj tarihi sonrası ve son n günlük)
        enriched_patient.transactions = recent_transactions
        enriched_patient.transactionCount = len(recent_transactions)
        enriched_patient.transactionsFound = True

        log_message = f"Enriched patient {patient_data.userPatientId}: {len(recent_transactions)} recent transactions found"
        if first_message_date:
            log_message += f" (after first message date: {first_message_date.strftime('%Y-%m-%d')})"
        log_message += f" (within {RECENT_TRANSACTIONS_DAYS} days)"
        
        self.logger.info(log_message)

        return enriched_patient

    def process_batch(self, patients: List[PatientData], app_id: str, batch_size: Optional[int] = None) -> List[EnrichedPatientData]:
        if batch_size is None:
            batch_size = HIYS_BATCH_SIZE

        enriched_patients = []
        total_patients = len(patients)
        skipped_count = 0

        self.logger.info(
            f"Processing {total_patients} patients in batches of {batch_size}")

        for i in range(0, total_patients, batch_size):
            batch = patients[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_patients + batch_size - 1) // batch_size

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} patients)")

            for patient in batch:
                try:
                    enriched_patient = self.enrich_patient_data(
                        patient, app_id)

                    # Sadece başarılı olanları ekle (None dönenler atlanır)
                    if enriched_patient is not None:
                        enriched_patients.append(enriched_patient)
                    else:
                        skipped_count += 1

                except Exception as e:
                    self.logger.error(
                        f"Error enriching patient {patient.userPatientId}: {e}")
                    skipped_count += 1

            # Batch'ler arası bekleme
            if i + batch_size < total_patients:
                batch_delay = self.request_delay * 5
                self.logger.info(
                    f"Batch {batch_num} completed. Waiting {batch_delay}s before next batch...")
                time.sleep(batch_delay)

        success_count = len(enriched_patients)
        self.logger.info(
            f"Batch processing completed: {success_count}/{total_patients} patients found in HIYS")
        self.logger.info(
            f"Skipped {skipped_count} patients (not found in HIYS or errors)")

        return enriched_patients
