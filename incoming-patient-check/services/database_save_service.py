from __future__ import annotations

import json
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import os
import logging
from mysql.connector import Error
from models.data_models import FinalProcessedData
from config.constants import (
    DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_CHARSET, DEFAULT_DB_COLLATION,
    DEFAULT_SAVE_BATCH_SIZE, RECENT_TRANSACTIONS_DAYS, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD,
    MAX_CHAT_TYPE_LENGTH, MAX_LANGUAGE_LENGTH, MAX_PHONE_NUMBER_LENGTH,
    MAX_UPN_LENGTH, MAX_TCKN_LENGTH, MAX_NAME_LENGTH, MAX_DOCTOR_NAME_LENGTH,
    MAX_CLINIC_NAME_LENGTH, MAX_SPECIALTY_LENGTH, MAX_GENDER_LENGTH,
    MAX_PATIENT_TYPE_LENGTH, MAX_TAG_LENGTH
)


class RunStatisticsTracker:
    """Global statistics tracker for a single run"""
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.new_records = 0
        self.updated_records = 0
        self.skipped_records = 0
        self.failed_records = 0
        self.total_processed = 0
    
    def add_new(self, count: int = 1):
        self.new_records += count
        self.total_processed += count
    
    def add_updated(self, count: int = 1):
        self.updated_records += count
        self.total_processed += count
    
    def add_skipped(self, count: int = 1):
        self.skipped_records += count
        self.total_processed += count
    
    def add_failed(self, count: int = 1):
        self.failed_records += count
        self.total_processed += count
    
    def get_stats(self) -> Dict[str, int]:
        return {
            'new_records': self.new_records,
            'updated_records': self.updated_records,
            'skipped_records': self.skipped_records,
            'failed_records': self.failed_records,
            'total_processed': self.total_processed
        }


class DatabaseSaveService:

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_config = {
            'user': DEFAULT_DB_USER,
            'password': DEFAULT_DB_PASSWORD,
            'host': DEFAULT_DB_HOST,
            'port': DEFAULT_DB_PORT,
            'charset': DEFAULT_DB_CHARSET,
            'collation': DEFAULT_DB_COLLATION
        }
        # Global statistics tracker for the current run
        self.run_stats = RunStatisticsTracker()

    def save_enriched_data_to_db(self, json_file_path: str, db_name: str = 'vatanpratikcrm_db') -> bool:
        connection = None
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            self.logger.info(f"JSON file read: {json_file_path}")
            self.logger.info(
                f"Total hospitals: {data.get('totalHospitals', 0)}, Total patients: {data.get('totalEnrichedPatients', 0)}")
            connection = self._get_db_connection(db_name)
            connection.start_transaction()

            total_saved = 0
            total_failed = 0
            batch_data: List[Tuple] = []
            batch_size = DEFAULT_SAVE_BATCH_SIZE

            try:
                for hospital in data.get('hospitals', []):
                    app_id = hospital.get('appId')
                    patients = hospital.get('patients', [])

                    self.logger.info(
                        f"Hastane {app_id} işleniyor: {len(patients)} hasta")

                    for patient in patients:
                        patient_data = self._prepare_patient_data(
                            data['timestamp'], app_id, patient)
                        if patient_data:
                            batch_data.append(patient_data)

                            if len(batch_data) >= batch_size:
                                saved, failed = self._save_batch_to_db(
                                    connection, batch_data)
                                total_saved += saved
                                total_failed += failed
                                batch_data = []

                if batch_data:
                    saved, failed = self._save_batch_to_db(
                        connection, batch_data)
                    total_saved += saved
                    total_failed += failed

                connection.commit()
                self.logger.info(
                    f"Veritabanı işlemi tamamlandı: {total_saved} başarılı, {total_failed} başarısız")

                return True

            except Exception as e:
                connection.rollback()
                self.logger.error(f"Transaction rollback yapıldı: {e}")
                raise

        except Exception as e:
            self.logger.error(f"Database save error: {str(e)}")
            return False
        finally:
            if connection and connection.is_connected():
                connection.close()
                self.logger.info("Veritabanı bağlantısı kapatıldı")

    def _save_batch_to_db(self, connection, batch_data: List[Tuple]) -> Tuple[int, int]:
        if not batch_data:
            return 0, 0

        cursor = connection.cursor()
        saved_count = 0
        failed_count = 0

        try:
            insert_query = """
                INSERT INTO incomingpatientshiys 
                (processTimestamp, appId, userPatientId, chatType, ilkMesajTarihi, language, phoneNumber, 
                hiysUpn, hastaAdi, hastaSoyadi, cinsiyet, dogumTarihi, email, 
                toplamIslemSayisi, hiysHastaBulundu, hiysIslemBulundu, 
                sonIslemTarihi, sonDoktorAdi, sonPoliklinik, sonBrans, transactionDetails) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    processTimestamp = VALUES(processTimestamp),
                    chatType = VALUES(chatType),
                    ilkMesajTarihi = VALUES(ilkMesajTarihi),
                    language = VALUES(language),
                    phoneNumber = VALUES(phoneNumber),
                    hiysUpn = VALUES(hiysUpn),
                    hastaAdi = VALUES(hastaAdi),
                    hastaSoyadi = VALUES(hastaSoyadi),
                    cinsiyet = VALUES(cinsiyet),
                    dogumTarihi = VALUES(dogumTarihi),
                    email = VALUES(email),
                    toplamIslemSayisi = VALUES(toplamIslemSayisi),
                    hiysHastaBulundu = VALUES(hiysHastaBulundu),
                    hiysIslemBulundu = VALUES(hiysIslemBulundu),
                    sonIslemTarihi = VALUES(sonIslemTarihi),
                    sonDoktorAdi = VALUES(sonDoktorAdi),
                    sonPoliklinik = VALUES(sonPoliklinik),
                    sonBrans = VALUES(sonBrans),
                    transactionDetails = VALUES(transactionDetails)
            """

            cursor.executemany(insert_query, batch_data)
            saved_count = cursor.rowcount

        except Exception as e:
            self.logger.error(f"Batch insert error: {e}")
            failed_count = len(batch_data)
        finally:
            cursor.close()

        return saved_count, failed_count

    def _prepare_patient_data(self, process_timestamp: str, app_id: str, patient: Dict[str, Any]) -> Optional[tuple]:
        """Hasta verisini veritabanı kaydı için hazırla"""
        try:
            dt = datetime.fromisoformat(
                process_timestamp.replace('T', ' ').split('.')[0])

            patient_details = patient.get('patientDetails', {})
            transactions = patient.get('transactions', [])

            patient_found = patient.get('patientFound', False)
            transactions_found = patient.get('transactionsFound', False)

            if not patient_found:
                return None

            if not transactions_found or not transactions:
                return None

            if not patient_details:
                return None

            recent_transactions = []
            cutoff_date = datetime.now() - timedelta(days=90)

            for transaction in transactions:
                try:
                    trans_date_str = transaction.get('TransactionDate', '')
                    if trans_date_str:
                        trans_date = datetime.fromisoformat(
                            trans_date_str.replace('T', ' ').split('.')[0])
                        if trans_date >= cutoff_date:
                            recent_transactions.append(transaction)
                except Exception:
                    continue

            if not recent_transactions:
                return None

            transactions = recent_transactions
            son_islem_tarihi = None
            son_doktor_adi = None
            son_poliklinik = None
            son_brans = None

            if transactions:
                try:
                    latest_transaction = max(
                        transactions, key=lambda x: x.get('TransactionDate', ''))
                    son_islem_tarihi = datetime.fromisoformat(
                        latest_transaction.get('TransactionDate', '').replace('T', ' ')).date()
                    son_doktor_adi = latest_transaction.get('DoctorName', '')[
                        :MAX_DOCTOR_NAME_LENGTH] if latest_transaction.get('DoctorName') else None
                    son_poliklinik = latest_transaction.get('ClinicName', '')[
                        :MAX_CLINIC_NAME_LENGTH] if latest_transaction.get('ClinicName') else None
                    son_brans = latest_transaction.get('SpecialtyName', '')[
                        :MAX_SPECIALTY_LENGTH] if latest_transaction.get('SpecialtyName') else None
                except Exception as e:
                    self.logger.debug(
                        f"Error processing latest transaction: {e}")

            user_patient_id = patient.get('userPatientId')
            chat_type = patient.get('chatType', '')[
                :MAX_CHAT_TYPE_LENGTH] if patient.get('chatType') else None
            language = patient.get('language', '')[
                :MAX_LANGUAGE_LENGTH] if patient.get('language') else None
            phone_number = patient.get('phoneNumber', '')[
                :MAX_PHONE_NUMBER_LENGTH] if patient.get('phoneNumber') else None

            hiys_upn = patient_details.get(
                'UPN', '')[:MAX_UPN_LENGTH] if patient_details.get('UPN') else None
            hiys_tckn = patient_details.get(
                'TCKN', '')[:MAX_TCKN_LENGTH] if patient_details.get('TCKN') else None
            hiys_ad = patient_details.get(
                'Name', '')[:MAX_NAME_LENGTH] if patient_details.get('Name') else None
            hiys_soyad = patient_details.get('Surname', '')[
                :MAX_NAME_LENGTH] if patient_details.get('Surname') else None

            hiys_dogum_tarihi = None
            if patient_details.get('BirthDate'):
                try:
                    hiys_dogum_tarihi = datetime.fromisoformat(
                        patient_details.get('BirthDate', '').replace('T', ' ')).date()
                except Exception:
                    pass

            hiys_cinsiyet = patient_details.get(
                'Gender', '')[:MAX_GENDER_LENGTH] if patient_details.get('Gender') else None
            hiys_patient_type = patient_details.get('PatientType', '')[
                :MAX_PATIENT_TYPE_LENGTH] if patient_details.get('PatientType') else None
            hiys_tag = patient_details.get(
                'Tag', '')[:MAX_TAG_LENGTH] if patient_details.get('Tag') else None

            # İlk mesaj tarihini al
            ilk_mesaj_tarihi = patient.get('ilkMesajTarihi')

            return (
                dt,  # processTimestamp
                app_id,  # appId
                user_patient_id,  # userPatientId
                chat_type,  # chatType
                ilk_mesaj_tarihi,  # ilkMesajTarihi
                language,  # language
                phone_number,  # phoneNumber
                hiys_upn,  # hiysUpn
                hiys_ad,  # hastaAdi
                hiys_soyad,  # hastaSoyadi
                hiys_cinsiyet,  # cinsiyet
                hiys_dogum_tarihi,  # dogumTarihi
                patient_details.get('Email'),  # email
                len(recent_transactions),  # toplamIslemSayisi
                patient_found,  # hiysHastaBulundu
                transactions_found,  # hiysIslemBulundu
                son_islem_tarihi,  # sonIslemTarihi
                son_doktor_adi,  # sonDoktorAdi
                son_poliklinik,  # sonPoliklinik
                son_brans,  # sonBrans
                None  # transactionDetails (will be filled separately if needed)
            )

        except Exception as e:
            self.logger.error(f"Error preparing patient data: {e}")
            return None

    def _get_db_connection(self, db_name: str):
        try:
            config = self.db_config.copy()
            config['database'] = db_name

            connection = mysql.connector.connect(**config)
            self.logger.debug(f"Database connection established: {db_name}")
            return connection
        except Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise

    def _save_patient_to_db(self, cursor, process_timestamp: str, app_id: str, patient: Dict[str, Any]) -> bool:
        """Tek hasta kaydını veritabanına kaydet"""
        try:
            dt = datetime.fromisoformat(
                process_timestamp.replace('T', ' ').split('.')[0])

            patient_details = patient.get('patientDetails', {})
            transactions = patient.get('transactions', [])

            patient_found = patient.get('patientFound', False)
            transactions_found = patient.get('transactionsFound', False)

            if not patient_found:
                self.logger.info(
                    f"Skipping patient {patient.get('userPatientId')}: Not found in HIYS")
                return True

            if not transactions_found or not transactions:
                self.logger.info(
                    f"Skipping patient {patient.get('userPatientId')}: No transactions in HIYS")
                return True

            if not patient_details:
                self.logger.info(
                    f"Skipping patient {patient.get('userPatientId')}: No patient details from HIYS")
                return True

            recent_transactions = []
            cutoff_date = datetime.now() - timedelta(days=90)

            for transaction in transactions:
                try:
                    trans_date_str = transaction.get('TransactionDate', '')
                    if trans_date_str:
                        trans_date = datetime.fromisoformat(
                            trans_date_str.replace('T', ' ').split('.')[0])
                        if trans_date >= cutoff_date:
                            recent_transactions.append(transaction)
                except Exception as e:
                    self.logger.debug(f"Error parsing transaction date: {e}")
                    continue

            if not recent_transactions:
                self.logger.info(
                    f"Skipping patient {patient.get('userPatientId')}: No recent transactions (last 90 days)")
                return True

            self.logger.debug(
                f"✅ Patient {patient.get('userPatientId')} passed quality filters")

            transactions = recent_transactions
            son_islem_tarihi = None
            son_doktor_adi = None
            son_poliklinik = None
            son_brans = None

            if transactions:
                try:
                    latest_transaction = max(
                        transactions, key=lambda x: x.get('TransactionDate', ''))
                    son_islem_tarihi = datetime.fromisoformat(
                        latest_transaction.get('TransactionDate', '').replace('T', ' '))
                    son_doktor_adi = latest_transaction.get('DrName')
                    son_poliklinik = latest_transaction.get('DeptName')
                    son_brans = latest_transaction.get('BranchName')
                except:
                    latest_transaction = transactions[-1]
                    son_doktor_adi = latest_transaction.get('DrName')
                    son_poliklinik = latest_transaction.get('DeptName')
                    son_brans = latest_transaction.get('BranchName')

            transaction_details_json = None
            if transactions:
                try:
                    transactions_dict = {}
                    for i, t in enumerate(transactions):
                        transaction_id = getattr(
                            t, 'TransactionID', None) or getattr(t, 'ID', None)
                        if not transaction_id:
                            transaction_date = getattr(
                                t, 'TransactionDate', '')
                            dr_name = getattr(t, 'DrName', '')
                            transaction_id = f"tx_{i}_{hash(f'{transaction_date}_{dr_name}') % 100000}"

                        transactions_dict[str(transaction_id)] = {
                            'TransactionID': transaction_id,
                            'TransactionDate': getattr(t, 'TransactionDate', None),
                            'DrName': getattr(t, 'DrName', None),
                            'DeptName': getattr(t, 'DeptName', None),
                            'BranchName': getattr(t, 'BranchName', None)
                        }

                    transaction_details_json = json.dumps(
                        transactions_dict, ensure_ascii=False, default=str)
                except Exception as e:
                    self.logger.error(f"Error serializing transactions: {e}")
                    transaction_details_json = json.dumps(
                        {}, ensure_ascii=False)
            else:
                transaction_details_json = json.dumps({}, ensure_ascii=False)

            check_query = """
                SELECT id FROM incomingpatientshiys 
                WHERE userPatientId = %s AND appId = %s
            """

            cursor.execute(check_query, (patient.get('userPatientId'), app_id))
            existing_record = cursor.fetchone()

            if existing_record:
                update_query = """
                    UPDATE incomingpatientshiys SET
                    processTimestamp = %s,
                    toplamIslemSayisi = %s,
                    hiysHastaBulundu = %s,
                    hiysIslemBulundu = %s,
                    sonIslemTarihi = %s,
                    sonDoktorAdi = %s,
                    sonPoliklinik = %s,
                    sonBrans = %s,
                    transactionDetails = %s
                    WHERE userPatientId = %s AND appId = %s
                """

                cursor.execute(update_query, (
                    process_timestamp, len(
                        transactions), patient_found, transactions_found,
                    son_islem_tarihi, son_doktor_adi, son_poliklinik, son_brans,
                    transaction_details_json, patient.get(
                        'userPatientId'), app_id
                ))

                self.logger.info(
                    f"Patient {patient.get('userPatientId')} UPDATED (existing record)")

            else:
                insert_query = """
                    INSERT INTO incomingpatientshiys 
                    (processTimestamp, appId, userPatientId, chatType, language, phoneNumber,
                     hiysUpn, hastaAdi, hastaSoyadi, cinsiyet, dogumTarihi, email,
                     toplamIslemSayisi, hiysHastaBulundu, hiysIslemBulundu,
                     sonIslemTarihi, sonDoktorAdi, sonPoliklinik, sonBrans, transactionDetails)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                params = (
                    dt,
                    app_id,
                    patient.get('userPatientId'),
                    patient.get('chatType', 'whatsapp'),
                    patient.get('language', 'TR'),
                    patient.get('phoneNumber'),
                    patient_details.get('UPN'),
                    patient_details.get('Name'),
                    patient_details.get('Surname'),
                    patient_details.get('Gender'),
                    patient_details.get('BirthDate'),
                    patient_details.get('Email'),
                    len(transactions),
                    patient.get('patientFound', False),
                    patient.get('transactionsFound', False),
                    son_islem_tarihi,
                    son_doktor_adi,
                    son_poliklinik,
                    son_brans,
                    transaction_details_json
                )

                cursor.execute(insert_query, params)
                self.logger.info(
                    f"Patient {patient.get('userPatientId')} INSERTED (new record)")

            patient_name = f"{patient_details.get('Name', '')} {patient_details.get('Surname', '')}".strip(
            )
            self.logger.debug(
                f"Hasta kaydedildi: {patient.get('userPatientId')} - {patient_name}")
            return True

        except Exception as e:
            self.logger.error(
                f"Hasta kaydetme hatası (ID: {patient.get('userPatientId')}): {str(e)}")
            return False

    def get_latest_run_summary(self, db_name: str = 'vatanpratikcrm_db') -> Optional[Dict[str, Any]]:
        connection = None
        try:
            connection = self._get_db_connection(db_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                "SELECT MAX(processTimestamp) as latest FROM incomingpatientshiys")
            result = cursor.fetchone()
            latest_timestamp = result['latest']

            if not latest_timestamp:
                return None

            cursor.execute("""
                SELECT 
                    processTimestamp,
                    appId,
                    COUNT(*) as toplamHasta,
                    SUM(hiysHastaBulundu) as bulunanHasta,
                    SUM(hiysIslemBulundu) as islemliHasta,
                    AVG(toplamIslemSayisi) as ortalamaIslem,
                    MAX(sonIslemTarihi) as enSonIslem
                FROM incomingpatientshiys 
                WHERE processTimestamp = %s
                GROUP BY processTimestamp, appId
            """, (latest_timestamp,))

            results = cursor.fetchall()

            summary = {
                'processTimestamp': latest_timestamp,
                'hospitals': results,
                'total': {
                    'toplamHasta': sum(r['toplamHasta'] for r in results),
                    'bulunanHasta': sum(r['bulunanHasta'] for r in results),
                    'islemliHasta': sum(r['islemliHasta'] for r in results)
                }
            }

            return summary

        except Exception as e:
            self.logger.error(f"Özet getirme hatası: {str(e)}")
            return None
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def get_run_statistics(self, db_name: str, timestamp: str) -> Optional[Dict[str, int]]:
        """Get detailed statistics for a specific run including new/updated counts"""
        connection = None
        try:
            connection = self._get_db_connection(db_name)
            cursor = connection.cursor()
            
            # Parse timestamp
            dt = datetime.fromisoformat(timestamp.replace('T', ' ').split('.')[0])
            
            # Count total records for this timestamp
            cursor.execute(
                "SELECT COUNT(*) FROM incomingpatientshiys WHERE processTimestamp = %s", 
                (dt,)
            )
            total_count = cursor.fetchone()[0]
            
            # For now, we'll assume all records in this run are either new or updated
            # In a real scenario, you'd need to track this in the database or logs
            
            return {
                'total_processed': total_count,
                'new_records': total_count,  # This is a simplification
                'updated_records': 0,  # This would need proper tracking
                'timestamp': timestamp
            }
            
        except Exception as e:
            self.logger.error(f"Error getting run statistics: {e}")
            return None
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def get_global_run_statistics(self) -> Dict[str, int]:
        """Get global statistics for the current run"""
        return self.run_stats.get_stats()
    
    def reset_run_statistics(self):
        """Reset statistics for a new run"""
        self.run_stats.reset()

    def save_batch_to_db_optimized(self, timestamp: str, app_id: str, patients: list, db_name: str) -> tuple:
        connection = None
        new_records_saved = 0
        skipped = 0
        failed = 0
        updated_records = 0

        try:
            self.logger.info(f"🔗 Connecting to database: {db_name}")
            connection = self._get_db_connection(db_name)
            cursor = connection.cursor()

            self.logger.info(
                f"📊 Processing batch: {len(patients)} patients to {db_name}")

            for patient in patients:
                try:
                    result, action = self._save_patient_data_to_db(
                        cursor, timestamp, app_id, patient, db_name)

                    if result is True:
                        if action == 'inserted':
                            new_records_saved += 1
                            self.run_stats.add_new()  # Track globally
                            self.logger.debug(
                                f"➕ Patient {getattr(patient, 'userPatientId', 'unknown')} INSERTED as NEW record")
                        elif action == 'updated':
                            updated_records += 1
                            self.run_stats.add_updated()  # Track globally
                            self.logger.debug(
                                f"🔄 Patient {getattr(patient, 'userPatientId', 'unknown')} UPDATED existing record")
                        elif action == 'skipped':
                            skipped += 1
                            self.run_stats.add_skipped()  # Track globally
                            self.logger.debug(
                                f"⏭️ Patient {getattr(patient, 'userPatientId', 'unknown')} SKIPPED (quality filters)")
                        else:
                            self.logger.warning(
                                f"Unknown action: {action} for patient {getattr(patient, 'userPatientId', 'unknown')}")
                    else:
                        failed += 1
                        self.run_stats.add_failed()  # Track globally
                        self.logger.debug(
                            f"❌ Patient {getattr(patient, 'userPatientId', 'unknown')} FAILED to process")

                except Exception as e:
                    self.logger.error(
                        f"Error processing patient {getattr(patient, 'userPatientId', 'unknown')}: {e}")
                    failed += 1
                    self.run_stats.add_failed()  # Track globally

            self.logger.info(
                f"🔄 Committing batch to {db_name}: {new_records_saved} new records, {updated_records} updated, {skipped} skipped, {failed} failed")
            connection.commit()
            self.logger.info(f"✅ Batch successfully committed to {db_name}")

        except Exception as e:
            self.logger.error(f"❌ Batch processing error in {db_name}: {e}")
            if connection:
                self.logger.warning(f"🔄 Rolling back transaction in {db_name}")
                connection.rollback()
        finally:
            if connection and connection.is_connected():
                connection.close()

        # Return 4 values: new_records, updated_records, skipped, failed
        return new_records_saved, updated_records, skipped, failed

    def save_final_data_to_db(self, data: FinalProcessedData, db_name: str = 'vatanpratikcrm_db') -> bool:
        connection = None
        try:
            self.logger.debug(
                f"Saving to database: {db_name} ({data.totalEnrichedPatients} patients)")

            connection = self._get_db_connection(db_name)
            cursor = connection.cursor()

            total_saved = 0
            total_failed = 0
            total_skipped = 0

            for hospital in data.hospitals:
                app_id = hospital.appId
                patients = hospital.patients

                self.logger.debug(
                    f"Processing hospital {app_id}: {len(patients)} patients")

                for patient in patients:
                    result = self._save_patient_data_to_db(
                        cursor, data.timestamp, app_id, patient)
                    if result is True:
                        if hasattr(patient, 'patientFound') and not patient.patientFound:
                            total_skipped += 1
                        elif hasattr(patient, 'transactionsFound') and not patient.transactionsFound:
                            total_skipped += 1
                        else:
                            total_saved += 1
                    else:
                        total_failed += 1

            connection.commit()
            self.logger.info(f"Quality filtering completed:")
            self.logger.info(
                f"  ✅ {total_saved} patients saved (met all quality criteria)")
            self.logger.info(
                f"  ⏭️ {total_skipped} patients skipped (quality filters)")
            self.logger.info(f"  ❌ {total_failed} patients failed (errors)")
            self.logger.info(f"Database operation completed successfully!")

            return True

        except Exception as e:
            self.logger.error(f"Error saving final data to database: {str(e)}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection and connection.is_connected():
                connection.close()
                self.logger.info("Database connection closed")

    def _save_patient_data_to_db(self, cursor, process_timestamp: str, app_id: str, patient, db_name: str = None) -> tuple:
        try:
            dt = datetime.fromisoformat(
                process_timestamp.replace('T', ' ').split('.')[0])

            patient_details = patient.patientDetails
            transactions = patient.transactions or []

            self.logger.debug(f"Processing patient {patient.userPatientId}:")
            self.logger.debug(f"  - Chat Type: {patient.chatType}")
            self.logger.debug(f"  - Language: {patient.language}")
            self.logger.debug(f"  - Phone: {patient.phoneNumber}")
            self.logger.debug(f"  - Patient Found: {patient.patientFound}")
            self.logger.debug(
                f"  - Transactions Found: {patient.transactionsFound}")
            self.logger.debug(f"  - Transaction Count: {len(transactions)}")
            self.logger.debug(f"  - İlk Mesaj Tarihi: {getattr(patient, 'ilkMesajTarihi', 'NONE')}")

            if not patient.patientFound:
                self.logger.info(
                    f"⏭️ Skipping patient {patient.userPatientId}: Not found in HIYS (patientFound=False)")
                return True, 'skipped'

            if not patient.transactionsFound or not transactions:
                self.logger.info(
                    f"⏭️ Skipping patient {patient.userPatientId}: No transactions in HIYS (transactionsFound={patient.transactionsFound}, count={len(transactions)})")
                return True, 'skipped'

            if not patient_details:
                self.logger.info(
                    f"⏭️ Skipping patient {patient.userPatientId}: No patient details from HIYS")
                return True, 'skipped'

            recent_transactions = []
            cutoff_date = datetime.now() - timedelta(days=90)

            for transaction in transactions:
                try:
                    if hasattr(transaction, 'TransactionDate') and transaction.TransactionDate:
                        trans_date = datetime.fromisoformat(
                            transaction.TransactionDate.replace('T', ' ').split('.')[0])
                        if trans_date >= cutoff_date:
                            recent_transactions.append(transaction)
                except Exception as e:
                    self.logger.debug(f"Error parsing transaction date: {e}")
                    continue

            if not recent_transactions:
                self.logger.info(
                    f"⏭️ Skipping patient {patient.userPatientId}: No recent transactions in last 90 days (total transactions: {len(transactions)})")
                return True, 'skipped'

            self.logger.debug(
                f"✅ Patient {patient.userPatientId} passed quality filters:")
            self.logger.debug(f"  - HIYS Patient Found: ✅")
            self.logger.debug(
                f"  - HIYS Transactions Found: ✅ ({len(recent_transactions)} recent)")
            self.logger.debug(f"  - Patient Details: ✅")
            self.logger.debug(f"  - Recent Transactions: ✅")

            if patient_details:
                self.logger.debug(f"  - Patient Details:")
                self.logger.debug(f"    - UPN: {patient_details.UPN}")
                self.logger.debug(f"    - TC: {patient_details.TCKNo}")
                self.logger.debug(f"    - Name: {patient_details.Name}")
                self.logger.debug(f"    - Surname: {patient_details.Surname}")
                self.logger.debug(f"    - Gender: {patient_details.Gender}")
                self.logger.debug(f"    - Birth: {patient_details.BirthDate}")
                self.logger.debug(f"    - Email: {patient_details.Email}")
            else:
                self.logger.debug(f"  - No patient details available")

            transactions = recent_transactions
            son_islem_tarihi = None
            son_doktor_adi = None
            son_poliklinik = None
            son_brans = None

            if transactions:
                self.logger.debug(
                    f"  - Processing {len(transactions)} recent transactions")
                try:
                    latest_transaction = max(
                        transactions, key=lambda x: x.TransactionDate or '')
                    self.logger.debug(
                        f"  - Latest transaction: {latest_transaction.TransactionDate}")
                    son_islem_tarihi = datetime.fromisoformat(
                        latest_transaction.TransactionDate.replace('T', ' ').split('.')[0])
                    son_doktor_adi = latest_transaction.DrName
                    son_poliklinik = latest_transaction.DeptName
                    son_brans = latest_transaction.BranchName

                    self.logger.debug(f"  - Latest transaction details:")
                    self.logger.debug(f"    - Date: {son_islem_tarihi}")
                    self.logger.debug(f"    - Doctor: {son_doktor_adi}")
                    self.logger.debug(f"    - Department: {son_poliklinik}")
                    self.logger.debug(f"    - Branch: {son_brans}")
                except Exception as e:
                    self.logger.warning(
                        f"  - Error parsing latest transaction date: {e}")
                    latest_transaction = transactions[-1]
                    son_doktor_adi = latest_transaction.DrName
                    son_poliklinik = latest_transaction.DeptName
                    son_brans = latest_transaction.BranchName

            transaction_json = None
            if transactions:

                try:
                    transactions_dict = {}
                    for i, t in enumerate(transactions):
                        transaction_id = getattr(
                            t, 'TransactionID', None) or getattr(t, 'ID', None)
                        if not transaction_id:

                            transaction_date = getattr(
                                t, 'TransactionDate', '')
                            dr_name = getattr(t, 'DrName', '')
                            transaction_id = f"tx_{i}_{hash(f'{transaction_date}_{dr_name}') % 100000}"

                        transactions_dict[str(transaction_id)] = {
                            "TransactionID": transaction_id,
                            "PtID": getattr(t, 'PtID', None),
                            "DrID": getattr(t, 'DrID', None),
                            "DrName": getattr(t, 'DrName', None),
                            "DrTitleName": getattr(t, 'DrTitleName', None),
                            "DeptID": getattr(t, 'DeptID', None),
                            "DeptName": getattr(t, 'DeptName', None),
                            "BranchID": getattr(t, 'BranchID', None),
                            "BranchName": getattr(t, 'BranchName', None),
                            "TransactionDate": getattr(t, 'TransactionDate', None),
                            "ProcessedAt": datetime.now().isoformat()
                        }

                    if transactions_dict:
                        transaction_json = json.dumps(
                            transactions_dict, ensure_ascii=False)
                        self.logger.debug(
                            f"  - Transaction dict created: {len(transactions_dict)} unique transactions")
                    else:
                        self.logger.debug(
                            f"  - No valid transaction IDs found")
                        transaction_json = None

                except Exception as e:
                    self.logger.warning(
                        f"  - Error creating transaction JSON: {e}")
                    transaction_json = None
            else:
                self.logger.debug(f"  - No transactions available")

            check_query = """
                SELECT id FROM incomingpatientshiys 
                WHERE userPatientId = %s AND appId = %s
            """

            cursor.execute(check_query, (patient.userPatientId, app_id))
            existing_record = cursor.fetchone()

            if existing_record:

                update_query = """
                    UPDATE incomingpatientshiys SET
                    processTimestamp = %s,
                    toplamIslemSayisi = %s,
                    hiysHastaBulundu = %s,
                    hiysIslemBulundu = %s,
                    sonIslemTarihi = %s,
                    sonDoktorAdi = %s,
                    sonPoliklinik = %s,
                    sonBrans = %s,
                    ilkMesajTarihi = %s,
                    transactionDetails = %s
                    WHERE userPatientId = %s AND appId = %s
                """

                # İlk mesaj tarihini patient'tan al
                ilk_mesaj_tarihi = getattr(patient, 'ilkMesajTarihi', None)
                self.logger.debug(f"  - UPDATE: İlk Mesaj Tarihi: {ilk_mesaj_tarihi}")

                cursor.execute(update_query, (
                    dt, len(
                        transactions), patient.patientFound, patient.transactionsFound,
                    son_islem_tarihi, son_doktor_adi, son_poliklinik, son_brans,
                    ilk_mesaj_tarihi, transaction_json, patient.userPatientId, app_id
                ))

                if cursor.rowcount == 1:
                    patient_name = ""
                    if patient_details:
                        patient_name = f"{patient_details.Name or ''} {patient_details.Surname or ''}".strip(
                        )

                    self.logger.info(
                        f"🔄 Patient {patient.userPatientId} UPDATED (existing record) - {patient_name}")
                    self.logger.info(
                        f"🎯 UPDATE Details: DB={db_name or 'unknown'}, Table=incomingpatientshiys, userPatientId={patient.userPatientId}, appId={app_id}")
                    return True, 'updated'
                else:
                    self.logger.error(
                        f"❌ UPDATE failed for patient {patient.userPatientId}: rowcount={cursor.rowcount}")
                    return False, 'failed'

            else:

                insert_query = """
                    INSERT INTO incomingpatientshiys 
                    (processTimestamp, appId, userPatientId, chatType, language, phoneNumber,
                     hiysUpn, hastaAdi, hastaSoyadi, cinsiyet, dogumTarihi, email,
                     toplamIslemSayisi, hiysHastaBulundu, hiysIslemBulundu,
                     sonIslemTarihi, sonDoktorAdi, sonPoliklinik, sonBrans, ilkMesajTarihi, transactionDetails)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                upn_value = patient_details.UPN if patient_details else None
                name_value = patient_details.Name if patient_details else None
                surname_value = patient_details.Surname if patient_details else None
                gender_value = patient_details.Gender if patient_details else None
                birth_value = patient_details.BirthDate if patient_details else None
                email_value = patient_details.Email if patient_details else None

                # İlk mesaj tarihini patient'tan al
                ilk_mesaj_tarihi = getattr(patient, 'ilkMesajTarihi', None)
                self.logger.debug(f"  - INSERT: İlk Mesaj Tarihi: {ilk_mesaj_tarihi}")

                params = (
                    dt,
                    app_id,
                    patient.userPatientId,
                    patient.chatType or 'whatsapp',
                    patient.language or 'TR',
                    patient.phoneNumber,
                    upn_value,
                    name_value,
                    surname_value,
                    gender_value,
                    birth_value,
                    email_value,
                    len(transactions),
                    patient.patientFound,
                    patient.transactionsFound,
                    son_islem_tarihi,
                    son_doktor_adi,
                    son_poliklinik,
                    son_brans,
                    ilk_mesaj_tarihi,
                    transaction_json
                )

                cursor.execute(insert_query, params)

                if cursor.rowcount == 1:
                    patient_name = ""
                    if patient_details:
                        patient_name = f"{patient_details.Name or ''} {patient_details.Surname or ''}".strip(
                        )

                    self.logger.info(
                        f"➕ Patient {patient.userPatientId} INSERTED (new record) - {patient_name}")
                    self.logger.info(
                        f"🎯 INSERT Details: DB={db_name or 'unknown'}, Table=incomingpatientshiys, userPatientId={patient.userPatientId}, appId={app_id}")
                    return True, 'inserted'
                else:
                    self.logger.error(
                        f"❌ INSERT failed for patient {patient.userPatientId}: rowcount={cursor.rowcount}")
                    return False, 'failed'

        except Exception as e:
            self.logger.error(
                f"Error saving patient data (ID: {patient.userPatientId}): {str(e)}")
            return False, 'failed'
