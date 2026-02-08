from __future__ import annotations

import json
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from services.database_service import DatabaseService
from services.hiys_api_service import HIYSAPIService
from services.database_save_service import DatabaseSaveService
from models.data_models import (
    PatientData, HospitalData, ProcessedData,
    EnrichedPatientData, EnrichedHospitalData, FinalProcessedData
)
from config.constants import (
    DEFAULT_BATCH_SIZE, DEFAULT_TEST_RECORD_LIMIT, TEST_MODE_DEFAULT,
    HOSPITAL_CONFIGS
)


class DataProcessor:

    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_save_service = DatabaseSaveService()
        self.batch_size = batch_size

        self.test_mode: bool = TEST_MODE_DEFAULT
        self.test_record_limit: int = DEFAULT_TEST_RECORD_LIMIT

        if self.test_mode:
            self.logger.warning(
                f"TEST MODE ENABLED - Limited to {self.test_record_limit} records per hospital")
        else:
            self.logger.info("Production mode - Processing all records")

    def load_hospital_configs(self, config_path: str = None) -> List[Dict[str, Any]]:
        if config_path:
            self.logger.debug(
                f"config_path parameter ignored, using constants: {config_path}")

        configs = HOSPITAL_CONFIGS
        self.logger.info(
            f"Loaded {len(configs)} hospital configurations from constants")
        return configs

    def process_hospital_data(self, hospital_config: Dict[str, Any], db_user: str, db_password: str, db_host: str) -> HospitalData:
        hospital_data = HospitalData(
            appId=None,
            patients=[]
        )

        try:
            with DatabaseService(
                host=db_host,
                user=db_user,
                password=db_password,
                database=hospital_config.get('dbName', '')
            ) as db_service:

                self.logger.info(
                    f"Connected to database: {hospital_config.get('dbName')}")
                db_name = hospital_config.get('dbName', '')
                if db_name:
                    app_id = db_service.get_hospital_app_id(db_name)
                    if app_id:
                        hospital_data.appId = app_id
                        self.logger.info(
                            f"Retrieved appId '{app_id}' for database: {db_name}")
                    else:
                        self.logger.warning(
                            f"No appId found for database: {db_name}")

                chatlist_data = db_service.get_chatlist_data()
                if not db_service.connection:
                    self.logger.error("Database connection is None")
                    return hospital_data

                query = '''
                    SELECT DISTINCT up.id as userPatientId, up.chatType, up.language, up.phoneNumber
                    FROM userpatient up 
                    INNER JOIN chatlist cl ON up.id = cl.userPatientId 
                    WHERE up.phoneNumber IS NOT NULL 
                    AND up.phoneNumber != '' 
                    AND up.phoneNumber NOT LIKE '%test%'
                    ORDER BY up.id DESC
                '''

                if self.test_mode:
                    query += f' LIMIT {self.test_record_limit}'
                    self.logger.info(
                        f"Test mode: Limiting query to {self.test_record_limit} records")

                cursor = db_service.connection.cursor()
                cursor.execute(query)
                patients_data = cursor.fetchall()
                cursor.close()

                if patients_data:
                    for patient_row in patients_data:
                        patient = PatientData(
                            userPatientId=patient_row[0],
                            chatType=patient_row[1],
                            language=patient_row[2],
                            phoneNumber=patient_row[3]
                        )
                        hospital_data.patients.append(patient)

                    self.logger.info(
                        f"Found {len(hospital_data.patients)} patients with phone numbers in {hospital_config.get('dbName')}")

                    if self.test_mode:
                        self.logger.warning(
                            f"TEST MODE: Processing only first {len(hospital_data.patients)} records (limit: {self.test_record_limit})")
                else:
                    self.logger.warning(
                        f"No patients found in {hospital_config.get('dbName')}")

        except Exception as e:
            self.logger.error(
                f"Error processing hospital {hospital_config.get('dbName')}: {e}")

        return hospital_data

    def process_all_hospitals(self, db_user: str, db_password: str, db_host: str = 'localhost') -> ProcessedData:
        hospital_configs = self.load_hospital_configs()
        processed_data = ProcessedData(hospitals=[])

        for config in hospital_configs:
            self.logger.info(f"Processing hospital: {config.get('dbName')}")
            hospital_data = self.process_hospital_data(
                config, db_user, db_password, db_host)
            processed_data.hospitals.append(hospital_data)

        return processed_data

    def save_processed_data_to_db(self, data: ProcessedData) -> tuple:
        try:
            self.logger.info(
                "Saving basic data: Each hospital to its own database")

            configs = self.load_hospital_configs()
            total_saved = 0

            for hospital in data.hospitals:
                app_id = hospital.appId or ""
                hospital_db = None
                for config in configs:
                    db_name = config.get('dbName')
                    if db_name:
                        hospital_db = db_name
                        break

                if not hospital_db:
                    self.logger.error(
                        f"Database not found for hospital {app_id}. Skipping...")
                    continue

                self.logger.info(
                    f"Saving hospital {app_id} data to DB: {hospital_db}")

                enriched_patients = []
                for patient in hospital.patients:
                    enriched_patient = EnrichedPatientData(
                        userPatientId=patient.userPatientId,
                        chatType=patient.chatType,
                        language=patient.language,
                        phoneNumber=patient.phoneNumber,
                        patientFound=False,
                        transactionsFound=False
                    )
                    enriched_patients.append(enriched_patient)

                enriched_hospital = EnrichedHospitalData(
                    appId=app_id,
                    originalPatientCount=len(hospital.patients),
                    enrichedPatientCount=0,
                    patients=enriched_patients
                )

                hospital_final_data = FinalProcessedData(
                    timestamp=datetime.now().isoformat(),
                    totalHospitals=1,
                    totalOriginalPatients=len(hospital.patients),
                    totalEnrichedPatients=0,
                    hospitals=[enriched_hospital]
                )

                if self.db_save_service.save_final_data_to_db(hospital_final_data, hospital_db):
                    total_saved += len(hospital.patients)
                    self.logger.info(
                        f"Hospital {app_id} data saved: {len(hospital.patients)} patients")
                else:
                    self.logger.error(
                        f"Failed to save hospital {app_id} data to {hospital_db}")

            self.logger.info(
                f"Basic data save completed: {total_saved} patients saved")
            
            # Get statistics from the database save service
            basic_stats = self.db_save_service.get_global_run_statistics()
            return total_saved > 0, basic_stats

        except Exception as e:
            self.logger.error(f"Error saving processed data to database: {e}")
            return False, {}

    def enrich_data_with_hiys_batch(self, processed_data: ProcessedData) -> tuple:
        self.logger.info("Starting HIYS enrichment with batch processing...")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info("Strategy: Each hospital saves to its own database")

        total_patients_processed = 0
        total_patients_saved = 0
        timestamp = datetime.now().isoformat()
        configs = self.load_hospital_configs()

        for hospital in processed_data.hospitals:
            app_id = hospital.appId or ""
            patients = hospital.patients
            
            # Hospital config'ini bul
            hospital_config = None
            for config in configs:
                if config.get('appId') == app_id:
                    hospital_config = config
                    break
            
            if not hospital_config:
                self.logger.error(f"Configuration not found for hospital {app_id}. Skipping...")
                continue
                
            hospital_db = hospital_config.get('dbName')
            if not hospital_db:
                self.logger.error(f"Database not found for hospital {app_id}. Skipping...")
                continue

            self.logger.info(f"Processing hospital {app_id}: {len(patients)} patients → DB: {hospital_db}")

            # Her hastane için kendi database service'i ile HIYS API service'i oluştur
            try:
                with DatabaseService(
                    database=hospital_db,
                    user=hospital_config.get('username'),
                    password=hospital_config.get('password'),
                    host=hospital_config.get('host', 'localhost')
                ) as db_service:
                    
                    # Database service'i ile HIYS API service'i oluştur
                    hiys_api = HIYSAPIService(database_service=db_service)
                    
                    enriched_patients = []
                    
                    # Process batch processing for this hospital
                    batch_count = 0
                    for i in range(0, len(patients), self.batch_size):
                        batch_patients = patients[i:i + self.batch_size]
                        batch_count += 1

                        self.logger.info(f"Hospital {app_id} - Processing batch {batch_count}: {len(batch_patients)} patients")
                        
                        # HIYS API ile batch process
                        batch_enriched = hiys_api.process_batch(batch_patients, app_id, self.batch_size)
                        enriched_patients.extend(batch_enriched)
                        total_patients_processed += len(batch_patients)

                    if enriched_patients:
                        try:
                            saved, updated, skipped, failed = self.db_save_service.save_batch_to_db_optimized(
                                timestamp=timestamp,
                                app_id=app_id,
                                patients=enriched_patients,
                                db_name=hospital_db
                            )

                            total_patients_saved += saved
                            self.logger.info(f"Hospital {app_id}: {saved} NEW records, {updated} updated, {skipped} skipped, {failed} failed")
                            
                            # DB verification
                            actual_db_count = self._get_actual_db_record_count(hospital_db, app_id, timestamp)
                            if actual_db_count is not None:
                                self.logger.info(f"Hospital {app_id} DB verification: {actual_db_count} total records in database for this run")

                        except Exception as e:
                            self.logger.error(f"Error saving hospital {app_id} data to {hospital_db}: {e}")
                    else:
                        self.logger.info(f"Hospital {app_id}: No patients met the filtering criteria (recent transactions after last message)")

            except Exception as e:
                self.logger.error(f"Error processing hospital {app_id}: {e}")
                continue

        self.logger.info(f"HIYS enrichment completed: {total_patients_saved}/{total_patients_processed} patients saved")
        
        # Get global statistics from database save service
        global_stats = self.db_save_service.get_global_run_statistics()
        actual_added = global_stats.get('new_records', 0)
        actual_updated = global_stats.get('updated_records', 0)
        
        self.logger.info(f"📊 Global Statistics - Added: {actual_added}, Updated: {actual_updated}")
        
        # Final verification
        self.logger.info("Final database verification...")
        for hospital in processed_data.hospitals:
            try:
                app_id = hospital.appId
                hospital_config = None
                for config in configs:
                    if config.get('appId') == app_id:
                        hospital_config = config
                        break

                if hospital_config and hospital_config.get('dbName'):
                    hospital_db = hospital_config.get('dbName')
                    total_records = self._get_total_db_record_count(hospital_db, app_id)
                    recent_records = self._get_recent_db_record_count(hospital_db, app_id)
                    self.logger.info(f"{hospital_db} (appId: {app_id}): {recent_records} new records added, {total_records} total records")
                else:
                    self.logger.warning(f"Could not find database config for appId: {app_id}")

            except Exception as e:
                self.logger.error(f"Error in final verification for hospital {app_id}: {e}")

        if total_patients_processed == 0:
            self.logger.warning("No patients were processed - this might indicate a data issue")
            return False, {}

        if total_patients_saved == 0:
            self.logger.info("No new patients were saved (all were skipped due to filtering criteria) - this is normal")
        else:
            self.logger.info(f"Successfully saved {total_patients_saved} new patients")

        # Return both success status and statistics
        return True, global_stats

    def _get_actual_db_record_count(self, db_name: str, app_id: str, timestamp: str) -> Optional[int]:
        try:
            # DatabaseSaveService'i kullanarak bağlantı kuralım
            connection = self.db_save_service._get_db_connection(db_name)
            cursor = connection.cursor()

            dt = datetime.fromisoformat(
                timestamp.replace('T', ' ').split('.')[0])
            query = """
                SELECT COUNT(*) FROM incomingpatientshiys 
                WHERE appId = %s AND processTimestamp >= %s
            """
            cursor.execute(query, (app_id, dt))
            count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return count

        except Exception as e:
            self.logger.error(f"Error getting actual DB record count: {e}")
            return None

    def _get_total_db_record_count(self, db_name: str, app_id: str) -> Optional[int]:
        try:
            connection = self.db_save_service._get_db_connection(db_name)
            cursor = connection.cursor()

            query = "SELECT COUNT(*) FROM incomingpatientshiys WHERE appId = %s"
            cursor.execute(query, (app_id,))
            count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return count

        except Exception as e:
            self.logger.error(f"Error getting total DB record count: {e}")
            return None

    def _get_recent_db_record_count(self, db_name: str, app_id: str) -> Optional[int]:
        try:
            connection = self.db_save_service._get_db_connection(db_name)
            cursor = connection.cursor()

            query = """
                SELECT COUNT(*) FROM incomingpatientshiys 
                WHERE appId = %s AND processTimestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            """
            cursor.execute(query, (app_id,))
            count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return count

        except Exception as e:
            self.logger.error(f"Error getting recent DB record count: {e}")
            return None

    def _convert_to_final_format(self, processed_data: ProcessedData) -> FinalProcessedData:
        enriched_hospitals = []

        for hospital in processed_data.hospitals:
            enriched_patients = []
            for patient in hospital.patients:
                enriched_patient = EnrichedPatientData(
                    userPatientId=patient.userPatientId,
                    chatType=patient.chatType,
                    language=patient.language,
                    phoneNumber=patient.phoneNumber,
                    patientFound=False,
                    transactionsFound=False
                )
                enriched_patients.append(enriched_patient)

            enriched_hospital = EnrichedHospitalData(
                appId=hospital.appId or "",
                originalPatientCount=len(hospital.patients),
                enrichedPatientCount=0,
                patients=enriched_patients
            )
            enriched_hospitals.append(enriched_hospital)

        total_original = sum(
            h.originalPatientCount for h in enriched_hospitals)

        return FinalProcessedData(
            timestamp=datetime.now().isoformat(),
            totalHospitals=len(enriched_hospitals),
            totalOriginalPatients=total_original,
            totalEnrichedPatients=0,
            hospitals=enriched_hospitals
        )

    def save_final_data_to_db(self, data: FinalProcessedData, db_name: str = 'vatanpratikcrm_db') -> bool:
        try:
            self.logger.info(f"Saving final data to database: {db_name}")
            self.logger.info(
                f"Total hospitals: {data.totalHospitals}, Total patients: {data.totalEnrichedPatients}")

            return self.db_save_service.save_final_data_to_db(data, db_name)

        except Exception as e:
            self.logger.error(f"Error saving final data to database: {e}")
            return False

    def save_final_data(self, data: FinalProcessedData, filename: Optional[str] = None) -> str:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logs/final_enriched_data_{timestamp}.json"

        try:
            data_dict = data.model_dump()

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data_dict, f, ensure_ascii=False,
                          indent=2, default=str)

            self.logger.info(f"Final enriched data saved to: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"Error saving final data to file: {e}")
            return ""
