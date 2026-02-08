# -*- coding: utf-8 -*-

import requests
import logging
from typing import Dict, Any
from datetime import datetime
from config.constants import (
    WHATSAPP_API_URL, WHATSAPP_PHONE_NUMBER, 
    ENABLE_WHATSAPP_NOTIFICATIONS, WHATSAPP_REQUEST_TIMEOUT,
    WHATSAPP_VERIFY_SSL
)

# SSL uyarılarını bastır
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WhatsAppService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_url = WHATSAPP_API_URL
        self.phone_number = WHATSAPP_PHONE_NUMBER
        self.enabled = ENABLE_WHATSAPP_NOTIFICATIONS
        self.timeout = WHATSAPP_REQUEST_TIMEOUT
        self.verify_ssl = WHATSAPP_VERIFY_SSL
    
    def send_message(self, message: str) -> bool:
        if not self.enabled or not message:
            return True
        
        try:
            response = requests.post(
                self.api_url,
                json={"to": self.phone_number, "message": message},
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"WhatsApp send error: {e}")
            return False
    
    def create_cron_completion_message(self, results: Dict[str, Any], duration: float) -> str:
        timestamp = datetime.now().strftime('%d.%m.%Y %H:%M')
        enabled_services = {k: v for k, v in results.items() if v.get('enabled', False)}
        failed_services = [k for k, v in enabled_services.items() if not v.get('success', False)]
        
        status = "❌ Başarısız" if failed_services else "✅ Başarılı"
        
        message_parts = [
            f"🤖 Incoming-Patient-Check - {status}",
            f"📅 {timestamp} ({duration:.0f}s)",
            ""
        ]
        
        # Veritabanı istatistikleri
        db_stats = results.get('data_cleanup', {}).get('stats', {})
        proc_stats = results.get('data_processing', {}).get('stats', {})
        
        if db_stats or proc_stats:
            db_name = proc_stats.get('database', 'DB')
            message_parts.append(f"📊 {db_name}:")
            
            # Toplam kontrol edilen hasta sayısı
            patients_count = proc_stats.get('patients', 0)
            message_parts.append(f"  🔍 Kontrol edilen: {patients_count}")
            
            # Toplam DB'ye eklenen yeni kayıt - SIFIRLARDAN DA GÖSTER
            added_count = proc_stats.get('added', 0)
            message_parts.append(f"  ➕ Yeni eklenen: {added_count}")
            
            # Toplam DB'de update edilen kayıt - SIFIRLARDAN DA GÖSTER  
            updated_count = proc_stats.get('updated', 0)
            message_parts.append(f"  🔄 Güncellenen: {updated_count}")
            
            # Toplam DB'de silinen kayıt - SIFIRLARDAN DA GÖSTER
            deleted_count = db_stats.get('deleted_records', 0)
            message_parts.append(f"  🗑️ Silinen: {deleted_count}")
            
            # Temizlenen kayıtlar - SIFIRLARDAN DA GÖSTER
            cleaned_count = db_stats.get('cleaned_records', 0)
            message_parts.append(f"  🧹 Temizlenen: {cleaned_count}")
            
            message_parts.append("")
        
        # Servis durumları
        for service_name, result in enabled_services.items():
            name = service_name.replace('_', ' ').title()
            icon = "✅" if result.get('success', False) else "❌"
            message_parts.append(f"{icon} {name}")
        
        return "\n".join(message_parts)
    
    def send_cron_completion_notification(self, results: Dict[str, Any], duration: float) -> bool:
        try:
            message = self.create_cron_completion_message(results, duration)
            return self.send_message(message)
        except Exception as e:
            self.logger.error(f"Notification error: {e}")
            return False
    
    def send_error_notification(self, error_message: str, service_name: str = None) -> bool:
        timestamp = datetime.now().strftime('%d.%m.%Y %H:%M')
        service_text = f" ({service_name})" if service_name else ""
        message = f"🚨 Cron Hata{service_text}\n📅 {timestamp}\n❌ {error_message}"
        return self.send_message(message)