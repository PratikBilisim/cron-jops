from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime


class ChatListEntry(BaseModel):
    id: int
    userSystemId: int
    userPatientId: int
    messageId: Optional[str]
    quotedMessageId: str
    editedMessageId: Optional[str]
    channelId: Optional[str]
    chatType: Optional[str]
    isEcho: str
    chatId: Optional[str]
    authorName: Optional[str]
    groupSender: Optional[str]
    groupChatId: Optional[str]
    dateTime: Optional[datetime]
    type: Optional[str]
    status: Optional[str]
    text: Optional[str]
    instPostSrc: Optional[str]
    contentUri: Optional[str]
    messageRead: int
    wazz: str


class UserPatient(BaseModel):
    id: int
    userSystemId: int
    identityId: Optional[str]
    fileNumber: Optional[str]
    profileName: str
    avatar: Optional[str]
    name: Optional[str]
    surname: Optional[str]
    channelId: Optional[str]
    chatId: Optional[str]
    chatType: str
    phoneNumber: str
    countryCode: Optional[str]
    mail: Optional[str]
    gender: Optional[int]
    birthDate: Optional[str]
    language: Optional[str]
    registerDate: datetime


class CrmHospital(BaseModel):
    id: int
    domain: str
    title: str
    logo: str
    folder: str
    appId: Optional[str]
    hospitalListId: Optional[str]
    wazzupApi: Optional[str]
    dbName: str
    host: str
    username: str
    password: str
    smsSender: Optional[str]
    smsTitle: Optional[str]
    smsKullaniciAdi: Optional[str]
    smsSifre: Optional[str]
    fbPageId: Optional[str]
    fbLeadgenToken: Optional[str]
    createdAt: Optional[datetime]
    updatedAt: Optional[datetime]
    backApi: Optional[str]
    baileys: Optional[str]
    status: Optional[str]


class PatientData(BaseModel):
    userPatientId: int
    chatType: Optional[str]
    language: Optional[str]
    phoneNumber: str


class HospitalData(BaseModel):
    appId: Optional[str]
    patients: List[PatientData]


class ProcessedData(BaseModel):
    hospitals: List[HospitalData]


class HIYSErrorResponse(BaseModel):
    error: bool
    message: str


class PatientDetail(BaseModel):
    UPN: str
    TCKNo: str
    PassportNo: str
    Name: str
    Surname: str
    FatherName: str
    Gender: str
    BirthDate: str
    PhoneNumber: str
    Email: str


class PatientDetailResponse(BaseModel):
    patients: List[PatientDetail]


class Transaction(BaseModel):
    PtID: str
    DrID: str
    DrName: str
    DrTitleName: str
    DeptID: str
    DeptName: str
    BranchID: str
    BranchName: str
    TransactionDate: str


class TransactionResponse(BaseModel):
    transactions: List[Transaction]
    total_count: int


class EnrichedPatientData(BaseModel):
    userPatientId: int
    chatType: Optional[str] = None
    language: Optional[str] = None
    phoneNumber: str
    patientDetails: Optional[PatientDetail] = None
    transactions: List[Transaction] = []
    transactionCount: int = 0
    patientFound: bool = False
    transactionsFound: bool = False
    ilkMesajTarihi: Optional[datetime] = None


class EnrichedHospitalData(BaseModel):
    appId: str
    originalPatientCount: int
    enrichedPatientCount: int
    patients: List[EnrichedPatientData]


class FinalProcessedData(BaseModel):
    timestamp: str
    totalHospitals: int
    totalOriginalPatients: int
    totalEnrichedPatients: int
    hospitals: List[EnrichedHospitalData]
