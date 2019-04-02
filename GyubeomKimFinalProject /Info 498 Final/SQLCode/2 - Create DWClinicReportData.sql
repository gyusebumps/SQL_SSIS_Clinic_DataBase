/***************************************************************************
ETL Final Project: DWClinicReportData
Dev: RRoot
Date:2/21/2017
Desc: This is a Data Warehouse for the Patient and DoctorsSchedule Databases.
	  ETL processing issues.
ChangeLog: (Who, When, What) 
			RRoot, 3/3/17, removed addresses from DimPatients
			RRoot, 3/4/17, removed addresses from DimDoctors and DimClinic
			RRoot, 3/4/17, altered the file description
			RRoot, 3/7/17, added names to all PK and FK constraints
      RRoot, 2/21/18, added SCD columns to DimPatients
*****************************************************************************/
Use Master;
go

If Exists (Select * From Sys.databases where Name = 'DWClinicReportData')
  Begin
   Alter Database DWClinicReportData set single_user with rollback immediate;
   Drop Database DWClinicReportData;
  End
go

Create Database DWClinicReportData;
go

Use DWClinicReportData;
go

Create Table DimDates -- Type 1 SCD
(DateKey int Constraint pkDimDates Primary Key Identity 
,FullDate datetime Not Null
,FullDateName nvarchar (50) Not Null 
,MonthID int Not Null
,[MonthName] nvarchar(50) Not Null
,YearID int Not Null
,YearName nvarchar(50) Not Null
);
go

Create Table DimClinics -- Type 1 SCD
(ClinicKey int Constraint pkDimClinics Primary Key Identity
,ClinicID int Not Null
,ClinicName nvarchar(100) Not Null 
,ClinicCity nvarchar(100) Not Null
,ClinicState nvarchar(100) Not Null 
,ClinicZip nvarchar(5) Not Null 
);
go

Create Table DimDoctors -- Type 1 SCD
(DoctorKey int Constraint pkDimDoctors Primary Key Identity
,DoctorID int Not Null  
,DoctorFullName nvarchar(200) Not Null 
,DoctorEmailAddress nvarchar(100) Not Null  
,DoctorCity nvarchar(100) Not Null
,DoctorState nvarchar(100) Not Null
,DoctorZip nvarchar(5) Not Null 
);
go

Create Table DimShifts -- Type 1 SCD
(ShiftKey int Constraint pkDimShifts Primary Key Identity
,ShiftID int Not Null
,ShiftStart time(0) Not Null
,ShiftEnd time(0) Not Null
);
go

Create Table FactDoctorShifts -- Type 1 SCD
(DoctorsShiftID int Not Null
,ShiftDateKey int Constraint fkFactDoctorShiftsToDimDates References DimDates(DateKey) Not Null
,ClinicKey int Constraint fkFactDoctorShiftsToDimClinics References DimClinics(ClinicKey) Not Null
,ShiftKey int Constraint fkFactDoctorShiftsToDimShifts References DimShifts(ShiftKey) Not Null
,DoctorKey int Constraint fkFactDoctorShiftsToDimDoctors References DimDoctors(DoctorKey) Not Null
,HoursWorked int
Constraint pkFactDoctorShifts Primary Key(DoctorsShiftID, ShiftDateKey , ClinicKey, ShiftKey, DoctorKey)
);
go

Create Table DimProcedures -- Type 1 SCD
(ProcedureKey int Constraint pkDimProcedures Primary Key Identity
,ProcedureID int Not Null
,ProcedureName varchar(100) Not Null
,ProcedureDesc varchar(1000) Not Null
,ProcedureCharge money Not Null 
);
go

Create Table DimPatients -- Type 2 SCD
(PatientKey int Constraint pkDimPatients Primary Key Identity
,PatientID int Not Null
,PatientFullName varchar(100) Not Null
,PatientCity varchar(100) Not Null
,PatientState varchar(100) Not Null
,PatientZipCode int Not Null
,StartDate date Not Null
,EndDate date Null
,IsCurrent int Constraint ckDimPatientsIsCurrent Check (IsCurrent In (1,0))
);
go

Create Table FactVisits -- Type 1 SCD
(VisitKey int Not Null
,DateKey int Constraint fkFactVisitsToDimDates References DimDates(DateKey) Not Null
,ClinicKey int Constraint fkFactVisitsToDimClinics References DimClinics(ClinicKey) Not Null
,PatientKey int Constraint fkFactVisitsToDimPatients References DimPatients(PatientKey) Not Null
,DoctorKey int Constraint fkFactVisitsToDimDoctors References DimDoctors(DoctorKey) Not Null
,ProcedureKey int Constraint fkFactVisitsToDimProcedures References DimProcedures(ProcedureKey) Not Null 
,ProcedureVistCharge money Not Null
Constraint pkFactVisits Primary Key(VisitKey, DateKey, ClinicKey, PatientKey, DoctorKey, ProcedureKey)
);
go
