/***************************************************************************
Project Name: OLTPToOLTPDWDatabase
Dev: GyubeomKim
Date:8/14/2018
Desc: This script put OLTP into DWDatabase
ChangeLog: (Who, When, What) 
      GyubeomKim, 8/14/2018, Created Database
	  GyuboemKim, 8/15/2018, Added error messages for the status
*****************************************************************************/
USE DWClinicReportData;
go

SET NoCount ON;
go

	If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimClinics')
   Drop View vETLDimClinics;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimClinics')
   Drop Procedure pETLFillDimClinics;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimDocotrs')
   Drop View vETLDimDocotrs;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDoctors')
   Drop Procedure pETLFillDimDoctors;
go
   If Exists(Select * from Sys.objects where Name = 'vETLDimPatients')
   Drop View vETLDimPatients;
go
   If Exists(Select * from Sys.objects where Name = 'pETLSyncDimPatients')
   Drop Procedure pETLSyncDimPatients;
go
   If Exists(Select * from Sys.objects where Name = 'vETLDimProcedures')
   Drop View vETLDimProcedures;
go
   If Exists(Select * from Sys.objects where Name = 'pETLFillDimProcedures')
   Drop Procedure pETLFillDimProcedures;
go
   If Exists(Select * from Sys.objects where Name = 'vETLDimShifts')
   Drop View vETLDimShifts;
go
   If Exists(Select * from Sys.objects where Name = 'pETLFillDimShifts')
   Drop Procedure pETLFillDimShifts;
go
   If Exists(Select * from Sys.objects where Name = 'vETLFactDoctorShifts')
   Drop View vETLFactDoctorShifts;
go
   If Exists(Select * from Sys.objects where Name = 'pETLFillFactDoctorShifts')
   Drop Procedure pETLFillFactDoctorShifts;
go
   If Exists(Select * from Sys.objects where Name = 'vETLFactVisits')
   Drop View vETLFactVisits;
go
   If Exists(Select * from Sys.objects where Name = 'pETLFillFactVisits')
   Drop Procedure pETLFillFactVisits;
go
   If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;
--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--
go
Create Procedure pETLDropForeignKeyConstraints
/* Author: <GyubeomKim>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	--Fact Visits Table--
    Alter Table [dbo].[FactVisits]
	  Drop CONSTRAINT [fkFactVisitsToDimClinics]; 
	
	ALTER TABLE [dbo].[FactVisits]
	  Drop CONSTRAINT [fkFactVisitsToDimDoctors];
   
    ALTER TABLE [dbo].[FactVisits]
	  Drop CONSTRAINT [fkFactVisitsToDimPatients]; 

    ALTER TABLE [dbo].[FactVisits]
	  Drop CONSTRAINT [fkFactVisitsToDimProcedures]; 
    --FactDoctorShifts--
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Drop CONSTRAINT [fkFactDoctorShiftsToDimClinics]; 
		
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Drop CONSTRAINT [fkFactDoctorShiftsToDimDoctors]; 
	
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Drop CONSTRAINT [fkFactDoctorShiftsToDimShifts]; 
    -- Optional: Unlike the other tables DimDates does not change often --
 	ALTER TABLE [dbo].[FactVisits]
	  Drop CONSTRAINT [fkFactVisitsToDimDates];
	
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Drop CONSTRAINT [fkFactDoctorShiftsToDimDates]; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

Create Procedure pETLTruncateTables
/* Author: <GyubeomKim>
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Truncate Table [DWClinicReportData].dbo.DimClinics;
    Truncate Table [DWClinicReportData].dbo.DimDoctors;
	Truncate Table [DWClinicReportData].dbo.DimPatients;
	Truncate Table [DWClinicReportData].dbo.DimProcedures;
	Truncate Table [DWClinicReportData].dbo.DimShifts;
	Truncate Table [DWClinicReportData].dbo.FactDoctorShifts;
	Truncate Table [DWClinicReportData].dbo.FactVisits;  
    -- Optional: Unlike the other tables DimDates does not change often --
    Truncate Table [DWClinicReportData].dbo.DimDates; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/*Testing Code:
 Declare @Status int;
 Exec @Status = pETLTruncateTables;
 Print @Status;
*/

--********************************************************************--
-- B) FILL the Tables
--********************************************************************--
/****** [dbo].[DimClinics] ******/
go 
Create View vETLDimClinics
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimClinics
** Change Log: When,Who,What
** 2018-08-14,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [ClinicID] = Cast(DC.ClinicID as int)
   ,[ClinicName] = Cast(IsNull(DC.ClinicName, 'Missing Data') as nvarchar(100))
   ,[ClinicCity] = Cast(IsNull(DC.City, 'Missing Data') as nvarchar(100))
   ,[ClinicState] = Cast(IsNull(DC.[State], 'Missing Data') as nvarchar(100))
   ,[ClinicZip] = Cast(IsNull(DC.Zip, 'Missing Data') as nvarchar(5))
  FROM [DoctorsSchedules].dbo.Clinics as DC
go
/* Testing Code:
 Select * From vETLDimClinics;
*/
go

Create Procedure pETLFillDimClinics
/* Author: <GyubeomKim>
** Desc: Inserts data into DimClinics using the vETLDimClinics
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimClinics) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.DimClinics
      ([ClinicID],[ClinicName],[ClinicCity],[ClinicState],[ClinicZip])
      SELECT
        [ClinicID]
       ,[ClinicName]
       ,[ClinicCity]
       ,[ClinicState]
       ,[ClinicZip]
      FROM vETLDimClinics
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimClinics;
 Print @Status;
 Select * From DimClinics;
*/

/****** [dbo].[DimDates] ******/
go
Create Procedure pETLFillDimDates
/* Author: <GyubeomKim>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '01/01/2020' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       SET IDENTITY_INSERT [dbo].[DimDates] ON; 
	   Insert Into [dbo].[DimDates]
       ( [DateKey], [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
		,@DateInProcess--[FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(100), @DateInProcess, 110) -- [FullDateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/

/****** [dbo].[DimDoctors] ******/
go 
Create View vETLDimDocotrs
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimDocotrs
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [DoctorID] = Cast(DD.DoctorID as int) 
   ,[DoctorFullName] = Cast(IsNull(DD.FirstName + ' ' + DD.LastName,'Missing Data') as nvarchar(200))
   ,[DoctorEmailAddress] = Cast(IsNull(EmailAddress,'Missing Data') as nvarchar(100))
   ,[DoctorCity] = Cast(IsNull(DD.City,'Missing Data') as nvarchar(100))
   ,[DoctorState] = Cast(IsNull(DD.[State],'Missing Data') as nvarchar(100))
   ,[DoctorZip] = Cast(IsNull(DD.Zip,'Missing Data') as nvarchar(5))
  FROM [DoctorsSchedules].[dbo].[Doctors] as DD
go
/* Testing Code:
 Select * From vETLDimDocotrs;
*/
go

Create Procedure pETLFillDimDoctors
/* Author: <GyubeomKim>
** Desc: Inserts data into DimDoctors
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From DimDoctors) = 0)
      INSERT INTO [DWClinicReportData].dbo.DimDoctors
      ([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
      SELECT
        [DoctorID]
       ,[DoctorFullName]
       ,[DoctorEmailAddress]
	   ,[DoctorCity]
	   ,[DoctorState]
       ,[DoctorZip]
      FROM vETLDimDocotrs
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDoctors;
 Print @Status;
  Select * From DimDoctors;
*/

/****** [dbo].[DimPatients] ******/
go 
Create View vETLDimPatients
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimPatients
** Change Log: When,Who,What
** 2018-08-14,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [PatientID] = CAST(PP.ID as int)
   ,[PatientFullName] = CAST(IsNull(PP.FName + ' ' + PP.LName, 'Missing Data') as varchar(100))
   ,[PatientCity] = CAST(IsNull(PP.City,'Missing Data') as varchar(100))
   ,[PatientState] = CAST(IsNull(PP.[State], 'Missing Data') as varchar(100))
   ,[PatientZipCode] = CAST(IsNull(PP.ZipCode, 0) as int)
  FROM [Patients].dbo.Patients as PP
go
/* Testing Code:
 Select * From vETLDimPatients;
*/
go

Create Procedure pETLSyncDimPatients
/* Author: <GyubeomKim>
** Desc: Inserts data into DimPaitients
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
	With ChangedPatients 
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
			Except
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode]From DimPatients
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWClinicReportData].dbo.DimPatients 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
         ,IsCurrent = 0
       WHERE PatientID IN (Select PatientID From ChangedPatients)
    ;

    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedPatients 
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
			Except
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode]From DimPatients
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO DWClinicReportData.dbo.DimPatients
      ([PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode], [StartDate], [EndDate], [IsCurrent])
      SELECT
        [PatientID]
       ,[PatientFullName]
       ,[PatientCity]
       ,[PatientState]
	   ,[PatientZipCode]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimPatients
      WHERE PatientID IN (Select PatientID From AddedORChangedPatients)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedPatients 
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode]From DimPatients
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
   	)UPDATE DWClinicReportData.dbo.DimPatients 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
         ,IsCurrent = 0
       WHERE PatientID IN (Select PatientID From DeletedPatients)
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimPatients;
 Print @Status;
*/
go



/****** [dbo].[DimProcedures] ******/
go 
Create View vETLDimProcedures
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimProcdedures
** Change Log: When,Who,What
** 2018-08-14,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [ProcedureID] = Cast(PPR.ID as int)
   ,[ProcedureName] = Cast(IsNull(PPR.[Name], 'Missing Data') as varchar(100))
   ,[ProcedureDesc] = Cast(IsNull(PPR.[Desc], 'Missing Data') as varchar(100))
   ,[ProcedureCharge] = Cast(IsNull(PPR.Charge, 0) as money)
  FROM [Patients].dbo.[Procedures] as PPR
go
/* Testing Code:
 Select * From vETLDimProcedures;
*/
go

Create Procedure pETLFillDimProcedures
/* Author: <GyubeomKim>
** Desc: Inserts data into DimProcdedures
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From DimProcedures) = 0)
      INSERT INTO [DWClinicReportData].dbo.DimProcedures
      ([ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge])
      SELECT
        [ProcedureID]
       ,[ProcedureName]
       ,[ProcedureDesc]
	   ,[ProcedureCharge]
      FROM vETLDimProcedures
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimProcedures;
 Print @Status;
  Select * From DimProcedures;
*/

/****** [dbo].[DimShift] ******/
go 
Create View vETLDimShifts
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimShift
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [ShiftID] = Cast(DSS.ShiftID as int)
   ,[ShiftStart] = Case When Cast(DSS.ShiftStart as time(0)) = '09:00:00' Then '09:00:00' 
   When Cast(DSS.ShiftStart as time(0)) = '01:00:00' Then '13:00:00'
   When Cast(DSS.ShiftStart as time(0)) = '21:00:00' Then '21:00:00'
   End
   ,[ShiftEnd] = Case When Cast(DSS.ShiftEnd as time(0)) = '05:00:00' Then '17:00:00'
   When Cast(DSS.ShiftEnd as time(0)) = '21:00:00' Then '21:00:00'
   When Cast(DSS.ShiftEnd as time(0)) = '09:00:00' Then '09:00:00'
   End
  FROM [DoctorsSchedules].dbo.Shifts as DSS
go
/* Testing Code:
 Select * From vETLDimShifts;
*/
go

Create Procedure pETLFillDimShifts
/* Author: <GyubeomKim>
** Desc: Inserts data into DimShifts
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From DimShifts) = 0)
      INSERT INTO [DWClinicReportData].dbo.DimShifts
      ([ShiftID], [ShiftStart], [ShiftEnd])
      SELECT
        [ShiftID]
       ,[ShiftStart]
       ,[ShiftEnd]
      FROM vETLDimShifts
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimShifts;
 Print @Status;
  Select * From DimShifts;
*/

/****** [dbo].[FactDoctorShifts] ******/
go 
Create View vETLFactDoctorShifts
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for FactDoctorShifts
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [DoctorsShiftID] = Cast(DSD.DoctorsShiftID as int)
   ,[ShiftDateKey] = Cast(DDA.DateKey as int)
   ,[ClinicKey] = Cast(DC.ClinicKey as int)
   ,[ShiftKey] = Cast(DS.ShiftKey as int)
   ,[DoctorKey] = Cast(DD.DoctorID as int)
   ,[HoursWorked] = abs(Cast(Datediff(hour,DS.ShiftStart,DS.ShiftEnd) as int)) 
  FROM [DoctorsSchedules].dbo.DoctorShifts as DSD
  JOIN [DWClinicReportData].dbo.DimClinics as DC
  ON DSD.ClinicID = DC.ClinicID
  JOIN [DWClinicReportData].dbo.DimShifts as DS
  On DSD.ShiftID = DS.ShiftID
  JOIN [DWClinicReportData].dbo.DimDoctors as DD
  On DSD.DoctorID = DD.DoctorID
  JOIN [DWClinicReportData].dbo.DimDates as DDA
  On Cast(Convert(nVarchar(50), DSD.ShiftDate, 112) as int) = DDA.DateKey;
go
/* Testing Code:
 Select * From vETLFactDoctorShifts;
*/
go

Create Procedure pETLFillFactDoctorShifts
/* Author: <GyubeomKim>
** Desc: Inserts data into FactDoctorShifts
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From FactDoctorShifts) = 0)
      INSERT INTO [DWClinicReportData].dbo.FactDoctorShifts
      ([DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked])
      SELECT
        [DoctorsShiftID]
       ,[ShiftDateKey]
       ,[ClinicKey]
	   ,[ShiftKey]
	   ,[DoctorKey]
       ,[HoursWorked]
      FROM vETLFactDoctorShifts
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactDoctorShifts;
 Print @Status;
  Select * From FactDoctorShifts;
*/

/****** [dbo].[FactVisits] ******/
go 
Create View vETLFactVisits
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for FactVisits
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [VisitKey] = Cast(PV.ID as int)
   ,[DateKey] = Cast(DDA.DateKey as int)
   ,[ClinicKey] = Cast(DC.ClinicKey as int)
   ,[PatientKey] = Cast(DP.PatientKey as int)
   ,[DoctorKey] =  Cast(DD.DoctorKey as int)
   ,[ProcedureKey] = Cast(DPR.ProcedureKey as int)
   ,[ProcedureVistCharge] = Cast(PV.Charge as money)
  FROM [Patients].dbo.Visits as PV
  JOIN [DWClinicReportData].dbo.DimClinics as DC
  ON PV.Clinic = Cast(DC.ClinicID as int) * 100
  JOIN [DWClinicReportData].dbo.DimPatients as DP
  On PV.Patient = DP.PatientKey
  JOIN [DWClinicReportData].dbo.DimDoctors as DD
  On PV.Doctor = DD.DoctorKey
  JOIN [DWClinicReportData].dbo.DimProcedures as DPR
  On PV.[Procedure] =  DPR.ProcedureKey
  JOIN [DWClinicReportData].dbo.DimDates as DDA
  On Cast(Convert(nVarchar(50), PV.[Date], 112) as int) = DDA.DateKey;
go
/* Testing Code:
 Select * From vETLFactVisits;
 Select * From Patients.dbo.Visits;
*/
go

Create Procedure pETLFillFactVisits
/* Author: <GyubeomKim>
** Desc: Inserts data into FactVisits
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From FactVisits) = 0)
      INSERT INTO [DWClinicReportData].dbo.FactVisits
      ([VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
      SELECT
        [VisitKey]
       ,[DateKey]
       ,[ClinicKey]
	   ,[PatientKey]
	   ,[DoctorKey]
       ,[ProcedureKey]
	   ,[ProcedureVistCharge]
      FROM vETLFactVisits
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactVisits;
 Print @Status;
  Select * From FactVisits;
*/

--********************************************************************--
-- C) Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
go

Create Procedure pETLAddForeignKeyConstraints
/* Author: <GyubeomKim>
** Desc: add FKs for the tables
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	--Fact Visits Table--
    Alter Table [dbo].[FactVisits]
	  Add Constraint [fkFactVisitsToDimClinics]
	  FOREIGN KEY (ClinicKey) REFERENCES DimClinics(ClinicKey); 

	ALTER TABLE [dbo].[FactVisits]
	  Add CONSTRAINT [fkFactVisitsToDimDoctors]
	  FOREIGN KEY (DoctorKey) REFERENCES DimDoctors(DoctorKey);
   
    ALTER TABLE [dbo].[FactVisits]
	  Add CONSTRAINT [fkFactVisitsToDimPatients]
	  FOREIGN KEY (PatientKey) REFERENCES DimPatients(PatientKey);

    ALTER TABLE [dbo].[FactVisits]
	  Add CONSTRAINT [fkFactVisitsToDimProcedures]
	  FOREIGN KEY (ProcedureKey) REFERENCES DimProcedures(ProcedureKey);

    --FactDoctorShifts--
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Add CONSTRAINT [fkFactDoctorShiftsToDimClinics]
	  FOREIGN KEY (ClinicKey) REFERENCES DimClinics(ClinicKey); 
		
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Add CONSTRAINT [fkFactDoctorShiftsToDimDoctors]
	  FOREIGN KEY (DoctorKey) REFERENCES DimDoctors(DoctorKey);  
	
	ALTER TABLE [dbo].[FactDoctorShifts]
	  Add CONSTRAINT [fkFactDoctorShiftsToDimShifts]
	  FOREIGN KEY (ShiftKey) REFERENCES DimShifts(ShiftKey); 
    -- Optional: Unlike the other tables DimDates does not change often --
 	ALTER TABLE [dbo].[FactVisits]
	  Add CONSTRAINT [fkFactVisitsToDimDates]
	  FOREIGN KEY (DateKey) REFERENCES DimDates(DateKey); 

	ALTER TABLE [dbo].[FactDoctorShifts]
	  Add CONSTRAINT [fkFactDoctorShiftsToDimDates]
	  FOREIGN KEY (ShiftDateKey) REFERENCES DimDates(DateKey); 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLAddForeignKeyConstraints;
 Print @Status;
*/
--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int;
Exec @Status = pETLDropForeignKeyConstraints;
Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = Case @Status
	  When +1 Then 'ETL to Drop Foriegn Keys was successful!'
	  When -1 Then 'ETL to Drop Foriegn Keys failed! Common Issues: FKs have already been dropped'
	  End

Exec @Status = pETLTruncateTables;
Select [Object] = 'pETLTruncateTables', [Status] = Case @Status
	  When +1 Then 'ETL to Truncate Tables was successful!'
	  When -1 Then 'ETL to Truncate Tables failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimClinics;
Select [Object] = 'pETLFillDimCustomers', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimClinics table was successful!'
	  When -1 Then 'ETL to Fill DimClinics table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimDates table was successful!'
	  When -1 Then 'ETL to Fill DimDates table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimDoctors;
Select [Object] = 'pETLFillDimDoctors', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimDoctors table was successful!'
	  When -1 Then 'ETL to Drop DimDoctors table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = Case @Status
	  When +1 Then 'ETL to Sync tables successful!'
	  When -1 Then 'ETL to Sync tables failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimProcedures;
Select [Object] = 'pETLFillDimProcedures', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimProcedures table was successful!'
	  When -1 Then 'ETL to Drop DimProcedures table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimShifts;
Select [Object] = 'pETLFillDimShifts', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimShifts table was successful!'
	  When -1 Then 'ETL to Drop DimShifts table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillFactDoctorShifts;
Select [Object] = 'pETLFillFactDoctorShifts', [Status] = Case @Status
	  When +1 Then 'ETL to Fill FactDoctorShifts table was successful!'
	  When -1 Then 'ETL to Drop FactDoctorShifts table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillFactVisits;
Select [Object] = 'pETLFillFactVisits', [Status] = Case @Status
	  When +1 Then 'ETL to Fill FactVisits table was successful!'
	  When -1 Then 'ETL to Drop FactVisits table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLAddForeignKeyConstraints;
Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = Case @Status
	  When +1 Then 'ETL to add Foriegn Keys was successful!'
	  When -1 Then 'ETL to add Foriegn Keys failed! Common Issues: FKs have already been added'
	  End
go

--SDC TYPE 2 Check--
Declare @Status int;
Insert Into Patients.dbo.Patients
Values('Insert Test', ' ', 'GB@uw.edu', '902 NE Apt502', 'Seattle', 'WA', 98105)
Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = Case @Status
	  When +1 Then 'Insert successful!'
	  When -1 Then 'Insert failed!'
	  End

Update Patients.dbo.Patients
SET FName = 'Update Test'
Where FName = 'Insert Test'
Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = Case @Status
	  When +1 Then 'Update successful!'
	  When -1 Then 'Update failed!'
	  End

Delete Patients.dbo.Patients
Where FName = 'Update Test'
Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = Case @Status
	  When +1 Then 'Delete successful!'
	  When -1 Then 'Delete failed!'
	  End
Go

--Current Tables Value--
Select * From [dbo].[DimClinics];
Select * From [dbo].[DimDates];
Select * From [dbo].[DimDoctors];
Select * From [dbo].[DimPatients];
Select * From [dbo].[DimProcedures];
Select * From [dbo].[DimShifts];
Select * From [dbo].[FactDoctorShifts];
Select * From [dbo].[FactVisits];
go