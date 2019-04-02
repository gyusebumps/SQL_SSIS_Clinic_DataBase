/***************************************************************************
Project Name: FileToOLTPDatabase
Dev: GyubeomKim
Date:8/14/2018
Desc: This script put file data into OLTP Database
ChangeLog: (Who, When, What) 
      GyubeomKim, 8/14/2018, Created Database
*****************************************************************************/
USE [tempdb]
GO

If(Select Object_ID('[pETLCreateOrClearAllStagingTables]')) Is Not Null Drop Procedure pETLCreateOrClearAllStagingTables;
Go
Create Procedure pETLCreateOrClearAllStagingTables
As
 /* Author: <GyubeomKim>
 ** Desc: Creates or Clear All Staging Tables
 ** Change Log: When,Who,What
 ** 2018-08-15,<GyubeomKim>,Created Sproc.
 */
  Begin
  If(Select Object_ID('[dbo].[StagingBellevue]')) Is Not Null
   Truncate Table [dbo].[StagingBellevue];
  Else
  CREATE TABLE [dbo].[StagingBellevue](
	[Time] [nvarchar](100) NULL,
	[Patient] [nvarchar](100) NULL,
	[Doctor] [nvarchar](100) NULL,
	[Procedure] [nvarchar](100) NULL,
	[Charge] [nvarchar](100) NULL
   ); 
   If(Select Object_ID('[dbo].[StagingKirkland]')) Is Not Null 
    Truncate Table [dbo].[StagingKirkland];
   Else
   CREATE TABLE [dbo].[StagingKirkland](
	[Time] [nvarchar](100) NULL,
	[Patient] [nvarchar](100) NULL,
	[Clinic] [nvarchar](100) NULL,
	[Doctor] [nvarchar](100) NULL,
	[Procedure] [nvarchar](100) NULL,
	[Charge] [nvarchar](100) NULL
   );
   If(Select Object_ID('[dbo].[StagingRedmond]')) Is Not Null 
    Truncate Table [dbo].[StagingRedmond];
   Else
   CREATE TABLE [dbo].[StagingRedmond](
	[Time] [nvarchar](100) NULL,
	[Clinic] [nvarchar](100) NULL,
	[Patient] [nvarchar](100) NULL,
	[Doctor] [nvarchar](100) NULL,
	[Procedure] [nvarchar](100) NULL,
	[Charge] [nvarchar](100) NULL
   );
  Declare @RC int = 0;
  Begin Try
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

If(Select Object_ID('pETLImportDataToStagingTables')) Is Not Null Drop Procedure pETLImportDataToStagingTables;
Go
Create Procedure pETLImportDataToStagingTables
As
 /* Author: <GyubeomKim>
 ** Desc: Import csv data
 ** Change Log: When,Who,What
 ** 2018-08-15,<GyubeomKim>,Created Sproc.
 */
  Begin
   Declare @RC int = 0;
   Begin Try
  --ETL Processing--
  BULK INSERT [dbo].[StagingBellevue]
  FROM 'C:\Bellevue\20100102Visits.csv'
  WITH 
    (FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     ROWTERMINATOR = '\n'
    );

  BULK INSERT [dbo].[StagingKirkland]
  FROM 'C:\Kirkland\20100102Visits.csv'
  WITH 
    (FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     ROWTERMINATOR = '\n'
    );
  BULK INSERT [dbo].[StagingRedmond]
  FROM 'C:\Redmond\20100102Visits.csv'
  WITH 
    (FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     ROWTERMINATOR = '\n'
    );
  --ETL Processing--
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

Execute pETLCreateOrClearAllStagingTables;
Go

Execute pETLImportDataToStagingTables;
Go
--Current Data--
Select * From [dbo].[StagingBellevue];
Select * From [dbo].[StagingKirkland];
Select * From [dbo].[StagingRedmond];

--Use different database--
Use Patients;
Go

--Create Three Table as a View--
If(Select Object_ID('vETLNewVisitData')) Is Not Null Drop View vETLNewVisitData;
Go
Create View vETLNewVisitData
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for MergeDataBKR
** Change Log: When,Who,What
** 2018-08-14,<GyubeomKim>,Created Sproc.
*/
As
  SELECT [Time] AS [Date], [Clinic] = 1, [Patient], [Doctor], [Procedure], [Charge]
   From [tempdb].[dbo].[StagingBellevue] 
   Union All
  SELECT [Time] AS [Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge]
   From [tempdb].[dbo].[StagingKirkland]
   Union All 
  SELECT [Time] AS [Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge]
   From [tempdb].[dbo].[StagingRedmond] 
GO

Select * From [VETLNewVisitData];
Go

--Merge Into Visits Table--
If(Select Object_ID('pETLSyncVisit')) Is Not Null Drop Procedure pETLSyncVisit;
Go
Create Procedure pETLSyncVisit
/* Author: <GyubeomKim>
** Desc: Inserts data into Visits
** Change Log: When,Who,What
** 2018-08-15,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    --ETL Processing--
	With NewRows
	As (
	 Select
      [Date] = Cast('2010-01-02' + ' ' + [Date] as datetime) 
     ,[Clinic] = Cast([Clinic] as int) * 100
     ,[Patient] = Cast([Patient] as int)
     ,[Doctor] = Cast([Doctor] as int)
     ,[Procedure] = Cast([Procedure] as int)
     ,[Charge] = Cast([Charge] as money)
	 From [VETLNewVisitData]
	Except
	Select [Date], [Clinic], [Patient],[Doctor], [Procedure], [Charge]
	 From [Patients].[dbo].Visits
  )
  Insert Into [Patients].[dbo].Visits
  ([Date], [Clinic], [Patient],[Doctor], [Procedure], [Charge])
  Select
  [Date], [Clinic], [Patient],[Doctor], [Procedure], [Charge]
  From NewRows
  Order By 1,2,3,4,5,6
  --ETL Processing--
  Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

--Review The Result--
Declare @Status int;
Exec @Status = pETLSyncVisit;
Select [Object] = 'pETLSyncVisit', [Status] = Case @Status
	  When +1 Then 'ETL to syncronize Visit table was successful!'
	  When -1 Then 'ETL to syncronize Visit table failed! Common Issues: Tables missing'
	  End

--Current Result--
Select * From [Patients].[dbo].[Visits];