/***************************************************************************
Project Name: BackupFinalDatabases
Dev: GyubeomKim
Date:8/14/2018
Desc: This script shows the procedure of backup for the job, and job script.
ChangeLog: (Who, When, What) 
      GyubeomKim, 8/18/2018, Created Database
*****************************************************************************/
Use [Master]
Go

If(Select Object_ID('[pBackupDatabases]')) Is Not Null Drop Procedure pBackupDatabases;
Go
Create Procedure pBackupDatabases
/* Author: <GyubeomKim>
** Desc: Back up three databases
** Change Log: When,Who,What
** 2018-08-17,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
   	BACKUP DATABASE Patients
    TO DISK = N'C:\BackDatabases\Patients.bak' 
    WITH INIT;

    BACKUP DATABASE DWClnicReportData
    TO DISK = N'C:\BackDatabases\DWClnicReportData.bak' 
    WITH INIT;

    BACKUP DATABASE DoctorSchedules
    TO DISK = N'C:\BackDatabases\DoctorSchedules.bak' 
    WITH INIT;
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
 Exec @Status = pBackupDatabases;
 Print @Status;
*/
--Script For the Job--
USE [msdb]
GO

/****** Object:  Job [BackupFinalDatabases]    Script Date: 8/18/18 6:04:39 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 8/18/18 6:04:39 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'BackupFinalDatabases', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'This Job peforms three backups for the final databases', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'CE33\GyubeomKim', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [BackupProcdedure]    Script Date: 8/18/18 6:04:39 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'BackupProcdedure', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Exec pBackupDatabases;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Once in a Day at 12:01AM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20180818, 
		@active_end_date=99991231, 
		@active_start_time=1, 
		@active_end_time=235959, 
		@schedule_uid=N'9a0688cb-13a7-4a0b-a34e-fd57e90e80f2'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO