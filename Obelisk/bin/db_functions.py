import pyodbc
import typing
from Obelisk.config.config_reader import ConfigReader
from settings import sql_username, sql_password


cr = ConfigReader()
DestinationDatabase = cr.get_config(enterprise=True, section="general", variable="DestinationDatabase")
ServerName = cr.get_config(enterprise=True, section="general", variable="ServerName")


def db_connect_f():
    connection = pyodbc.connect(Driver='{SQL SERVER}',
                                Server=ServerName,
                                Database=DestinationDatabase,
                                Trusted_connection='no',
                                UID=sql_username,
                                PWD=sql_password)
    connection.timeout = 86400
    connection = connection.cursor()
    return connection


def db_start_process(ProcessType: str, ProcessName: str, FullLoad: bool) -> int:
    connection = db_connect_f()
    connection.execute(f"""
    DECLARE @ProcessExecutionID INT = NULL
    EXEC dbo.etl_ProcessExecution @ProcessExecutionID = @ProcessExecutionID OUTPUT, 
                                                        @ProcessType = '{ProcessType}',
                                                        @ProcessName = '{ProcessName}',
                                                        @FullLoad = {FullLoad}
    SELECT @ProcessExecutionID AS ProcessExecutionID
    """)
    process_execution_id = connection.fetchval()
    connection.commit()
    connection.close()
    return process_execution_id


def db_complete_process(process_execution_id: int, ProcessType: str, ProcessName: str, FullLoad: bool) -> None:
    connection = db_connect_f()
    connection.execute(f"""
    EXEC dbo.etl_ProcessExecution @ProcessExecutionID = {process_execution_id},
                                  @ProcessType = '{ProcessType}',
                                  @ProcessName = '{ProcessName}',
                                  @FullLoad = {FullLoad}                                  
    """)
    connection.commit()
    connection.close()


def get_process_parameters(process_name: str) -> str:
    connection = db_connect_f()
    connection.execute(f"""
    DECLARE @ProcessParameters NVARCHAR(128)
    EXEC dbo.etl_Process_Parameters_S @ProcessName = '{process_name}', 
                                      @ProcessParameters = @ProcessParameters OUTPUT

    SELECT @ProcessParameters
    """)
    process_parameters = connection.fetchval()
    connection.commit()
    connection.close()
    return process_parameters
