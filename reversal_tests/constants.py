"""The field lists are the list of all fields that make sense to group by."""
FULL_SQL_FIELD_LIST = [
    "Succeeded", "UTC Offset", "Access Rule Description", "Analyzed Client IP",
    "Client Host Name", "DB User Name", "Database Name", "OS User",
    "Network Protocol", "Server Host Name", "Server IP", "Server Port",
    "Server Type", "Service Name", "SonarG Source", "Source Program"
]

EXCEPTION_FIELD_LIST = [
    "Destination Address", "Exception Type ID", "User Name"
]

SESSION_FIELD_LIST = [
    "Login Succeeded", "UTC Offset", "Analyzed Client IP", "Client Host Name",
    "DB User Name", "Database Name", "OS User", "Server Host Name", "Server IP",
    "Server Port", "Server Type", "Service Name", "Session Ignored",
    "Source Program",
]

INSTANCE_FIELD_LIST = [
    "UTC Offset", "Analyzed Client IP", "App User Name", "Client Host Name",
    "DB User Name", "Database Name", "Network Protocol", "OS User",
    "Server Host Name", "Server IP", "Server Port", "Server Type",
    "Service Name", "SonarG Source", "Source Program",
]

SESSION_SPECIAL_FIELDS = [
    "Login Succeeded", "Access Id", "Session End", "Client Port", "Uid Chain",
    "Session Ignored", "Sender IP"
]

FULL_GROUP_FIELD_LIST = ['Succeeded', 'UTC Offset', 'Access Rule Description',
                         'Analyzed Client IP', 'Client Host Name',
                         'DB User Name', 'Database Name', 'OS User',
                         'Network Protocol', 'Server Host Name', 'Server IP',
                         'Server Port', 'Server Type', 'Service Name',
                         'SonarG Source', 'Source Program',
                         'Destination Address', 'Exception Type ID',
                         'User Name', 'Login Succeeded', 'UTC Offset',
                         'Analyzed Client IP', 'Client Host Name',
                         'DB User Name', 'Database Name', 'OS User',
                         'Server Host Name', 'Server IP', 'Server Port',
                         'Server Type', 'Service Name', 'Session Ignored',
                         'Source Program', 'UTC Offset', 'Analyzed Client IP',
                         'App User Name', 'Client Host Name', 'DB User Name',
                         'Database Name', 'Network Protocol', 'OS User',
                         'Server Host Name', 'Server IP', 'Server Port',
                         'Server Type', 'Service Name', 'SonarG Source',
                         'Source Program']

FIELD_LIST_DICTIONARY = {"exception": EXCEPTION_FIELD_LIST,
                         "full_sql": FULL_SQL_FIELD_LIST,
                         "session": SESSION_FIELD_LIST,
                         "instance": INSTANCE_FIELD_LIST}

TIME_STAMP_DICTIONARY = {"exception": "Exception Timestamp",
                         "full_sql": "Timestamp",
                         "session": "Session Start",
                         "instance": "Period Start"}
USER_GROUP_DESCRIPTIONS = ["ronosusers", "Suspicious Users",
                           "Dependencies_exclude_schema-Informix",
                           "MS-SQL Database Administrators",
                           "Oracle Predefined Users",
                           "Dependencies_exclude_schema-Oracle",
                           "Oracle exclude default object owners",
                           "Application Schema Users",
                           "Teradata exclude default grantees",
                           "FR - PU DMSS SYSTEM Accounts", "FR - PU DMSS Users",
                           "--Report_ORACLE_Performance Monitoring Server Ips",
                           "Functional Users", "DB Predefined Users",
                           "Functional IDs",
                           "Dependencies_exclude_schema-SybaseIQ",
                           "SAP Hana exclude default grantees", "cbusers",
                           "Admin Users", "--Report_ORACLE_PD01_ClientIP",
                           "Oracle exclude default system grantees",
                           "--Report_ORACLE_FOGLIGHT Client IPs",
                           "Cloudera Manager Default Password",
                           "Dependencies_exclude_schema-DB2LUW",
                           "SAP Hana exclude technical database users",
                           "Dependencies_exclude_schema-DB2zOS",
                           "--Report_ORACLE_DBUser-Temp",
                           "--Report_ORACLE_WEBSRVCP_ClientIPs"]
BSON_FILE_NAME1 = "bson1"
BSON_FILE_NAME2 = "bson2"
C_EXECUTABLE_PATH = '/home/ubuntu/git/qa/QANG/reversal_tests/C/Debug/' \
                    'Bson_comparer'
BSON_PATH = '/local/raid0/sonarw/agg_out/'
