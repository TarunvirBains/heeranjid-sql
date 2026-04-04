-- HeeRanjID MSSQL Installation Script
-- Run with sqlcmd: sqlcmd -S server -d database -i install.sql

:r schema.sql
:r procedures\session.sql
:r procedures\generate_heerid.sql
:r procedures\generate_ranjid.sql
