/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'CarrierWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'CarrierWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master; 
GO

-- Drop and recreate the 'CarrierWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CarrierWarehouse')
BEGIN
    ALTER DATABASE CarrierWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CarrierWarehouse;
END;
GO

-- Create the 'CarrierWarehouse' database
CREATE DATABASE CarrierWarehouse;
GO

USE CarrierWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
