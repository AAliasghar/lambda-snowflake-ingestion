/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

/* Identiy Schema in use */
SELECT DB_NAME();
GO

USE CarrierWarehouse;
GO

IF OBJECT_ID('bronze.lmc_data_outbound', 'U') IS NOT NULL
    DROP TABLE bronze.lmc_data_outbound;
GO

CREATE TABLE bronze.lmc_data_outbound (
    SHIPMENTDATE NVARCHAR(50),
    BARCODE NVARCHAR(50),
    ACCOUNTNUMBER NVARCHAR(50),
    INVOICE_NUMBER NVARCHAR(50),
    Consignment NVARCHAR(50),
    REFERENCE NVARCHAR(50),
    WEIGHT NVARCHAR(50),
    VOLUMETRIC_WEIGHT NVARCHAR(50),
    LENGTH NVARCHAR(50),
    WIDTH NVARCHAR(50),
    HEIGHT NVARCHAR(50),
    PRODUCT_TYPE_NAME NVARCHAR(50),
    Destination NVARCHAR(50),
    ZONE NVARCHAR(50),
    BASE_RATE NVARCHAR(50),
    FINAL_PRICE NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.lmc_data_inbound', 'U') IS NOT NULL
    DROP TABLE bronze.lmc_data_inbound;
GO

CREATE TABLE bronze.lmc_data_inbound (
    SHIPMENTDATE NVARCHAR(50),
    Consignment  NVARCHAR(50),
    Customer  NVARCHAR(50),
    INVOICE  NVARCHAR(50),
    REFERENCE  NVARCHAR(50),
    WEIGHT  NVARCHAR(50),
    ORIGIN  NVARCHAR(50),
    DESTINATION  NVARCHAR(50),
    ZONE  NVARCHAR(50),
    VALUE  NVARCHAR(50)
);

