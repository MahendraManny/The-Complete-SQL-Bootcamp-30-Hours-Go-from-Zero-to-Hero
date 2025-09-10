/*
===========================================
Script:    Data Warehouse Initialization
Author:    [Your Name]
Date:      [Insert Date]
Purpose:   Creates the DataWarehouse database along with
           the Bronze, Silver, and Gold schemas to support
           a multi-layered data architecture for ETL and analytics.
           
Layers:
 - bronze: Raw/staging layer for data ingestion
 - silver: Cleaned and transformed data
 - gold:   Curated, business-ready data for reporting

Usage:
Run this script in SQL Server Management Studio (SSMS)
to initialize the data warehouse structure.

===========================================
*/


-- Create the data warehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Create schemas to organize layers of data processing

-- Bronze Layer: Raw / Staging data (unchanged from source)
CREATE SCHEMA bronze;
GO

-- Silver Layer: Cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Gold Layer: Aggregated / business-ready data
CREATE SCHEMA gold;
GO
