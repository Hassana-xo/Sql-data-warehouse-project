/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates three schema called bronze, silver and gold.
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- Drop and recreate the 'DataWarehouseAnalytics' database
DROP DATABASE IF EXISTS DataWarehouseAnalytics;
-- CREATE DATABASE DataWarehouseAnalytics;
-- USE DataWarehouseAnalytics;



-- Create Schemas
DROP SCHEMA IF EXISTS gold;
DROP SCHEMA IF EXISTS Silver;
DROP SCHEMA IF EXISTS Bronze;
CREATE SCHEMA Gold;
CREATE SCHEMA Silver;
CREATE SCHEMA Bronze;
