--Create database
create database dataclean

--Create table company_employee

CREATE TABLE IF NOT EXISTS company_employee (
    company TEXT NOT NULL,
    location TEXT NOT NULL,
    industry TEXT,
    total_laid_off DOUBLE PRECISION,
    percentage_laid_off DOUBLE PRECISION,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions DOUBLE PRECISION,
)

--import .CSV file from your local to postgres, their are many ways to import csv file but i prefer this way
COPY company_employee
FROM 'C:\Swati\study\sql\SQL portfolio project_Blog\company_employee.csv'
DELIMITER ','
CSV HEADER
NULL 'NULL';

select * from company_employee_staging;

-- It is good practice to create dummy table instead of working on raw table, i am creating company_employee_staging and duplicating company_employee table
Create table company_employee_staging AS 
select * from company_employee;

-- row number is unique in below ss
select *,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry, total_laid_off,stage, country, percentage_laid_off, date, funds_raised_millions) AS row_num
from company_employee_staging;

/**************************** Handle Duplicates ********************/
--now we need to filter out where row_num is > 1 , if it is greater than 1 that means it has duplicates
--let's create CTE to filter out duplicate, if you run this you will see total 5 rows are dulicate which has row_num = 2 value
with duplicate_cte AS
(
select *,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry, total_laid_off,stage, country, percentage_laid_off, date, funds_raised_millions) AS row_num
from company_employee_staging
)
select * from duplicate_cte
where row_num > 1;


--let's check one of the company to see duplicates, now we don't need to delete all of the below rows, we just want to delete duplicates. 
select * 
from company_employee_staging
where company = 'Casper';

--let's delete duplicates where row_num > 1 , 
delete 
from company_employee_staging
where row_num > 1;

--Let's create another table to delete dulicates, this time will add one new column 'row_num'. Empty table got created 

CREATE TABLE IF NOT EXISTS employee_layoffs (
    company TEXT NOT NULL,
    location TEXT NOT NULL,
    industry TEXT,
    total_laid_off DOUBLE PRECISION,
    percentage_laid_off DOUBLE PRECISION,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions DOUBLE PRECISION,
	row_num INT
);

select * from employee_layoffs;

--Insert row_num partition table into employee_layoffs table
insert into employee_layoffs
select *,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry, total_laid_off,stage, country, percentage_laid_off, date, funds_raised_millions) AS row_num
from company_employee_staging;


select * from employee_layoffs;


--let's identify duplicates from employee_layoffs table
select * 
from employee_layoffs
where row_num > 1;

--delete all 5 duplicate rows
delete 
from employee_layoffs
where row_num > 1;

select * 
from employee_layoffs
where row_num > 1;

/******************************** Standardize Data ********************/
--Remove white space from company column using trim() function
select company, TRIM(company)
from employee_layoffs;


--now update column company with Trim(company) column

update employee_layoffs
set company = TRIM(company);

select * from employee_layoffs;

--similarly clean/update industry column, where you will see lot of rows have crypto and Crypto Currency, i want to update Crypto Currency as Crypto in industry column
select *
from employee_layoffs
where industry like 'Crypto%';

update employee_layoffs
set industry = 'Crypto'
where industry like 'Crypto%';

--Run below query to  validate column
select distinct industry
from employee_layoffs;

--Similarly look into location column, if you don't see issue move on to next column
select distinct location
from employee_layoffs;

--Similarly dive into country column, I can see for few rows country united states have dot at the end "United States.", let's update with united states.
select distinct country
from employee_layoffs;

select *
from employee_layoffs
where country like 'United States.%';

--Update United states. with United States 
update employee_layoffs
set country = 'United States'
where country like 'United States%';


--Let's fix date column, It has text data type instead of date
SELECT date
FROM employee_layoffs
WHERE date IS NOT NULL
  AND date !~ '^\d{4}-\d{2}-\d{2}$';

--in our datset date format is M/D/YYYY, we need to explicitly cast it using TO_DATE() with format 'YYYY-MM-DD'. 
UPDATE employee_layoffs
SET date = TO_CHAR(TO_DATE(date, 'MM/DD/YYYY'), 'YYYY-MM-DD');


--convert date column to DATE datatype
alter table employee_layoffs
alter column date type DATE
using TO_DATE(date, 'YYYY-MM-DD');

select * from employee_layoffs;

/********************** Null values **************************************/
--Null values : handling null values in columns
--Null values in total_laid_off and percentage_laid_off looks good ,i don't think i want to change that. 
--I like having them null because it makes it easier for calculations during the EDA phase
select * from employee_layoffs
where total_laid_off is null
and percentage_laid_off is null;

select * from employee_layoffs
where funds_raised_millions is null;


/*************************** Remove unwanted columns and rows *****************************/
--it is good to work on the data which is required for analysis, other column can be deleted 

select * from employee_layoffs
where total_laid_off is null
and percentage_laid_off is null;


delete 
from employee_layoffs
where total_laid_off is null
and percentage_laid_off is null;

--i don't need row_num column in final clean data, remove row_num column
select * from employee_layoffs

alter table employee_layoffs
drop column row_num;








