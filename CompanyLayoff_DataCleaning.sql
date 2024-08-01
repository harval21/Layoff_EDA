-- ---------------------------------------------
-- DATABASE & TABLE SET UP 
-- ---------------------------------------------
-- Creating a New Database
CREATE DATABASE worldlayoffs; -- Import layoffs raw data file here

-- Select Database to Use
USE worldlayoffs;

-- Rename Raw Table
ALTER TABLE layoffs
RENAME TO layoffs_raw;

-- Check Imported Raw Data
SELECT * FROM layoffs_raw;
-- ---------------------------------------------
-- DATA CLEANING
-- Goal 1: Remove Duplicates
-- Goal 2: Standardize the Data
-- Goal 3: Seek Out NULL Value or Blank Values
-- Goal 4: Remove Any Unwanted Columns
-- ---------------------------------------------
-- Goal 1: Removing Duplicates 
-- ---------------------------------------------
-- Copy Values in Raw Table to a Staging Data for Data Manipulation
CREATE TABLE layoffs_staging AS
SELECT *
FROM layoffs_raw;
-- Check Staging Table
SELECT * FROM layoffs_staging;
DESCRIBE layoffs_staging; -- Checking Column Names & Data Types in Staging Table
-- Using ROW_NUMBER() Function to Filter Duplicate Rows in Staging Table
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- Creating a CTE to View Duplicate Rows in Staging Table
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- Create a New Table Named layoffs_duplicate with row_num Column Included
CREATE TABLE layoffs_duplicate AS
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- Check layoffs_duplicate Table
SELECT * FROM layoffs_duplicate;
-- Delete All Duplicates
DELETE FROM layoffs_duplicate
WHERE row_num > 1;
-- Confirm Deletion of Duplicates in the Table
SELECT *
FROM layoffs_duplicate
WHERE row_num > 1;
-- Drop row_num() Column
ALTER TABLE layoffs_duplicate
DROP COLUMN row_num;
-- For the purpose of the project, layoffs_duplicate table wasn't dropped
-- Copy Duplicate-Free Values into a New Table
CREATE TABLE layoffs_staging2 AS
SELECT *
FROM layoffs_duplicate;
-- Check the New Duplicate-Free layoffs_staging2 Table
SELECT *
FROM layoffs_staging2;
-- ---------------------------------------------
-- Goal 2: Standardizing Data
-- ---------------------------------------------
SELECT *
FROM layoffs_staging2;
-- Remove Whitespaces in All Columns
UPDATE layoffs_staging2
SET company = TRIM(company), location = TRIM(location), industry = TRIM(industry),
total_laid_off = TRIM(total_laid_off), percentage_laid_off = TRIM(percentage_laid_off), `date` = TRIM(`date`),
stage = TRIM(stage), country = TRIM(country), funds_raised_millions = TRIM(funds_raised_millions);
-- Check for Unique Values and Standardize Similar Names
SELECT DISTINCT(company) FROM layoffs_staging2 ORDER BY 1;
SELECT DISTINCT(location) FROM layoffs_staging2 ORDER BY 1;
SELECT DISTINCT(industry) FROM layoffs_staging2 ORDER BY 1;
SELECT DISTINCT(country) FROM layoffs_staging2 ORDER BY 1;
-- Standardize Same Industries with Different Names into Singular Unit
-- Example: Crypto, CryptoCurrency, Crypto Currency all into Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry REGEXP '^Crypto';
-- Standardize Same Countries with Different Names into Singular Unit
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country REGEXP 'States.$';
-- Convert Date in Text Form to Date Form, Then Change Text Data Type to Date Data Type
UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- ---------------------------------------------
-- Goal 3: Solving NULLs & Blanks
-- ---------------------------------------------
SELECT * FROM layoffs_staging2;
-- Check for Any Null or Blank Values in Each Field
-- Fields: company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';
-- Convert Blank '' Values Into Null Values in Industry
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
-- Match Null Industry Values in Company with Pre-existing Industry Values in the Same Company
-- Using Self-Join to Link Companies to their respective industries
UPDATE layoffs_staging2 tbl1
JOIN layoffs_staging2 tbl2
	ON tbl1.company = tbl2.company
SET tbl1.industry = tbl2.industry
WHERE tbl1.industry IS NULL
AND tbl2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally\'s%';










