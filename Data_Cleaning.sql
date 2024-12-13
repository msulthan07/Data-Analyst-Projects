-- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Inspect the raw data
SELECT *
FROM layoffs;

-- Creating a staging table to work on; this keeps the raw table intact for backup purposes
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Copying data into the staging table
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Data Cleaning Steps:
-- 1. Check for duplicates and remove them
-- 2. Standardize data
-- 3. Handle null or blank values
-- 4. Remove unnecessary columns or rows

-- Step 1: Remove Duplicates

-- Checking for duplicates using ROW_NUMBER() since no single column is unique
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
                                     percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Identifying duplicates by filtering rows with row_num > 1
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
                                         percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Creating a new table to include row_num for easier duplicate removal
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
);

-- Populating the new table with row numbers
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
                                         percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting duplicate rows where row_num > 1
DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Step 2: Standardize Data

-- Trimming white spaces from the 'company' column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Correcting variations in 'industry' values (e.g., fixing "Crypto" entries)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardizing 'country' values (e.g., fixing "United States")
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Converting 'date' column from text to DATE type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Filling missing 'industry' values using data from rows with the same company and location
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
  ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Step 3: Handle Null Values

-- Reviewing null values in key numeric columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL OR percentage_laid_off IS NULL OR funds_raised_millions IS NULL;

-- Deciding not to modify null values in these columns for better accuracy in EDA

-- Removing rows where both 'total_laid_off' and 'percentage_laid_off' are null
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Step 4: Remove Unnecessary Columns

-- Dropping 'row_num' column as it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final Inspection of Cleaned Data
SELECT *
FROM layoffs_staging2;

-- Summary:
-- The data cleaning process removed duplicates, standardized key fields, handled null values, and removed irrelevant rows/columns.
-- The cleaned dataset is now ready for exploratory data analysis (EDA) or further processing.
