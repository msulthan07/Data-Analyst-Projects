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
----------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Step 2: Exploratory Data Analysis (EDA)

-- The goal of this step is to explore the data, uncover trends, patterns, and interesting insights such as outliers.
-- Typically, during EDA, you may have specific questions or hypotheses in mind, but it is also useful to explore the data broadly.

-- Inspecting the cleaned data
SELECT *
FROM layoffs_staging2;

-- Finding maximum values for layoffs and percentage laid off
SELECT MAX(total_laid_off) AS max_laid_off, MAX(percentage_laid_off) AS max_percent_laid_off
FROM layoffs_staging2;

-- Total layoffs by company in descending order
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC;

-- Total layoffs by industry in descending order
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- Total layoffs by country in descending order
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC;

-- Identifying companies that shut down completely
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Earliest and latest dates of layoffs
SELECT MIN(`date`) AS first_layoff_date, MAX(`date`) AS last_layoff_date
FROM layoffs_staging2;

-- Total layoffs by year in descending order
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY year DESC;

-- Monthly time series of layoffs
SELECT DATE_FORMAT(`date`, '%Y-%m') AS month, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE `date` IS NOT NULL
GROUP BY month
ORDER BY month ASC;

-- Rolling total of layoffs by month
WITH monthly_totals AS (
    SELECT DATE_FORMAT(`date`, '%Y-%m') AS month, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    WHERE `date` IS NOT NULL
    GROUP BY month
    ORDER BY month ASC
)
SELECT month, total_laid_off, 
       SUM(total_laid_off) OVER(ORDER BY month) AS rolling_total
FROM monthly_totals;

-- Ranking companies with the highest layoffs by year
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company, year
ORDER BY total_laid_off DESC;

WITH company_year_summary AS (
    SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, year
), company_year_ranked AS (
    SELECT company, year, total_laid_off,
           DENSE_RANK() OVER(PARTITION BY year ORDER BY total_laid_off DESC) AS rank
    FROM company_year_summary
)
SELECT *
FROM company_year_ranked
WHERE rank <= 5;
