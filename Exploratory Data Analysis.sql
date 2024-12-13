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
