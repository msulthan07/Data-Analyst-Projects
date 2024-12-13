-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- Maximum laid off and Maximum Percent laid off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Total laid off by companies in descending order
SELECT company, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_off DESC;

-- Total laid off by industry in descending order
SELECT industry, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_off DESC;

-- Total laid off by country in descending order
SELECT country, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY country
ORDER BY total_off DESC;

-- Companies who got shut off completely
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- first and last date when laying off started
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Total laid off by year in descending order
SELECT YEAR(`date`), SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Time Series of Layoffs my each month in a year
SELECT SUBSTRING(`date`, 1, 7) AS date, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY 1 ASC;

-- rolling of total laid by each month
WITH rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;

-- Ranking companies with the highest laid offs by each year
SELECT company, YEAR(`date`), SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;
