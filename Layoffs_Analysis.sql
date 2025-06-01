USE world_layoffs;

DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs (
  company VARCHAR(100),
  location VARCHAR(100),
  industry VARCHAR(100),
  total_laid_off INT NULL,
  percentage_laid_off FLOAT NULL,
  date DATE NULL,
  stage VARCHAR(100),
  country VARCHAR(100),
  funds_raised_millions FLOAT NULL
);

SELECT DISTINCT date FROM layoffs LIMIT 10;

SELECT * FROM layoffs;

-- DATA CLEANING

-- Creating a new table with the same structure as `layoffs`
-- To safely perform data cleaning and transformations without affecting the original `layoffs` table.
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

-- Copying all data from the original `layoffs` table into the `layoffs_staging` table
INSERT layoffs_staging
SELECT * FROM layoffs;

-- STEP 1. Removing Duplicates

-- Creating a CTE (Common Table Expression) to find duplicate rows
-- Using ROW_NUMBER() to assign a unique row number to duplicate rows (based on all columns)
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1; -- Selecting only the duplicate rows

SELECT * FROM layoffs_staging WHERE company = 'Casper'; -- Checking if the output is correct

-- Creating a new table `layoffs_staging2` with the same columns as the previous staging table,
-- plus an extra column `row_num` to track and remove duplicates.
-- Reason: We used a CTE earlier to detect duplicates,
-- but CTEs are read-only and do not allow updates or deletions directly.
-- Therefore, we create a new staging table with row numbers included, so we can safely delete duplicates.
CREATE TABLE `layoffs_staging2` (
  `company` varchar(100) DEFAULT NULL,
  `location` varchar(100) DEFAULT NULL,
  `industry` varchar(100) DEFAULT NULL,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` float DEFAULT NULL,
  `date` date DEFAULT NULL,
  `stage` varchar(100) DEFAULT NULL,
  `country` varchar(100) DEFAULT NULL,
  `funds_raised_millions` float DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

-- Insert data into `layoffs_staging2` with `row_num` calculated using ROW_NUMBER()
-- Duplicate rows will have a row_num > 1
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Verify duplicate rows that will be deleted
SELECT * FROM layoffs_staging2 WHERE row_num > 1;

-- Deleting all duplicate rows (row_num > 1), keeping only the first instance of each duplicate group
DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- STEP 2. Standardize the Data

-- Previewing company names with leading/trailing spaces to identify inconsistencies
SELECT company, TRIM(company) FROM layoffs_staging2;

-- Removing any extra spaces from the beginning and end of company names
UPDATE layoffs_staging2 SET company = TRIM(company);

-- Reviewing distinct values in the 'industry' column to detect variations in naming
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY industry;

SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

-- Reviewing distinct values in the 'country' column to find inconsistencies
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2 ORDER BY 1;

UPDATE layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- STEP 3. Null Values or blank values

-- Checking for records where the 'industry' column is either NULL or an empty string
-- This helps identify incomplete or improperly formatted data
SELECT * FROM layoffs_staging2 
WHERE industry IS NULL OR industry = '';

-- Converting all empty strings in the 'industry' column to NULL
-- This standardizes missing values and simplifies future filtering
UPDATE layoffs_staging2 
SET industry = NULL 
WHERE industry = '';

-- Re-checking how many records now have NULL in the 'industry' column
SELECT * FROM layoffs_staging2 
WHERE industry IS NULL;

-- Checking a sample example: check all records for 'Airbnb'
SELECT * FROM layoffs_staging2 WHERE company = 'Airbnb';

-- Finding NULL industry values that can be filled in
-- by joining with the same table on `company` where industry is known
-- Logic: If other records for the same company have a known industry, use it
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company = t2.company 
 WHERE t1.industry IS NULL
 AND t2.industry IS NOT NULL;
   
-- Fill in NULL industries using known values from the same company (self-join update)
-- Only updates records with NULL industry if a matching record with non-NULL industry exists
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company = t2.company
 SET t1.industry = t2.industry
WHERE t1.industry IS NULL
 AND t2.industry IS NOT NULL;

-- Final check to see if any NULL values remain in the 'industry' column
SELECT * FROM layoffs_staging2 
WHERE industry IS NULL; -- Only 1 record with company Bally

-- STEP 4. Remove any Columns or Rows 

-- Identify records where both total_laid_off and percentage_laid_off are NULL
SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND 
percentage_laid_off IS NULL; 
-- We can't impute or estimate total_laid_off based on percentage_laid_off,
-- because we don't know the total number of employees at the company.


-- DELETE only those rows where both fields are NULL
-- This ensures we are not deleting rows that have at least some useful layoff data
DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND 
percentage_laid_off IS NULL; 

SELECT * FROM layoffs_staging2;

-- Removing the column `row_num` which was only needed for deduplication earlier
-- Now that deduplication is complete, the column is no longer necessary
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- EXPLORATORY DATA ANALYSIS

-- 1. Maximum Layoffs and Maximum Layoff Percentage

-- Get the maximum number of employees laid off in a single event,
-- and the maximum layoff percentage (i.e., proportion of workforce laid off).
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging2;

/* Interpretation: One company laid off 12,000 employees — this is likely the largest layoff event in the dataset.
A percentage_laid_off = 1 indicates several companies had complete shutdowns (100% workforce laid off), 
which is a strong signal of bankruptcy or acquisition-related shutdown. */

-- 2. Which Companies Laid Off 100% of Staff?
SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

/* Interpretation: Many companies that shut down had raised significant funding (e.g., Britishvolt with $2.4B).
This highlights that even heavily funded startups are vulnerable to failure. */

-- 3. Aggregated Layoff Analysis

-- a. Total layoffs by company
-- This query shows which individual companies laid off the most employees in total.
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

/* Interpretation: Amazon leads with over 18,000 layoffs, followed by Google, Meta, and Salesforce.
Most top companies are major tech firms, reflecting widespread cuts in the tech industry. */

-- Checking the earliest and latest layoff dates in the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- b. Total layoffs by industry
-- Aggregates layoffs across each industry to identify sectors hit hardest overall.
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

/* Interpretation: Consumer and Retail sectors top the list with over 40,000 layoffs each.
Transportation, Finance, Healthcare, and Food also saw large-scale cuts, showing that layoffs spanned multiple industries. */

-- c. Total layoffs by country
-- Shows total layoffs per country to understand the geographical spread of job losses.
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

/* Interpretation:
-- The United States dominates with over 250,000 layoffs — far higher than other countries.
-- India, Netherlands, and Sweden follow distantly, indicating the layoffs were concentrated in U.S.-based companies. */

-- d. Total layoffs by year
-- Breaks down total layoffs by year to identify trends over time.
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY SUM(total_laid_off) DESC;

/* Interpretation: 2022 recorded the highest layoffs (160K), followed by 2023 with over 125K,
even though the data only goes up to March 2023. This suggests layoffs were accelerating sharply in early 2023.
2020 and 2021 saw comparatively lower totals, reflecting earlier pandemic-phase disruptions. */

-- e. Total layoffs by funding stage
-- Examines how layoffs vary based on a company’s funding stage (e.g., Series A, Post-IPO).
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY SUM(total_laid_off) DESC;

/* Interpretation:  Post-IPO companies accounted for the most layoffs (204K+), suggesting that public 
or mature companies had the largest cuts. "Post-IPO" means the company has gone public and is trading 
on the stock market. Layoffs were also common among Acquired companies and those in Series C to E funding rounds. 
Series A–E are stages of private investment that typically reflect a company's growth, with 
Series C+ often indicating late-stage startups nearing IPO. This shows that even well-funded, 
later-stage startups faced significant downsizing before reaching the public stage. */

-- 4. Monthly layoff Trends

-- Shows total number of employees laid off each month.
-- Helps track the rise and fall of layoffs over time.
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

/* Interpretation: Layoffs peaked sharply in April–May 2020 due to the onset of COVID-19. After a relatively 
quiet 2021, a strong new wave started mid-2022 and intensified into early 2023, highlighting growing market instability.*/

-- 5. Rolling Total of Layoffs (Cumulative Impact)

-- Calculates the cumulative sum of layoffs over months to show the overall damage over time.
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

/* Interpretation: The total number of layoffs crossed 100,000 by early 2022, reflecting deepening 
workforce reductions. The accelerating slope into 2023 shows continued instability, with no clear recovery trend yet. */

-- 6. Layoffs by Company and Year

-- Breaks down total layoffs per company, year by year.
-- Useful for identifying which companies had layoffs in which year.
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

/* Interpretation: Google (2023), Meta (2022), and Amazon (multiple years) had the largest layoff events. 
These numbers show that even tech giants are downsizing aggressively in consecutive years, likely due to over-hiring during pandemic. */

-- 7. Top 5 Companies with Most Layoffs Each Year

-- Ranks companies per year by their total layoffs using DENSE_RANK.
-- Filters only the top 5 for each year to show key players annually.
WITH Company_Year(Company, Years, Total_Laid_Off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(SELECT * ,
DENSE_RANK() OVER(PARTITION BY Years ORDER BY Total_Laid_Off DESC) AS Ranking
FROM Company_Year
WHERE Years IS NOT NULL
)
SELECT * FROM Company_Year_Rank
WHERE Ranking <= 5;

/* Interpretation: Each year had different top contributors: Uber (2020), Bytedance (2021), Meta (2022), and Google (2023).
This highlights how economic shifts, policy changes, and sector-specific downturns influence layoffs differently each year. */

