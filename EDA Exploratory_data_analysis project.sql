-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!


SELECT * 
FROM layoffs.layoffs_staging2;

select * from layoffs_staging2;



SELECT MAX(total_laid_off)
FROM layoffs_staging2;






-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under - ouch




-- TOUGHER QUERIES------------------------------------------------------------------------------------------------------------------------------------

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 
WITH rolling_total AS (
SElECT substring(`date`,1,7) AS `MONTH`,MAX(total_laid_off) AS total_off
from layoffs_staging2
where substring(`date`,1,7) is NOT NULL
group by `MONTH`
order by 2 desc
)
select `MONTH`,total_off,SUM(total_off)
OVER( order by `MONTH`) as running_total
from rolling_total;


-- per year total layoff

WITH rolling_total AS (
SElECT substring(`date`,1,7) AS `MONTH`,MAX(total_laid_off) AS total_off
from layoffs_staging2
where substring(`date`,1,7) is NOT NULL
group by `MONTH`
order by 2 desc
)
select `MONTH`,total_off,SUM(total_off)
OVER(partition by substring(`MONTH`,1,4) order by `MONTH`) as running_total
from rolling_total;


select company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 3 desc;


WITH company_year(company,years,total_laid_off) AS(
select company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
),company_yer_ranking as
(select * ,
DENSE_RANK() OVER(partition by years order by total_laid_off desc) as ranking
from company_year
)
select * from company_yer_ranking 
where ranking <=5;





