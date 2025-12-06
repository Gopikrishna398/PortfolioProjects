-- Data cleaning

select * from layoffs;


-- 1. remove duplicates 
-- 2. stadardize the data 
-- 3. Null values or blank values
-- 4.Remove any columns 

create table layoffs_staging 
LIKE layoffs;  -- creating a duplicate table . we donot do data cleaning on raw data.

select * from layoffs_staging;

INSERT layoffs_staging 
select * from layoffs;

-- Removing duplicate rows if all details are same (this is done by window function , CTE (Common Table Expression) )

-- only generated row_num temporarily inside the CTE:
-- But this row_num is not stored in the table—it is virtual, created only while the CTE runs.


WITH duplicate_cte AS(
 select *,
 ROW_NUMBER() OVER(
 PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS row_num from layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num>1;

-- creating another table by copying the schema 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- layoffs_staging2 = created by you with row_num column permanently added

select * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS row_num from layoffs_staging;

select * from layoffs_staging2
where row_num>1;

-- delete duplicant rows 

delete 
from layoffs_staging2
where row_num>1;

select * from layoffs_staging2;

-- standardizinng data

select company,trim(company)
from layoffs_staging2;

UPDATE layoffs_staging2
set company=trim(company);

select distinct industry from layoffs_staging2 order by 1;


select * from layoffs_staging2
where industry LIKE 'Crypto%';

UPDATE layoffs_staging2
set industry='Crypto'
where industry LIKE 'Crypto%';

select location from layoffs_staging2
order by 1;

select distinct country from layoffs_staging2
order by 1;

update layoffs_staging2
set country ='United States'
where country LIKE 'United States%';

select distinct country, TRIM(TRAILING '.' from country)
from layoffs_staging2 
order by 1;

update layoffs_staging2
set country =TRIM(TRAILING '.' from country)
where country LIKE 'United States%';

select * from layoffs_staging2;

select `date` from 
layoffs_staging2;

select `date` , str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;




select * from layoffs_staging2
where industry IS NULL OR industry='';

-- filling the null or blank fields with its matching data in table 

select * from layoffs_staging2
where company='Airbnb';

select t1.industry,t2.industry from layoffs_staging2 t1 JOIN
	layoffs_staging2 t2 
	ON t1.company=t2.company
where (t1.industry IS NULL OR t1.industry='') 
AND t1.industry IS NOT NULL;

-- first we need the chnage it into NULL if it is in ''  then we can change using JoINS

UPDATE layoffs_staging2
set industry=NULL
where industry='';

-- query for copying the data from one field to another which is row having some common data like country,company name so by this we can fill in that NULL fields 

UPDATE layoffs_staging2 t1 
JOIN layoffs_staging2 t2 
	ON t1.company=t2.company
    SET t1.industry=t2.industry
where (t1.industry IS NULL OR t1.industry='') 
AND t2.industry IS NOT NULL;

select * from layoffs_staging2;


select * from layoffs_staging2
where industry IS NULL OR industry='';



select * from layoffs_staging2
where total_laid_off IS NULL OR total_laid_off=' ';

select * from layoffs_staging2
where percentage_laid_off IS NULL OR percentage_laid_off=' ';

-- if it is NULL then we dont need those tuples/records it won't we helpfull to us so, remove those tuples where total_laid_off && percecntage_laid_off is NULL 
-- (No LaYOFF is happen in those industry )

DELETE from layoffs_staging2 
where percentage_laid_off IS NULL AND total_laid_off IS NULL;

select * from layoffs_staging2;

-- we dont need the row_num now so remove the column

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;

-- Data cleaning is completed for this DATASET
