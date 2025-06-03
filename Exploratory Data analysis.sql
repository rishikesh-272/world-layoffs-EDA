-- Exploratory Data Analysis
-- perform various queries to find trends and insights
-- basically explore the data

select * 
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2; 

-- top companies to do layoffs by total_laid_off 

select company, sum(total_laid_off) as total_layoffs
from layoffs_staging2
group by company
order by 2 DESC;

-- top companies to do layoffs by percentage_laid_off

select company, sum(percentage_laid_off) AS sum_percentage_laid_off
from layoffs_staging2
group by company
order by 2 DESC;

-- shows the data of layoffs from covid till now

select min(`date`), max(`date`)
from layoffs_staging2;

-- top industries to do total layoffs 

select industry, sum(total_laid_off) as total_layoffs
from layoffs_staging2
group by industry
order by 2 DESC;

-- countries with total layoffs

select country, sum(total_laid_off) as total_layoffs
from layoffs_staging2
group by country
order by 2 DESC;

-- location with total layoffs 

select location, sum(total_laid_off) as total_layoffs
from layoffs_staging2
group by location
order by 2 DESC;

-- layoffs done by year

select year(`date`), sum(total_laid_off) as total_layoffs
from layoffs_staging2
group by year(`date`)
order by 1 DESC;

-- layoffs done by month

select month(`date`), sum(total_laid_off)as total_layoffs
from layoffs_staging2
group by month(`date`)
order by 1 asc;

-- peak layoffs month 

select month(`date`) as `Month`, sum(total_laid_off)as total_layoffs
from layoffs_staging2
group by month(`date`)
order by 2 DESC
limit 1;

-- layoffs done in order of stage of companies

select stage, sum(total_laid_off)as total_layoffs
from layoffs_staging2
group by stage
order by 2 DESC;

-- layoffs by stage of company 

select stage, COUNT(*) AS layoffs
from layoffs_staging2
group by stage
ORDER BY 2 DESC;

-- Companies that laid off people more than once

select company, COUNT(*) AS num_layoff_events, SUM(total_laid_off) AS total_layoffs
from layoffs_staging2
group by company
having num_layoff_events > 1
order by 3 DESC;

-- average layoffs per company

select company, ROUND(AVG(total_laid_off), 2) AS avg_layoff
from layoffs
group by company
order by avg_layoff DESC;

-- first and last date per company

select company, MIN(`date`) AS first_layoff, MAX(`date`) AS last_layoff
from layoffs_staging2
group by company;

-- rolling total of total layoffs with respect to each month

select substring(`date`,1,7) as `Month`, sum(total_laid_off) AS total_layoffs
from layoffs_staging2
group by `Month`
order by 1 ASC;

-- now use it in a CTE so we can query off of it

WITH Rolling_Total AS 
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) AS total_layoffs
from layoffs_staging2
group by `Month`
order by 1 ASC
)
select `Month`, total_layoffs , sum(total_layoffs) over (order by `Month` ASC) as rolling_total_layoffs
from Rolling_Total
order by 1 ASC;

-- rolling total of total layoffs with respect to each year and company

select company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_layfoffs
  from layoffs_staging2
  group by company, YEAR(`date`)
  order by 3 desc;
  
with Company_year (company, years, total_laid_off) as 
(
select company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_layfoffs
  from layoffs_staging2
  group by company, YEAR(`date`)
), 
Company_year_rank as
(select *, 
dense_Rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_year
where years is not null
)
select *
from Company_year_rank
where ranking <= 5;

-- in above query used 2 CTEs which was a bit difficult and complex query

-- summary 
SELECT 
    COUNT(DISTINCT company) AS total_companies,
    COUNT(*) AS total_records,
    SUM(total_laid_off) AS total_people_laid_off,
    ROUND(AVG(percentage_laid_off), 2) AS avg_percentage_laid_off,
    ROUND(AVG(funds_raised), 2) AS avg_funding
FROM layoffs;





























