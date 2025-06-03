-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
from world_layoffs.layoffs;

create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select * from layoffs;

select * 
from layoffs_staging;

alter table layoffs_staging
drop column `source`;

-- lets check for duplicates

select *,
row_number() over (partition by company, location, industry, total_laid_off,percentage_laid_off,
`date`, stage, country, funds_raised, date_added) AS row_num
from world_layoffs.layoffs_staging;

with duplicate_cte as
(
select *,
		row_number() over (
			partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised, date_added
			) AS row_num
	from 
		world_layoffs.layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

-- we cannot delete in this way

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
		row_number() over (
			partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised, date_added
			) AS row_num
	from 
		world_layoffs.layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

-- standardizing data

select company, (trim(company))
from layoffs_staging2
order by 1;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select `date`
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select funds_raised,
replace(funds_raised, '$', '') AS cleaned_amount
from layoffs;

select *
from layoffs_staging2;

-- remove null or empty values 

select *
from layoffs_staging2
where total_laid_off = ''
and percentage_laid_off = '';

update layoffs_staging2
set industry = NULL
where industry = '';

select *
from layoffs_staging2
where industry is NULL
or industry = '';

select * 
from layoffs_staging2
where company = 'Appsmith';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is NUll or t1.industry = '')
and t2.industry is not NULL;

update layoffs_staging2
set industry = NULL
where company = 'Appsmith';

select *
from layoffs_staging2
where total_laid_off = ''
and percentage_laid_off = '';

delete
from layoffs_staging2
where total_laid_off = ''
and percentage_laid_off = '';

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;
