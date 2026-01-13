CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS conformed;

-- === SILVER (3 Tables) ===

CREATE OR REPLACE TABLE silver.dim_companies AS
SELECT company_id, UPPER(name) as name, location, industry FROM raw.companies;

CREATE OR REPLACE TABLE silver.dim_employees AS
SELECT emp_id, full_name, gender FROM raw.employees;

CREATE OR REPLACE TABLE silver.fact_jobs AS
SELECT
    j.*, c.name as company_name
FROM raw.job_history j
JOIN raw.companies c ON j.company_id = c.company_id;

-- === CONFORMED (4 Tables) ===

-- 1. KPI לחברות
CREATE OR REPLACE TABLE conformed.company_stats AS
SELECT company_name, AVG(salary) as avg_salary, COUNT(*) as workers
FROM silver.fact_jobs GROUP BY company_name;

-- 2. סיכום קריירה לעובד
CREATE OR REPLACE TABLE conformed.career_summary AS
SELECT emp_id, COUNT(job_id) as total_jobs, MAX(salary) as peak_salary
FROM silver.fact_jobs GROUP BY emp_id;

-- 3. ניתוח תעשיות
CREATE OR REPLACE TABLE conformed.industry_trends AS
SELECT c.industry, AVG(j.salary) as avg_pay
FROM silver.fact_jobs j JOIN silver.dim_companies c ON j.company_id = c.company_id
GROUP BY c.industry;

-- 4. טבלת הסיכון (הלוגיקה המעניינת ל-Lineage)
CREATE OR REPLACE TABLE conformed.churn_risk AS
SELECT
    e.full_name,
    cs.total_jobs,
    -- לוגיקה עסקית מורכבת:
    CASE
        WHEN cs.total_jobs >= 3 THEN 'HIGH (Job Hopper)'
        WHEN cs.peak_salary < 50000 THEN 'HIGH (Underpaid)'
        ELSE 'LOW'
    END AS risk_level
FROM conformed.career_summary cs
JOIN silver.dim_employees e ON cs.emp_id = e.emp_id;