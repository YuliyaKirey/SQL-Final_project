CREATE DATABASE final_project;
UPDATE customer SET Gender = NULL WHERE Gender ='';
UPDATE customer SET Age = NULL WHERE Age ='';
ALTER TABLE customer MODIFY Age INT NULL;

SELECT * FROM customer;

CREATE TABLE transactions 
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment DECIMAL(10,2));


LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TRANSACTIONS_final.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'secure_file_priv';


# 1.Cписок клиентов с непрерывной историей за год, 
-- средний чек за период с 01.06.2015 по 01.06.2016, средняя сумма покупок за месяц, количество всех операций по клиенту за период
SELECT
    ID_client,
    COUNT(DISTINCT Id_check) AS total_operations,
    AVG(check_sum) AS avg_check,
    SUM(check_sum) / 12 AS avg_month_sum
FROM (
    SELECT
        ID_client,
        Id_check,
        DATE_FORMAT(date_new, '%Y-%m') AS month,
        SUM(Sum_payment) AS check_sum
    FROM transactions
    WHERE date_new >= '2015-06-01'
      AND date_new <  '2016-06-01'
    GROUP BY ID_client, Id_check, month
) sub
GROUP BY ID_client
HAVING COUNT(DISTINCT month) = 12
ORDER BY ID_client;

# 2. Информация в разрезе месяцев
-- a.средняя сумма чека в месяц
SELECT
  month,
  AVG(check_sum) AS avg_check
FROM (
  SELECT
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    Id_check,
    SUM(Sum_payment) AS check_sum
  FROM transactions
  WHERE date_new >= '2015-06-01'
    AND date_new <  '2016-06-01'
  GROUP BY month, Id_check
) checks
GROUP BY month
ORDER BY month;

-- b.среднее количество операций в месяц
SELECT
  month,
  COUNT(*) AS operations_cnt
FROM (
  SELECT
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    Id_check
  FROM transactions
  WHERE date_new >= '2015-06-01'
    AND date_new <  '2016-06-01'
  GROUP BY month, Id_check
) checks
GROUP BY month
ORDER BY month;

-- c.среднее количество клиентов, которые совершали операции
SELECT
  month,
  COUNT(DISTINCT ID_client) AS clients_cnt
FROM (
  SELECT
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    ID_client
  FROM transactions
  WHERE date_new >= '2015-06-01'
    AND date_new <  '2016-06-01'
) checks
GROUP BY month
ORDER BY month;

-- d.доля от общего количества операций за год и доля в месяц от общей суммы операций
WITH checks AS (
  SELECT
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    Id_check,
    SUM(Sum_payment) AS check_sum
  FROM transactions
  WHERE date_new >= '2015-06-01'
    AND date_new <  '2016-06-01'
  GROUP BY month, Id_check
),
month_stats AS (
  SELECT
    month,
    COUNT(*) AS ops_month,
    SUM(check_sum) AS sum_month
  FROM checks
  GROUP BY month
),
year_stats AS (
  SELECT
    SUM(ops_month) AS ops_year,
    SUM(sum_month) AS sum_year
  FROM month_stats
)
SELECT
  m.month,
  m.ops_month / y.ops_year AS share_ops_of_year,
  m.sum_month / y.sum_year AS share_sum_of_year
FROM month_stats m
CROSS JOIN year_stats y
ORDER BY m.month;

-- e.% соотношение M/F/NA в каждом месяце с их долей затрат
SELECT
  month,
  gender,
  clients_cnt * 100.0 / SUM(clients_cnt) 
  OVER (PARTITION BY month) AS gender_percent,
  spend_sum * 100.0 / SUM(spend_sum) 
  OVER (PARTITION BY month) AS spend_percent
FROM (
  SELECT
    month,
    gender,
    COUNT(DISTINCT ID_client) AS clients_cnt,
    SUM(check_sum) AS spend_sum
  FROM (
    SELECT
      DATE_FORMAT(t.date_new, '%Y-%m') AS month,
      t.ID_client,
      t.Id_check,
      CASE
        WHEN c.Gender IN ('M','F') THEN c.Gender
        ELSE 'NA'
      END AS gender,
      SUM(t.Sum_payment) AS check_sum
    FROM transactions t
    LEFT JOIN customer c
      ON t.ID_client = c.Id_client
    WHERE t.date_new >= '2015-06-01'
      AND t.date_new <  '2016-06-01'
    GROUP BY month, t.ID_client, t.Id_check, gender
  ) checks
  GROUP BY month, gender
) m
ORDER BY month, gender;

# 3.Возрастные группы клиентов с шагом 10 лет и отдельно клиенты, у которых нет данной информации, 
-- с параметрами сумма и количество операций за весь период, и поквартально - средние показатели и %.
SELECT
  CASE
    WHEN c.Age IS NULL OR c.Age <= 0 THEN 'Unknown'
    ELSE CONCAT(FLOOR(c.Age/10)*10, '-', FLOOR(c.Age/10)*10 + 9)
  END AS age_group,
  COUNT(DISTINCT t.Id_check) AS operations_cnt,
  SUM(t.Sum_payment) AS total_sum
FROM transactions t
JOIN customer c
  ON t.ID_client = c.Id_client
WHERE t.date_new >= '2015-06-01'
  AND t.date_new <  '2016-06-01'
GROUP BY age_group
ORDER BY age_group;


SELECT
    CASE 
        WHEN c.Age IS NULL OR c.Age <= 0 THEN 'NA'
        ELSE CONCAT(FLOOR(c.Age/10)*10, '-', FLOOR(c.Age/10)*10 + 9)
    END AS age_group,
    CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS quarter,
    AVG(t.Sum_payment) AS avg_payment,
    COUNT(DISTINCT t.Id_check) AS operations_count,
    SUM(t.Sum_payment)
      / SUM(SUM(t.Sum_payment)) OVER (
          PARTITION BY CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new))
        ) AS payment_share
FROM customer c
JOIN transactions t 
  ON c.Id_client = t.ID_client
WHERE t.date_new >= '2015-06-01'
  AND t.date_new <  '2016-06-01'
GROUP BY
    age_group,
    CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new))
ORDER BY age_group, quarter;