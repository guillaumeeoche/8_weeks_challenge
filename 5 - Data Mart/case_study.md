# Context

Data Mart is Danny’s latest venture and after running international operations for his online supermarket that specialises in fresh produce - Danny is asking for your support to analyse his sales performance.

In June 2020 - large scale supply changes were made at Data Mart. All Data Mart products now use sustainable packaging methods in every single step from the farm all the way to the customer.

Danny needs your help to quantify the impact of this change on the sales performance for Data Mart and it’s separate business areas.

The key business question he wants you to help him answer are the following:

- What was the quantifiable impact of the changes introduced in June 2020?
- Which platform, region, segment and customer types were the most impacted by this change?
- What can we do about future introduction of similar sustainability updates to the business to minimise impact on sales?


# DATASETS 

One dataset : 
* weekly_sales

## TABLE 1 : weekly_sales

The `weekly_sales` table is composed of : 
- Data Mart has international operations using a multi-`region` strategy
- Data Mart has both, a retail and online `platform` in the form of a Shopify store front to serve their customers
- Customer `segment` and `customer_type` data relates to personal age and demographics information that is shared with Data Mart
- `transactions` is the count of unique purchases made through Data Mart and sales is the actual dollar amount of purchases

![weekly_sales_table](img/weekly_sales_table.PNG)

# CASE STUDY 

**LOAD INIT SQL FILE BEFORE TO INITIALIZE TABLES**

## Data Cleansing Steps

```sql 
DROP TABLE IF EXISTS data_mart.clean_weekly_sales;
CREATE TABLE data_mart.clean_weekly_sales 
(
  week_date DATE, 
  week_number INTEGER,
  month_number INTEGER,
  calendar_year INTEGER,
  region STRING, 
  plateform STRING, 
  segment STRING, 
  age_band STRING,
  demographic STRING,
  customer_type STRING, 
  transactions INTEGER, 
  sales INTEGER, 
  avg_transaction FLOAT64
)
AS (
  SELECT 
    week_date, 
    EXTRACT(WEEK(MONDAY) FROM week_date) AS week_number,
    EXTRACT(MONTH FROM week_date) AS month_number,
    EXTRACT(YEAR FROM week_date) AS calendar_year,
    region, 
    platform, 
    segment,
    CASE 
      WHEN segment LIKE '%1%' THEN "Young Adults"
      WHEN segment LIKE '%2%' THEN "Middle Aged"
      WHEN segment LIKE '%3%'OR segment LIKE '%4%' THEN "Retirees"
      ELSE "unknown"
    END AS age_band,
    CASE 
      WHEN segment LIKE '%C%' THEN "Couples"
      WHEN segment LIKE '%F%' THEN "Families"
      ELSE "unknown"
    END AS demographic,
    customer_type, 
    transactions, 
    sales, 
    ROUND(sales/transactions, 2)
  FROM `ferrous-syntax-352217.data_mart.weekly_sales`
);
```
# Data Exploration

## **Q1**

> What day of the week is used for each `week_date` value?

```sql
SELECT 
  FORMAT_DATE('%A', week_date) AS week_day,
  COUNT(*)
FROM data_mart.clean_weekly_sales
GROUP BY 1; 
```
**Monday**

## **Q2**

> What range of week numbers are missing from the dataset?

```sql
SELECT 
  week_number 
FROM data_mart.clean_weekly_sales
GROUP BY 1
ORDER BY 1; 
```
[1-11] & [37-52]

We can have a list of these values 

```sql
WITH all_week_numbers AS (
 SELECT 
  *
 FROM UNNEST(GENERATE_ARRAY(1, 52)) AS week_number 
)
SELECT
  week_number 
FROM all_week_numbers AS t1
WHERE NOT EXISTS (
  SELECT 1
  FROM data_mart.clean_weekly_sales AS t2
  WHERE t1.week_number = t2.week_number
)
```
## **Q3**

 > How many total transactions were there for each year in the dataset?

 ```sql
SELECT 
  calendar_year, 
  SUM(transactions) AS total_transactions 
FROM data_mart.clean_weekly_sales 
GROUP BY calendar_year
ORDER BY 2 DESC; 
 ```
![transactions_by_year](img/transactions_by_year.PNG)

## **Q4**

> What is the total sales for each region for each month?


