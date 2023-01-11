# CONTEXT 

Did you know that over 115 million kilograms of pizza is consumed daily worldwide??? (Well according to Wikipedia anyway…)

Danny was scrolling through his Instagram feed when something really caught his eye - “80s Retro Styling and Pizza Is The Future!”

Danny was sold on the idea, but he knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire - so he had one more genius idea to combine with it - he was going to Uberize it - and so Pizza Runner was launched!

Danny started by recruiting “runners” to deliver fresh pizza from Pizza Runner Headquarters (otherwise known as Danny’s house) and also maxed out his credit card to pay freelance developers to build a mobile app to accept orders from customers.

# PROBLEM STATEMENT 

Because Danny had a few years of experience as a data scientist - he was very aware that data collection was going to be critical for his business’ growth.

He has prepared for us an entity relationship diagram of his database design but requires further assistance to clean his data and apply some basic calculations so he can better direct his runners and optimise Pizza Runner’s operations.

# DATASETS 

Six datasets : 
* runners
* runners_orders
* customer_orders
* pizza_names
* pizza_recipes
* pizza_toppings 

## ERD 

![erd](img/erd.PNG)


## TABLE 1 : RUNNERS 

The `runners` table is composed of : 
- **runner_id** - PRIMARY KEY
- **registration_date** - Registration date of a runner_id

![runners_table](img/runners_table.PNG)

## TABLE 2 : CUSTOMER_ORDERS

The `customer_orders` table is composed of : 
- **order_id** - FOREIGN KEY
- **customer_id** - FOREIGN KEY
- **pizza_id** - FOREIGN KEY
- **exclusions** - ingredient_id values which should be removed from the pizza 
- **extras** - ingredient_id values which should be added to the pizza 
- **order_time** - ordered day and time of the pizza

![customer_orders_table](img/customer_orders_table.PNG)

## TABLE 3 : RUNNER_ORDERS

The `runners_orders` table is composed of : 
- **order_id** - FOREIGN KEY
- **runner_id** - FOREIGN KEY
- **pickup_time** -  the timestamp at which the runner arrives at the Pizza Runner headquarters to pick up the freshly cooked pizza
- **distance** - distance to deliver the pizza
- **duration** - time to deliver the pizza
- **cancellation** - if there is a restaurant cancellation or a customer cancellation

## TABLE 4 : PIZZA_NAMES

The `pizza_names` table is composed of : 
- **pizza_id** - PRIMARY KEY
- **pizza_name** - name of the pizza

## TABLE 5 : PIZZA_RECIPES

The `pizza_recipes` table is composed of : 
- **pizza_id** - FOREIGN KEY
- **toppings** - ingrediens on the pizza

## TABLE 6 : PIZZA_TOPPINGS

The `pizza_toppings` table is composed of : 
- **topping_id** - PRIMARY KEY
- **topping_name** - ingredient name

# CASE STUDY 

Before we are going further, we have to clean two datasets : `customer_orders` and `runner_orders`. 

**Creating Views with data cleaning**

1. In `customer_orders`, we have to set values to NULL when we don't any exclusion or extra ingrediens.
2. In `runner_orders`, we have to delete "km" from distance column and "minutes", "mins" or "minute" from duration column. We also need to set NULL value when there is not cancellation. 

All the 'null' values already in the datasets are in STRING type. We have to convert all in the correct NULL value. 

```sql
DROP SCHEMA IF EXISTS v_pizza_runner CASCADE; 
CREATE SCHEMA v_pizza_runner;

DROP VIEW IF EXISTS v_pizza_runner.runners; 
CREATE VIEW v_pizza_runner.runners AS
SELECT 
  *
FROM pizza_runner.runners; 

DROP VIEW IF EXISTS v_pizza_runner.customer_orders; 
CREATE VIEW v_pizza_runner.customer_orders AS
SELECT 
  order_id, 
  customer_id, 
  pizza_id, 
  CASE 
    WHEN exclusions = '' OR 
    exclusions = 'null' THEN NULL 
    ELSE exclusions
  END AS exclusions, 
  CASE 
    WHEN extras = ''  OR 
    extras = 'NaN' OR 
    extras = 'null' THEN NULL 
    ELSE extras
  END AS extras,
  order_time
FROM pizza_runner.customer_orders;

DROP VIEW IF EXISTS v_pizza_runner.runner_orders; 
CREATE VIEW v_pizza_runner.runner_orders AS
SELECT 
  order_id, 
  runner_id,
  CASE 
    WHEN pickup_time = 'null' THEN NULL
    ELSE pickup_time
  END AS pickup_time, 
  CASE 
    WHEN distance LIKE '%km%' THEN 
      (REGEXP_MATCH(
        distance, 
        '^\d*\.?\d*'
        )
      )[1]
    WHEN distance = 'null' THEN NULL 
    ELSE distance
  END AS distance, 
  CASE 
    WHEN duration ILIKE '%mins%' OR 
      duration ILIKE '%minutes%'OR
      duration ILIKE '%minute%' THEN 
      (REGEXP_MATCH(
        duration, 
        '^\d*\.?\d*'
        )
      )[1]
    WHEN duration = 'null' THEN NULL 
    ELSE duration
  END AS duration, 
  CASE 
    WHEN cancellation = '' OR 
    cancellation = 'NaN' OR 
    cancellation = 'null' THEN NULL
    ELSE cancellation
  END AS cancellation
FROM pizza_runner.runner_orders;


DROP VIEW IF EXISTS v_pizza_runner.pizza_names; 
CREATE VIEW v_pizza_runner.pizza_names AS
SELECT 
  *
FROM pizza_runner.pizza_names;

DROP VIEW IF EXISTS v_pizza_runner.pizza_recipes; 
CREATE VIEW v_pizza_runner.pizza_recipes AS
SELECT 
  *
FROM pizza_runner.pizza_recipes; 

DROP VIEW IF EXISTS v_pizza_runner.pizza_toppings; 
CREATE VIEW v_pizza_runner.pizza_toppings AS
SELECT 
  *
FROM pizza_runner.pizza_toppings; 

DROP VIEW IF EXISTS v_pizza_runner.pizza_join; 
CREATE VIEW v_pizza_runner.pizza_join AS
SELECT 
  customer_orders.*, 
  runner_orders.cancellation, 
  pizza_names.pizza_name
FROM v_pizza_runner.customer_orders
LEFT JOIN v_pizza_runner.runner_orders   
  ON customer_orders.order_id = runner_orders.order_id
LEFT JOIN v_pizza_runner.pizza_names 
  ON customer_orders.pizza_id = pizza_names.pizza_id;
```

## Pizza Metrics 

First, we just consider pizzas. We want to have more informations about teh most ordered pizzas. 

## **Q1**

> How many pizzas were ordered?

```sql
SELECT 
  COUNT(*) AS total_purchases
FROM v_pizza_runner.customer_orders;
```
**COUNT : 14**

## **Q2**

> How many unique customer orders were made?

```sql
SELECT 
  COUNT(DISTINCT order_id) AS total_unique_orders
FROM v_pizza_runner.customer_orders; 
```

**COUNT : 10**

## **Q3**

> How many successful orders were delivered by each runner?

```sql 
SELECT 
  runner_id, 
  COUNT(order_id) AS number_deliveries
FROM v_pizza_runner.runner_orders
WHERE cancellation IS NULL
GROUP BY 1;
```

![result_q_3](img/result_q_3.PNG)

## **Q4**

> How many of each type of pizza was delivered?

```sql
SELECT 
  pizza_name,
  COUNT(*) AS number_pizzas
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1;
```
![result_q_4](img/result_q_4.PNG)

## **Q5**

> How many Vegetarian and Meatlovers were ordered by each customer?

```sql
SELECT
  customer_id,
  SUM(CASE WHEN pizza_id = 1 THEN 1 ELSE 0 END) AS meatlovers,
  SUM(CASE WHEN pizza_id = 2 THEN 1 ELSE 0 END) AS vegetarian
FROM v_pizza_runner.customer_orders
GROUP BY 1
ORDER BY 1;
```
![result_q_5](img/result_q_5.PNG)

## **Q6**

> What was the maximum number of pizzas delivered in a single order?

```sql
SELECT 
  order_id,
  COUNT(pizza_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 1;
```

![result_q_6](img/result_q_6.PNG)

## **Q7**

> For each customer, how many delivered pizzas had at least 1 change and how many had no changes?

```sql
WITH cte_flag_change AS (
  SELECT 
    customer_id, 
    CASE 
      WHEN exclusions IS NOT NULL OR 
        extras IS NOT NULL THEN 1
      ELSE 0
    END AS flag_change
  FROM v_pizza_runner.pizza_join
  WHERE cancellation IS NULL
) 
SELECT 
  customer_id, 
  SUM(CASE WHEN flag_change = 1 THEN flag_change ELSE 0 END) AS at_least_1_change, 
  COUNT(CASE WHEN flag_change = 0 THEN flag_change END) AS no_changes 
FROM cte_flag_change
GROUP BY 1
ORDER BY 1;
```
![result_q_7](img/result_q_7.PNG)

## **Q8**

> How many pizzas were delivered that had both exclusions and extras?

```sql
WITH cte_flag_change AS (
  SELECT 
    customer_id, 
    exclusions, 
    extras, 
    CASE 
      WHEN exclusions IS NOT NULL AND 
        extras IS NOT NULL THEN 1
      ELSE 0
    END AS flag_change
  FROM v_pizza_runner.pizza_join
  WHERE cancellation IS NULL
  ORDER BY order_id, customer_id
) 
SELECT 
  COUNT(*) AS number_pizzas
FROM cte_flag_change
WHERE flag_change = 1;
```

**COUNT : 1**

## **Q9**

> What was the total volume of pizzas ordered for each hour of the day?

```sql
SELECT 
  EXTRACT("HOUR" from order_time) AS hour, 
  COUNT(pizza_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 1;
```

![result_q_9](img/result_q_9.PNG)

## **Q10**

> What was the volume of orders for each day of the week?

```sql
SELECT 
  TO_CHAR(order_time, 'Day') AS day_week, 
  COUNT(order_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 1;
```
![result_q_10](img/result_q_10.PNG)
