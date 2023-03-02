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

![erd](img/erd_pizza_runner.png)


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

![runner_orders_table](img/runner_orders_table.PNG)

## TABLE 4 : PIZZA_NAMES

The `pizza_names` table is composed of : 
- **pizza_id** - PRIMARY KEY
- **pizza_name** - name of the pizza

![pizza_names_table](img/pizza_names_table.PNG)

## TABLE 5 : PIZZA_RECIPES

The `pizza_recipes` table is composed of : 
- **pizza_id** - FOREIGN KEY
- **toppings** - ingrediens on the pizza

![pizza_recipes_table](img/pizza_recipes_table.PNG)

## TABLE 6 : PIZZA_TOPPINGS

The `pizza_toppings` table is composed of : 
- **topping_id** - PRIMARY KEY
- **topping_name** - ingredient name

![pizza_toppings_table](img/pizza_toppings_table.PNG)

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
  order_time, 
  ROW_NUMBER() OVER(PARTITION BY order_id) AS _row_number
FROM pizza_runner.customer_orders;

DROP VIEW IF EXISTS v_pizza_runner.runner_orders; 
CREATE VIEW v_pizza_runner.runner_orders AS
WITH cte_wrong_types AS (
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
FROM pizza_runner.runner_orders
)
SELECT 
  order_id, 
  runner_id, 
  pickup_time::TIMESTAMP WITHOUT TIME ZONE, 
  distance::NUMERIC, 
  duration::NUMERIC, 
  cancellation::TEXT
FROM cte_wrong_types;



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
  pizza_names.pizza_name, 
  runner_orders.runner_id, 
  runner_orders.pickup_time,
  runner_orders.duration, 
  runner_orders.distance,
  DATE_PART('minute', AGE(pickup_time, order_time))::INTEGER AS pickup_minutes
FROM v_pizza_runner.customer_orders
LEFT JOIN v_pizza_runner.runner_orders   
  ON customer_orders.order_id = runner_orders.order_id
LEFT JOIN v_pizza_runner.pizza_names 
  ON customer_orders.pizza_id = pizza_names.pizza_id;
  
DROP TABLE IF EXISTS pizza_runner.customer_ratings;
CREATE TABLE pizza_runner.customer_ratings (
   order_id INT,
   rating INT
);

INSERT INTO pizza_runner.customer_ratings(order_id, rating)
SELECT 
  order_id, 
  FLOOR(1 + 5 * RANDOM()) AS rating
FROM pizza_runner.runner_orders
WHERE pickup_time IS NOT NULL; 
  
DROP VIEW IF EXISTS v_pizza_runner.customer_ratings; 
CREATE VIEW v_pizza_runner.customer_ratings AS 
SELECT
  *
FROM pizza_runner.customer_ratings; 
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

## Runner and Customer Experience

## **Q1**

> How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)

```sql
SELECT 
  DATE_TRUNC('week', registration_date)::DATE + 4 AS registration_week, 
  COUNT(*) AS runners_count
FROM v_pizza_runner.runners
GROUP BY 1
ORDER BY 1;
```
![result_q_2_1](img/result_q_2_1.PNG)

## **Q2**

> What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?

*We have to modify the type of duration*

```sql
WITH cte_diff_time AS (
  SELECT DISTINCT
    order_id,
    pickup_time, 
    order_time, 
    DATE_PART('minute', AGE(pickup_time, order_time))::INTEGER AS pickup_minutes
  FROM v_pizza_runner.pizza_join
  WHERE cancellation IS NULL
) 
SELECT 
  ROUND(AVG(pickup_minutes), 3) AS avg_pickup_minutes
FROM cte_diff_time;
```
![result_q_2_2](img/result_q_2_2.PNG)

## **Q3**

> Is there any relationship between the number of pizzas and how long the order takes to prepare?

```sql
SELECT DISTINCT
  order_id,
  pickup_minutes, 
  COUNT(pizza_id) AS pizza_count
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1, 2
ORDER BY 3;
```

![result_q_2_3](img/result_q_2_3.PNG)

## **Q4**

> What was the average distance travelled for each customer?

```sql
SELECT 
  customer_id,
  ROUND(AVG(DISTINCT distance), 1) AS avg_distance
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1
ORDER BY 1;
```

![result_q_2_4](img/result_q_2_4.PNG)

## **Q5**

> What was the difference between the longest and shortest delivery times for all orders?

```sql
SELECT 
  MAX(duration) - MIN(duration) AS max_diff_delivery_time
FROM v_pizza_runner.runner_orders
WHERE cancellation IS NULL;
```

**DIFF : 30**

## **Q6**

> What was the average speed for each runner for each delivery and do you notice any trend for these values?

```sql
WITH cte_trend_speed_time AS (
  SELECT 
    runner_id, 
    order_id,
    AVG(distance/duration) AS avg_km_per_minutes
  FROM v_pizza_runner.pizza_join 
  WHERE cancellation IS NULL
  GROUP BY 1, 2
  ORDER BY 1, 2
) 
SELECT  
  runner_id, 
  order_id,
  ROUND(avg_km_per_minutes * 60, 1) AS avg_km_per_hours, 
  ROUND(
    100 * (avg_km_per_minutes*60 - LAG(avg_km_per_minutes*60) OVER(PARTITION BY runner_id ORDER BY avg_km_per_minutes*60))/
    LAG(avg_km_per_minutes*60) OVER(PARTITION BY runner_id ORDER BY avg_km_per_minutes*60)::NUMERIC, 
    1
  ) AS trend_speed
FROM cte_trend_speed_time
ORDER BY 1, 2;
```

![result_q_2_6](img/result_q_2_6.PNG)

## **Q7**

> What is the successful delivery percentage for each runner?

```sql 
WITH cte_success_percentage AS (
  SELECT 
    runner_id,
    SUM(
      CASE 
        WHEN cancellation IS NULL THEN 1 
        ELSE 0
      END
    ) AS successful_delivery, 
    SUM(
      CASE 
        WHEN cancellation IS NOT NULL THEN 1 
        ELSE 0
      END
    ) AS cancelled_delivery
  FROM v_pizza_runner.runner_orders
  GROUP BY 1
)
SELECT 
  runner_id, 
  successful_delivery, 
  cancelled_delivery, 
  100 * successful_delivery / (successful_delivery + cancelled_delivery) AS success_percentage
FROM cte_success_percentage
ORDER BY 4 DESC;
```
![result_q_2_7](img/result_q_2_7.PNG)

# Ingredient Optimisation 

We focused our analysis into the pizzas, customers and runners. Now, we want to knoz more about the toppings. What toppings are the most purchases ? 

## **Q1**

> What are the standard ingredients for each pizza?

```sql
WITH cte_separate_string AS (
  SELECT  
    UNNEST(STRING_TO_ARRAY(toppings, ','))::NUMERIC AS topping_id,
    pizza_id
  FROM v_pizza_runner.pizza_recipes
), 
pizza_toppings_join AS (
  SELECT 
    cte_separate_string.*, 
    pizza_toppings.topping_name 
  FROM cte_separate_string 
  LEFT JOIN v_pizza_runner.pizza_toppings 
    ON cte_separate_string.topping_id = pizza_toppings.topping_id
)
SELECT 
  pizza_id, 
  STRING_AGG(topping_name, ', ') AS toppings
FROM pizza_toppings_join
GROUP BY 1
ORDER BY 1; 
```

![result_q_3_1](img/result_q_3_1.PNG)

## **Q2**

> What was the most commonly added extra?

```sql 
WITH cte_separate_string AS (
  SELECT  
    order_id,
    pizza_id, 
    --UNNEST(STRING_TO_ARRAY(exclusions, ','))::NUMERIC AS exclusion_id
    UNNEST(STRING_TO_ARRAY(extras, ','))::NUMERIC AS extra_id
  FROM v_pizza_runner.customer_orders
), 
pizza_toppings_join AS (
  SELECT 
    --cte_separate_string.exclusion_id, 
    cte_separate_string.extra_id, 
    pizza_toppings.topping_name 
  FROM cte_separate_string 
  LEFT JOIN v_pizza_runner.pizza_toppings 
    --ON cte_separate_string.exclusion_id = pizza_toppings.topping_id
    ON cte_separate_string.extra_id = pizza_toppings.topping_id
)
SELECT 
  topping_name, 
  COUNT(*) AS topping_count
FROM pizza_toppings_join
GROUP BY 1
ORDER BY 2 DESC; 
```
![result_q_3_2](img/result_q_3_2.PNG)


## **Q3**

> What was the most common exclusion?

We can take the same query as before and just uncomment few lines. 

```sql
WITH cte_separate_string AS (
  SELECT  
    order_id,
    pizza_id, 
    UNNEST(STRING_TO_ARRAY(exclusions, ','))::NUMERIC AS exclusion_id
    --UNNEST(STRING_TO_ARRAY(extras, ','))::NUMERIC AS extra_id
  FROM v_pizza_runner.customer_orders
), 
pizza_toppings_join AS (
  SELECT 
    cte_separate_string.exclusion_id, 
    --cte_separate_string.extra_id, 
    pizza_toppings.topping_name 
  FROM cte_separate_string 
  LEFT JOIN v_pizza_runner.pizza_toppings 
    ON cte_separate_string.exclusion_id = pizza_toppings.topping_id
    --ON cte_separate_string.extra_id = pizza_toppings.topping_id
)
SELECT 
  topping_name, 
  COUNT(*) AS topping_count
FROM pizza_toppings_join
GROUP BY 1
ORDER BY 2 DESC; 
```
![result_q_3_3](img/result_q_3_3.PNG)


## **Q4**

> Generate an order item for each record in the customers_orders table in the format of one of the following:
> * Meat Lovers
> * Meat Lovers - Exclude Beef
> * Meat Lovers - Extra Bacon
> * Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers

```sql
DROP TABLE IF EXISTS exclude_toppings; 
CREATE TEMP TABLE exclude_toppings AS (
  WITH cte_separate_string AS (
    SELECT 
      order_id,
      customer_id,
      pizza_id, 
      exclusions,
      UNNEST(STRING_TO_ARRAY(exclusions, ','))::NUMERIC AS exclusion_id, 
      _row_number
    FROM v_pizza_runner.customer_orders
  ), 
  cte_agg_string AS (
    SELECT DISTINCT 
      cte_separate_string.order_id, 
      cte_separate_string.customer_id,
      cte_separate_string.pizza_id, 
      cte_separate_string.exclusion_id, 
      pizza_toppings.topping_name, 
      cte_separate_string._row_number
    FROM cte_separate_string
    INNER JOIN v_pizza_runner.pizza_toppings 
      ON cte_separate_string.exclusion_id = pizza_toppings.topping_id
  )
  SELECT 
    order_id,
    customer_id,  
    pizza_id,
    CONCAT('Exclude ', STRING_AGG(topping_name, ', ')) AS exclusion_toppings, 
	_row_number
  FROM cte_agg_string
  GROUP BY order_id, customer_id, pizza_id, _row_number
);

DROP TABLE IF EXISTS extra_toppings; 
CREATE TEMP TABLE extra_toppings AS (
  WITH cte_separate_string AS (
    SELECT 
      order_id,
      pizza_id, 
      exclusions,
      UNNEST(STRING_TO_ARRAY(extras, ','))::NUMERIC AS extra_id
    FROM v_pizza_runner.customer_orders
  ), 
  cte_agg_string AS (
    SELECT DISTINCT 
      cte_separate_string.order_id, 
      cte_separate_string.pizza_id, 
      cte_separate_string.extra_id, 
      pizza_toppings.topping_name
    FROM cte_separate_string
    INNER JOIN v_pizza_runner.pizza_toppings 
      ON cte_separate_string.extra_id = pizza_toppings.topping_id
  )
  SELECT 
    order_id, 
    pizza_id, 
    CONCAT('Extra ', STRING_AGG(topping_name, ', ')) AS extra_toppings
  FROM cte_agg_string
  GROUP BY order_id, pizza_id
);

WITH agg_string_type AS (
  SELECT 
    customer_orders.order_id, 
    customer_orders.customer_id, 
    customer_orders.pizza_id, 
    pizza_names.pizza_name, 
    customer_orders.exclusions, 
    CASE 
      WHEN exclusions IS NOT NULL THEN exclude_toppings.exclusion_toppings
      ELSE '' 
    END AS exclusion_toppings, 
    customer_orders.extras, 
    CASE 
      WHEN extras IS NOT NULL THEN extra_toppings.extra_toppings
      ELSE '' 
    END AS extra_toppings
  FROM v_pizza_runner.customer_orders
  LEFT JOIN v_pizza_runner.pizza_names 
    ON customer_orders.pizza_id = pizza_names.pizza_id
  LEFT JOIN exclude_toppings 
    ON customer_orders.order_id = exclude_toppings.order_id 
    AND customer_orders.pizza_id = exclude_toppings.pizza_id
    AND customer_orders._row_number = exclude_toppings._row_number
  LEFT JOIN extra_toppings 
    ON customer_orders.order_id = extra_toppings.order_id 
    AND customer_orders.pizza_id = extra_toppings.pizza_id
)
SELECT 
  order_id, 
  customer_id, 
  pizza_id, 
  pizza_name, 
  exclusion_toppings, 
  extra_toppings,
  CASE 
    WHEN exclusion_toppings != '' AND extra_toppings != '' THEN CONCAT(pizza_name, ' - ', exclusion_toppings, ' - ', extra_toppings)
    WHEN exclusion_toppings != '' AND extra_toppings = '' THEN CONCAT(pizza_name, ' - ', exclusion_toppings)
    WHEN exclusion_toppings = '' AND extra_toppings != '' THEN CONCAT(pizza_name, ' - ', extra_toppings)
    ELSE pizza_name 
  END AS pizza_long_name
FROM agg_string_type
ORDER BY 1; 
```

![result_q_3_4](img/result_q_3_4.PNG)

## **Q5**

> Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients

**We reuse the `exclude_toppings` table that we created in the previous question for this question.** We have to add the `exclusion_id` column before. 

```sql 
DROP TABLE IF EXISTS exclude_toppings; 
CREATE TEMP TABLE exclude_toppings AS (
  WITH cte_separate_string AS (
    SELECT 
      order_id,
      customer_id,
      pizza_id, 
      exclusions,
      UNNEST(STRING_TO_ARRAY(exclusions, ','))::NUMERIC AS exclusion_id, 
      _row_number
    FROM v_pizza_runner.customer_orders
  ), 
  cte_agg_string AS (
    SELECT DISTINCT 
      cte_separate_string.order_id, 
      cte_separate_string.customer_id,
      cte_separate_string.pizza_id, 
      cte_separate_string.exclusion_id, 
      pizza_toppings.topping_name, 
      cte_separate_string._row_number
    FROM cte_separate_string
    INNER JOIN v_pizza_runner.pizza_toppings 
      ON cte_separate_string.exclusion_id = pizza_toppings.topping_id
  )
  SELECT 
    order_id,
    customer_id,  
    pizza_id,
	exclusion_id,
    CONCAT('Exclude ', STRING_AGG(topping_name, ', ')) AS exclusion_toppings, 
	_row_number
  FROM cte_agg_string
  GROUP BY order_id, customer_id, pizza_id, exclusion_id, _row_number
);
```

```sql
DROP TABLE IF EXISTS text_preparation_recipe;
CREATE TEMP TABLE text_preparation_recipe AS (
  WITH cte_recipe AS (
    SELECT
      customer_orders.order_id, 
      customer_orders.pizza_id,
      customer_orders.customer_id,
      UNNEST(STRING_TO_ARRAY(
        CASE 
          WHEN extras IS NOT NULL THEN CONCAT(pizza_recipes.toppings, ', ',  customer_orders.extras) 
          ELSE pizza_recipes.toppings
        END, 
        ','
      ))::NUMERIC AS topping_id, 
      customer_orders._row_number
    FROM v_pizza_runner.customer_orders 
    INNER JOIN v_pizza_runner.pizza_recipes
      ON customer_orders.pizza_id = pizza_recipes.pizza_id
  ), 
  cte_topping_number AS (
    SELECT 
      cte_recipe.order_id,
      cte_recipe.pizza_id, 
      cte_recipe.customer_id, 
      cte_recipe.topping_id, 
      pizza_toppings.topping_name, 
      cte_recipe._row_number
    FROM cte_recipe
    LEFT JOIN exclude_toppings 
      ON cte_recipe.pizza_id = exclude_toppings.pizza_id 
      AND cte_recipe.customer_id = exclude_toppings.customer_id
      AND cte_recipe.topping_id = exclude_toppings.exclusion_id 
      AND cte_recipe._row_number = exclude_toppings._row_number
    LEFT JOIN v_pizza_runner.pizza_toppings 
      ON cte_recipe.topping_id = pizza_toppings.topping_id
    WHERE exclusion_id IS NULL
    ORDER BY order_id, pizza_id, topping_id
  ),
  cte_final_text AS (
    SELECT 
      order_id, 
      pizza_id,
      topping_id,
      COUNT(topping_id) AS topping_number,
      topping_name,
      _row_number
    FROM cte_topping_number
    GROUP BY 1, 2, 3, 5, 6
  )
  SELECT 
    order_id, 
    topping_id,
    pizza_id,
    CASE
      WHEN topping_number != 1 THEN CONCAT(topping_number, 'x', topping_name)
      ELSE topping_name
    END AS topping_name,
    _row_number 
  FROM cte_final_text
); 

WITH cte_full_recipe_with_pizza_name AS (
  SELECT 
    text_preparation_recipe.order_id, 
    text_preparation_recipe.pizza_id,
    text_preparation_recipe._row_number,
    STRING_AGG(text_preparation_recipe.topping_name, ', ' order by text_preparation_recipe.topping_name) AS full_recipe
  FROM text_preparation_recipe 
  GROUP BY 1, 2, 3
) 
SELECT 
  cte_full_recipe_with_pizza_name.order_id,
  cte_full_recipe_with_pizza_name.pizza_id, 
  CONCAT(pizza_names.pizza_name, ': ', full_recipe) AS full_recipe
FROM cte_full_recipe_with_pizza_name 
LEFT JOIN v_pizza_runner.pizza_names
  ON cte_full_recipe_with_pizza_name.pizza_id = pizza_names.pizza_id
ORDER BY order_id, pizza_id;
```

![result_q_3_5](img/result_q_3_5.PNG)


## **Q6**

> What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?

**We can reuse the previoyus query.**

```sql
DROP TABLE IF EXISTS text_preparation_recipe;
CREATE TEMP TABLE text_preparation_recipe AS (
  WITH cte_recipe AS (
    SELECT
      customer_orders.order_id, 
      customer_orders.pizza_id,
      customer_orders.customer_id,
      UNNEST(STRING_TO_ARRAY(
        CASE 
          WHEN extras IS NOT NULL THEN CONCAT(pizza_recipes.toppings, ', ',  customer_orders.extras) 
          ELSE pizza_recipes.toppings
        END, 
        ','
      ))::NUMERIC AS topping_id, 
      customer_orders._row_number
    FROM v_pizza_runner.customer_orders 
    INNER JOIN v_pizza_runner.pizza_recipes
      ON customer_orders.pizza_id = pizza_recipes.pizza_id
  ), 
  cte_topping_number AS (
    SELECT 
      cte_recipe.order_id,
      cte_recipe.pizza_id, 
      cte_recipe.customer_id, 
      cte_recipe.topping_id, 
      pizza_toppings.topping_name, 
      cte_recipe._row_number
    FROM cte_recipe
    LEFT JOIN exclude_toppings 
      ON cte_recipe.pizza_id = exclude_toppings.pizza_id 
      AND cte_recipe.customer_id = exclude_toppings.customer_id
      AND cte_recipe.topping_id = exclude_toppings.exclusion_id 
      AND cte_recipe._row_number = exclude_toppings._row_number
    LEFT JOIN v_pizza_runner.pizza_toppings 
      ON cte_recipe.topping_id = pizza_toppings.topping_id
    WHERE exclusion_id IS NULL
    ORDER BY order_id, pizza_id, topping_id
  )
  SELECT * FROM cte_topping_number
); 

SELECT 
  topping_name, 
  COUNT(*) AS toppping_number
FROM text_preparation_recipe
GROUP BY 1 
ORDER BY 2 DESC; 
```

![result_q_3_6](img/result_q_3_6.PNG)

# Pricing and Ratings 

## **Q1**

> If a Meat Lovers pizza costs `$12` and Vegetarian costs `$10` and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?

```sql
SELECT
  SUM(
    CASE
      WHEN pizza_id = 1 THEN 12
      ELSE 10
    END
  ) AS revenue
FROM v_pizza_runner.customer_orders;
```

**Revenue : 160**

## **Q2**

> What if there was an additional `$1` charge for any pizza extras?
* Add cheese is `$1` extra

```sql
WITH cte_pizza_price AS (
  SELECT
    runner_orders.order_id, 
    runner_orders.runner_id,
    customer_orders.pizza_id, 
    customer_orders.extras, 
    CASE 
      WHEN pizza_id = 1 THEN 12
      ELSE 10
    END AS pizza_price, 
    customer_orders._row_number
  FROM v_pizza_runner.runner_orders
  LEFT JOIN v_pizza_runner.customer_orders 
    ON runner_orders.order_id = customer_orders.order_id 
  WHERE cancellation IS NULL
), 
cte_pizza_extra_price AS (
  SELECT 
   order_id, 
   runner_id, 
   pizza_id, 
   extras::NUMERIC, 
   pizza_price, 
   _row_number
  FROM cte_pizza_price 
  WHERE extras IS NULL
  
  UNION ALL
  
  SELECT 
    order_id, 
    runner_id, 
    pizza_id, 
    UNNEST(STRING_TO_ARRAY(extras, ','))::NUMERIC AS extras, 
    pizza_price, 
    _row_number
  FROM cte_pizza_price
), 
cte_total_extra_price AS (
  SELECT 
    order_id, 
    _row_number,
    pizza_price,
    SUM(
      CASE 
        WHEN extras IS NULL THEN 0 
        WHEN extras = 4 THEN 2
        ELSE 1
      END
    ) AS extra_price
  FROM cte_pizza_extra_price
  GROUP BY order_id, _row_number, pizza_price
)
SELECT
  SUM(pizza_price + extra_price) AS revenue
FROM cte_total_extra_price
ORDER BY 1;
```
**Revenue : 143**

## **Q3**

> The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, how would you design an additional table for this new dataset - generate a schema for this new table and insert your own data for ratings for each successful customer order between 1 to 5.

**Schema update.** You can find it in the beginning of this file. 

## **Q4**

```sql
DROP TABLE IF EXISTS avg_runner_speed; 
CREATE TEMP TABLE avg_runner_speed AS 
SELECT 
  runner_id, 
  order_id,
  ROUND(AVG(distance/duration)*60, 1) AS speed
FROM v_pizza_runner.pizza_join 
WHERE cancellation IS NULL
GROUP BY 1, 2;

SELECT DISTINCT
  customer_id, 
  pizza_join.order_id, 
  pizza_join.runner_id, 
  customer_ratings.rating, 
  order_time, 
  pickup_time, 
  pickup_minutes, 
  duration, 
  avg_runner_speed.speed, 
  COUNT(pizza_id) OVER(PARTITION BY customer_id, pizza_join.order_id) AS total_pizzas
FROM v_pizza_runner.pizza_join
LEFT JOIN v_pizza_runner.customer_ratings 
  ON pizza_join.order_id = customer_ratings.order_id
LEFT JOIN avg_runner_speed 
  ON pizza_join.order_id = avg_runner_speed.order_id
WHERE pickup_time IS NOT NULL
ORDER BY 2, 1;
```

![result_q_4_4](img/result_q_4_4.PNG)

## **Q5**

> If a Meat Lovers pizza was `$12` and Vegetarian `$10` fixed prices with no cost for extras and each runner is paid `$0.30` per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?

```sql 
WITH cte_income_with_fees AS (
  SELECT 
   order_id, 
   distance,
   SUM(CASE WHEN pizza_id = 1 THEN 1 ELSE 0 END) AS meatlover_count, 
   SUM(CASE WHEN pizza_id = 2 THEN 1 ELSE 0 END) AS vegetarian_count
  FROM v_pizza_runner.pizza_join
  WHERE cancellation IS NULL
  GROUP BY 1, 2
) 
SELECT 
  ROUND(
    SUM( 
      12 * meatlover_count + 10 * vegetarian_count - 0.30 * distance 
    ), 
    2
  ) AS revenue_without_fees
FROM cte_income_with_fees; 
```

**revenue_withtout_fees : 94.44$**

# Bonus Questions 

```sql
DROP TABLE IF EXISTS temp_pizza_names;
CREATE TEMP TABLE temp_pizza_names AS
SELECT * FROM pizza_runner.pizza_names;

INSERT INTO temp_pizza_names(pizza_id, pizza_name)
VALUES
  (3, 'Supreme');

DROP TABLE IF EXISTS temp_pizza_recipes;
CREATE TEMP TABLE temp_pizza_recipes AS
SELECT * FROM pizza_runner.pizza_recipes;

INSERT INTO temp_pizza_recipes(pizza_id, toppings)
SELECT
  3,
  STRING_AGG(topping_id::TEXT, ', ')
FROM pizza_runner.pizza_toppings;

SELECT * FROM temp_pizza_recipes;
```
