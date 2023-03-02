--schema init 

DROP SCHEMA IF EXISTS pizza_runner CASCADE;
CREATE SCHEMA pizza_runner; 

CREATE TABLE pizza_runner.runners (
    runner_id INT, 
    registration_date DATE
); 

INSERT INTO pizza_runner.runners (runner_id, registration_date)
VALUES 
(1, '2021-01-01'), 
(2, '2021-01-03'), 
(3, '2021-01-08'), 
(4, '2021-01-15'); 

CREATE TABLE pizza_runner.customer_orders (
    order_id INT, 
    customer_id INT, 
    pizza_id INT, 
    exclusions VARCHAR(4), 
    extras VARCHAR(4), 
    order_time TIMESTAMP
);  

INSERT INTO pizza_runner.customer_orders (order_id, customer_id, pizza_id, exclusions, extras, order_time)
VALUES 
(1, 101, 1, '', '', '2021-01-01 18:05:02'), 
(2, 101, 1, '', '', '2021-01-01 19:00:52'), 
(3, 102, 1, '', '', '2021-01-02 23:51:23'), 
(3, 102, 2, '', 'NaN', '2021-01-02 23:51:23'), 
(4, 103, 1, '4', '', '2021-01-01 18:05:02'), 
(4, 103, 1, '4', '', '2021-01-01 18:05:02'), 
(4, 103, 2, '4', '', '2021-01-01 18:05:02'), 
(5, 104, 1, 'null', '1', '2021-01-01 18:05:02'), 
(6, 101, 2, 'null', 'null', '2021-01-01 18:05:02'), 
(7, 105, 2, 'null', '1', '2021-01-01 18:05:02'), 
(8, 102, 1, 'null', 'null', '2021-01-01 18:05:02'), 
(9, 103, 1, '4', '1,5', '2021-01-01 18:05:02'), 
(10, 104, 1, 'null', 'null', '2021-01-01 18:05:02'), 
(10, 104, 1, '2,6', '1,4', '2021-01-01 18:05:02'); 


CREATE TABLE pizza_runner.runner_orders (
    order_id INT, 
    runner_id INT, 
    pickup_time VARCHAR(19), 
    distance VARCHAR(7), 
    duration VARCHAR(10), 
    cancellation VARCHAR(23)
);  

INSERT INTO pizza_runner.runner_orders (order_id, runner_id, pickup_time, distance, duration, cancellation)
VALUES 
(1, 1, '2021-01-01 18:15:34', '20km', '32 minutes', ''), 
(2, 1,  '2021-01-01 19:10:54', '20km', '27 minutes', ''),
(3, 1, '2021-01-03 00:12:37', '13.4km', '20 mins', 'NaN'), 
(4, 2, '2021-01-04 13:53:03', '23.4', '40', 'NaN'), 
(5, 3, '2021-01-08 21:10:57', '10', '15', 'NaN'), 
(6, 3, 'null', 'null', 'null', 'Restaurant Cancellation'), 
(7, 2, '2020-01-08 21:30:45', '25km', '25mins', 'null'), 
(8, 2, '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'), 
(9, 2, 'null', 'null', 'null', 'Customer Cancellation'), 
(10, 1, '2020-01-11 18:50:20', '10km', '10minutes', 'null'); 


CREATE TABLE pizza_runner.pizza_names (
    pizza_id INT, 
    pizza_name TEXT
);  

INSERT INTO pizza_runner.pizza_names (pizza_id, pizza_name)
VALUES 
(1, 'Meat Lovers'), 
(2, 'Vegetarian');

CREATE TABLE pizza_runner.pizza_recipes (
    pizza_id INT, 
    toppings TEXT
);  

INSERT INTO pizza_runner.pizza_recipes (pizza_id, toppings)
VALUES 
(1, '1, 2, 3, 4, 5, 6, 8, 10'), 
(2, '4, 6, 7, 9, 11, 12');


CREATE TABLE pizza_runner.pizza_toppings (
    topping_id INT, 
    topping_name TEXT
);  

INSERT INTO pizza_runner.pizza_toppings (topping_id, topping_name)
VALUES 
(1, 'Bacon'), 
(2, 'BBQ Sauce'), 
(3, 'Beef'), 
(4, 'Cheese'), 
(5, 'Chicken'), 
(6, 'Mushrooms'), 
(7, 'Onions'), 
(8, 'Pepperoni'), 
(9, 'Peppers'), 
(10, 'Salami'), 
(11, 'Tomatoes'), 
(12, 'Tomato Sauce');

--init view to not overlap data sources 

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

/*PIZZA INSIGHTS*/

--how many pizzas were ordered?

SELECT 
  COUNT(*) AS total_purchases
FROM v_pizza_runner.customer_orders;

--how many unique customer orders were made?

SELECT 
  COUNT(DISTINCT order_id) AS total_unique_orders
FROM v_pizza_runner.customer_orders; 


--how many successful orders were delivered by each runner?

SELECT 
  runner_id, 
  COUNT(order_id) AS number_deliveries
FROM v_pizza_runner.runner_orders
WHERE cancellation IS NULL
GROUP BY 1;

--how many of each type of pizza was delivered?

SELECT 
  pizza_name,
  COUNT(*) AS number_pizzas
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1;

--how many Vegetarian and Meatlovers were ordered by each customer?

SELECT
  customer_id,
  SUM(CASE WHEN pizza_id = 1 THEN 1 ELSE 0 END) AS meatlovers,
  SUM(CASE WHEN pizza_id = 2 THEN 1 ELSE 0 END) AS vegetarian
FROM v_pizza_runner.customer_orders
GROUP BY 1
ORDER BY 1;

--what was the maximum number of pizzas delivered in a single order?

SELECT 
  order_id,
  COUNT(pizza_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 1;

--for each customer, how many delivered pizzas had at least 1 change and how many had no changes?

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

--how many pizzas were delivered that had both exclusions and extras?

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


--what was the total volume of pizzas ordered for each hour of the day?

SELECT 
  EXTRACT("HOUR" from order_time) AS hour, 
  COUNT(pizza_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 1;

--what was the volume of orders for each day of the week?


SELECT 
  TO_CHAR(order_time, 'Day') AS day_week, 
  COUNT(order_id) AS number_pizzas
FROM v_pizza_runner.pizza_join
GROUP BY 1
ORDER BY 1;


/*RUNNER AND CUSTOMER INSIGHTS*/

--how many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)

SELECT 
  DATE_TRUNC('week', registration_date)::DATE + 4 AS registration_week, 
  COUNT(*) AS runners_count
FROM v_pizza_runner.runners
GROUP BY 1
ORDER BY 1;

--what was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?

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

--is there any relationship between the number of pizzas and how long the order takes to prepare?

SELECT DISTINCT
  order_id,
  pickup_minutes, 
  COUNT(pizza_id) AS pizza_count
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1, 2
ORDER BY 3;

--what was the average distance travelled for each customer?

SELECT 
  customer_id,
  ROUND(AVG(DISTINCT distance), 1) AS avg_distance
FROM v_pizza_runner.pizza_join
WHERE cancellation IS NULL
GROUP BY 1
ORDER BY 1;

--what was the difference between the longest and shortest delivery times for all orders?

SELECT 
  MAX(duration) - MIN(duration) AS max_diff_delivery_time
FROM v_pizza_runner.runner_orders
WHERE cancellation IS NULL;

--what was the average speed for each runner for each delivery and do you notice any trend for these values?

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


--what is the successful delivery percentage for each runner?

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

/*INGREDIENTS INSIGHTS*/

--what are the standard ingredients for each pizza?

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

--what was the most commonly added extra?

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

--what was the most common exclusion?

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

/*Generate an order item for each record in the customers_orders table in the format of one of the following:
* Meat Lovers
* Meat Lovers - Exclude Beef
* Meat Lovers - Extra Bacon
* Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers
*/

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

--generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients

--we have to add a column to exclude_toppings : exclusion_id 

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

/*-------------------------------------------*/

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

--what is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?

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


/* PRICINGS AND RATINGS INSIGHTS */

--if a Meat Lovers pizza costs `$12` and Vegetarian costs `$10` and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?

SELECT
  SUM(
    CASE
      WHEN pizza_id = 1 THEN 12
      ELSE 10
    END
  ) AS revenue
FROM v_pizza_runner.customer_orders;

/*what if there was an additional `$1` charge for any pizza extras?
Add cheese is `$1` extra*/

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

--The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, how would you design an additional table for this new dataset - generate a schema for this new table and insert your own data for ratings for each successful customer order between 1 to 5.

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

--if a Meat Lovers pizza was `$12` and Vegetarian `$10` fixed prices with no cost for extras and each runner is paid `$0.30` per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?

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

/* SUPP */

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

