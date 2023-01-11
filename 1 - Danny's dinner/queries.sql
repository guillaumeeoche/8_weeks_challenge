/* Creating Schema */

DROP SCHEMA IF EXISTS dannys_diner;
CREATE SCHEMA dannys_diner;

DROP TABLE IF EXISTS dannys_diner.sales;
CREATE TABLE dannys_diner.sales (
  customer_id VARCHAR(1),
  order_date DATE,
  product_id INTEGER
);

INSERT INTO dannys_diner.sales VALUES
  ('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3');
 
DROP TABLE IF EXISTS dannys_diner.menu;
CREATE TABLE dannys_diner.menu (
  product_id INTEGER,
  product_name VARCHAR(5),
  price INTEGER
);
INSERT INTO dannys_diner.menu VALUES
  ('1', 'sushi', '10'),
  ('2', 'curry', '15'),
  ('3', 'ramen', '12');
  
DROP TABLE IF EXISTS dannys_diner.members;
CREATE TABLE dannys_diner.members (
  customer_id VARCHAR(1),
  join_date DATE
);
INSERT INTO dannys_diner.members VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09');

/* Creating View */

DROP VIEW IF EXISTS dannys_diner.sales_menu;
CREATE VIEW dannys_diner.sales_menu AS 
SELECT 
  sales.customer_id, 
  sales.order_date,
  sales.product_id, 
  menu.product_name, 
  menu.price, 
  ROW_NUMBER() OVER(
    PARTITION BY sales.customer_id
    ORDER BY order_date
  ) AS ranking_date, 
  join_date, 
  CASE 
    WHEN join_date IS NULL THEN 'N'
    WHEN join_date > order_date THEN 'N'
    ELSE 'Y'
  END AS member
FROM dannys_diner.sales
LEFT JOIN dannys_diner.menu
  ON sales.product_id = menu.product_id
LEFT JOIN dannys_diner.members
  ON sales.customer_id = members.customer_id;

/* ------------------------
    CASE STUDY QUESTIONS
   ------------------------

Q1 - What is the total amount each customer spent at the restaurant?
*/

SELECT 
  customer_id, 
  SUM(price) AS total_price
FROM dannys_diner.sales_menu
GROUP BY 1
ORDER BY 2 DESC; 

/*
Q2 - How many days has each customer visited the restaurant?
*/

SELECT 
  customer_id, 
  COUNT(DISTINCT order_date) AS days_visiting_number
FROM dannys_diner.sales_menu 
GROUP BY 1
ORDER BY 2 DESC; 

/*
Q3 - What was the first item from the menu purchased by each customer?
*/

SELECT DISTINCT 
  customer_id, 
  product_name AS first_purchased_product
FROM dannys_diner.sales_menu
WHERE ranking_date = 1;

/*
Q4 - What is the most purchased item on the menu and how many times was it purchased by all customers?
*/

SELECT 
  product_name, 
  COUNT(*) AS total_pourchases
FROM dannys_diner.sales_menu
GROUP BY 1 
ORDER BY 2 DESC
LIMIT 1;

/*
Q5 - Which item was the most popular for each customer?
*/

WITH cte_count AS (
  SELECT 
    customer_id, 
    product_name, 
    COUNT(*) AS total_pourchases
  FROM dannys_diner.sales_menu
  GROUP BY 1, 2
),
cte_ranking AS (
  SELECT 
    *, 
    RANK() OVER(
      PARTITION BY customer_id
      ORDER BY total_pourchases DESC
    ) AS ranking_count
  FROM cte_count
)
SELECT 
  customer_id,
  product_name, 
  total_pourchases 
FROM cte_ranking
WHERE ranking_count = 1;

/*
Q6 - Which item was purchased first by the customer after they became a member?
*/

WITH cte_ranking AS (
  SELECT 
    customer_id, 
    product_name,
    order_date, 
    join_date,
    ROW_NUMBER() OVER(
      PARTITION BY customer_id
      ORDER BY order_date
    ) AS ranking_date
  FROM dannys_diner.sales_menu
  WHERE join_date IS NOT NULL 
    AND order_date >= join_date 
) 
SELECT
  customer_id, 
  product_name 
FROM cte_ranking
WHERE ranking_date = 1;

/*
Which item was purchased just before the customer became a member? 
*/

WITH cte_ranking AS (
  SELECT 
    customer_id, 
    product_name,
    order_date, 
    join_date,
    price,
    RANK() OVER(
      PARTITION BY customer_id
      ORDER BY 
        order_date DESC
    ) AS ranking_date
  FROM dannys_diner.sales_menu
  WHERE join_date IS NOT NULL 
    AND order_date < join_date::DATE
) 
SELECT
  customer_id, 
  product_name
FROM cte_ranking
WHERE ranking_date = 1;

/*
Q8 - What is the total items and amount spent for each member before they became a member?
*/

WITH cte_before_member AS (
  SELECT 
    customer_id, 
    product_name,
    order_date, 
    join_date,
    price
  FROM dannys_diner.sales_menu
  WHERE join_date IS NOT NULL 
    AND order_date < join_date 
) 
SELECT 
  customer_id, 
  COUNT(product_name) AS product_count, 
  SUM(price) AS total_price
FROM cte_before_member 
GROUP BY customer_id;

/*
Q9 - If each `$`1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
*/

WITH cte_points_number AS (
  SELECT 
    customer_id, 
    product_name,
    order_date, 
    join_date,
    price, 
    CASE 
      WHEN product_name = 'sushi' THEN 20 * price
      ELSE 10 * price
    END AS points_number
  FROM dannys_diner.sales_menu
) 
SELECT 
  customer_id, 
  SUM(points_number) AS total_points
FROM cte_points_number
GROUP BY 1
ORDER BY 2 DESC;

/*
Q10 - In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
*/

SELECT 
  customer_id, 
  SUM(
    CASE 
      WHEN product_name = 'sushi' THEN 20 * price
      WHEN order_date BETWEEN join_date::DATE  AND 
      (join_date::DATE+6) THEN 20 * price
      ELSE 10 * price
    END 
  ) AS total_points
FROM dannys_diner.sales_menu
WHERE join_date IS NOT NULL
  AND order_date <= '2021-01-31'
GROUP BY 1
ORDER BY 2 DESC;

/*
Reverse engineering part

Q11/Q12 - Recreate the tables, you cand find them in case_study.md
*/

SELECT
  customer_id, 
  order_date, 
  product_name,
  price, 
  member
FROM dannys_diner.sales_menu
ORDER BY 1, 2;

SELECT
  customer_id, 
  order_date, 
  product_name,
  price, 
  member, 
  CASE 
    WHEN member = 'N' THEN NULL 
    ELSE DENSE_RANK() OVER(
      PARTITION BY 
        customer_id,
        member
      ORDER BY 
        order_date, 
        price DESC
    )
  END AS ranking
FROM dannys_diner.sales_menu;