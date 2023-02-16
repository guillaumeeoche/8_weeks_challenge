--schema init 

DROP SCHEMA IF EXISTS pizza_runner CASCADE;
CREATE SCHEMA pizza_runner; 

CREATE TABLE pizza_runner.runners (
    runner_id INT, 
    registration_date TIMESTAMP
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
    exclusions INT, 
    extras

)

