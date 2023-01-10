**SQL COURSE OF DANNY MA https://www.datawithdanny.com/**
LinkedIn : https://www.linkedin.com/in/datawithdanny/

In the end of the course, we have 8 challenges to do in 8 weeks. 

# FIRST CHALLENGE : Danny's Dinner 
# CONTEXT 

A restaurant needs help to stay afloat. 
Three kinds of food : sushi, curry and ramen. 

The restaurant has captured some very basic data from their few months of operation but have no idea how to use their data to help them run the business

# PROBLEM STATEMENT 

Danny wants to use the data to answer a few simple questions about his customers, especially about their visiting patterns, how much money they’ve spent and also which menu items are their favourite. Having this deeper connection with his customers will help him deliver a better and more personalised experience for his loyal customers.

**The restaurant's owner plans to expand the existing customer loyalty program.**

# DATASETS 

Three datasets : 
* sales 
* menu 
* members 

## ERD 

![erd](img/erd.PNG)


## TABLE 1 : SALES 

The `sales` table is composed of : 
- **customer_id** - FOREIGN KEY 
- **order_date** - Date when the product is ordered by the customer. 
- **product_id** - FOREIGN KEY - id of the product that was ordered

![sales_table](img/sales_table.PNG)

## TABLE 2 : MENU

The `menu` table is composed of : 
- **product_id** - PRIMARY KEY 
- **product_name** - Name of the product
- **price** - Price in $ 
of the product

![menu_table](img/menu_table.PNG)

## TABLE 3 : MEMBERS 

The `members` table is composed of : 
- **customer_id** - PRIMARY KEY 
- **join_date** - When a customer joined the beta version of the Danny’s Diner loyalty program.
