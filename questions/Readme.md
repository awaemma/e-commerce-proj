# Alt school Data Engineering second semester exams

**Warning**: Please be sure to read the readme.md file at the root of the project folder before starting!! remeber do not enforce referential intergrity in your table DDL statements!! Also be advised to spend some time really understanding data in the events table before attmepting the questions as the dataset was designed to be a tasking one to work with. Good luck!!

## Part 1

Based on the table diagrams provided, you're expected to complete the creation of tables in `infra_setup.sql` and also the loading of data int these tables. Note that you cannot proceed to answers questions in part two of this assesment if you do not setup and load the tables

## Part 2a

one common task performed in e-commerce is exploring customer orders to understand how products and customers are performing on you platform. 
consider the following questions below:

- what is the most ordered item based on the number of times it appears in an order cart that checked out successfully?
  you are expected to return the product_id, and product_name and num_times_in_successful_orders where:

  - product_id: the uuid string uniquely representing the product
  - product_name: the name of the product as provided in the product table
  - the number of times the product appeared in a a users cart that was successfully checked out. if ten customers added the item to their carts and only 8 of them successfully checked out and paid, then the answer should be 8 not 10.

- without considering currency, and without using the line_item table, find the top 5 spenders
  you are exxpected to return the customer_id, location, total_spend where:

  - customer_id: uuid string that uniquely identifies a customer
  - location - the customer's location
  - total_spend - the total amount of money spent on orders


## part 2b

a cart is considered abandoned if a user fails to checkout i.e the user adds/removes items from their cart but never proceeds to pay for their order. consider the following questions:

- using the events table, Determine **the most common location** (country) where successful checkouts occurred. return `location` and `checkout_count` where:
    - location: the name of the location
    - checkout_count: the number of checkouts that occured in the location

- using the events table, identify the customers who abandoned their carts and count the number of events (excluding visits) that occurred before the abandonment. return the `customer_id` and `num_events` where:

    - customer_id: id uniquely identifying the customers
    - num_events: the number of events excluding visits that occured before abandonment

- Find the average number of visits per customer, considering only customers who completed a checkout! return average_visits to 2 decimal place

    - average_visits: this number is a metric that suggests the avearge number of times a customer visits the website before they make a successful transaction!

