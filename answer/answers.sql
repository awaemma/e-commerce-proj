/* Part 2a -- NUMBER 1 QUESTION
       - what is the most ordered item based on the number of times it appears 
         in an order cart that checked out successfully?  

My approach to the solution:
THE SOLUTION USES CTEs IN A PROGRESSIVE WAY SUCH THAT EACH CTE LEVERAGES THE OUTPUT FROM A PREVIOUS CTE IF ANY
1. First, I expand all of the values within the event_data column into different columns using the ->> operator.
   This first CTE is named the event_data and acts as the base table for other transformations. 
   This is sorted by customer_id and timestamp to keep the order of events accurately.

2. Next, I begin to prepare the data to take care of items that were added to cart and removed. 
   This should not be included in the final result.
   One thing to note is that when an item is added to cart by a custmer, the item_id appears just once. 
   However, if the item is added and then removed from cart, we see a duplicate of the item_id.
   My observation shows that items added to cart and then removed were not re-added again.
   This would be a pointer to say that if an item_id appears twice within a customer_id group, it should be removed.
   An edge case would be when an item is added, removed and then re-added. This would make the item_id appear 3 times hence should be considered in the final result.
   If this happens, we would see that if an item_id count shows an even number, it should be discarded, otherwise keep it.
   
   In this next step, I created 2 CTEs. in the first CTE named event_data_count, I added a count colum that
   counts the number of times an item_id appears within a customer_id and item_id partition.
   In the second CTE named add_remove_cart_cleanup, I filter out all records where the count of item_id is 2.
   However, if I was to implement for an edge case scenario, I would exclude all counts that are even numbers e.g not in (2,4,6,8,10 etc)
   The result of this operation leaves us with item_ids that were added to cart and not removed.

3. Next step, we want to only keep items that were added to cart and eventually checked out successfully.
   However, the current state of the data does not allow for a good manipulation to retrieve only successful checkouts.
   So in each customer_id group, within the status column, I need to make sure that the status is same for each customer_id.
   This action will aid for appropriate filtering so no data is lost when summing the quantity column.
   
            There are only 4 possible status :-
                Null - which means there was no attempt to checkout
                Cancelled - Checkout was cancelled
                Failed - Checkout failed maybe due to system error
                Success - Checkout was successfull.

   In the update_status CTE, I created a status2 column that allows me to update the status of each customer_id
   across the rows within the customer_id partition.

4. Finally, the result of the previous CTE is joined with the products table so the product name can be retrievd.
   I then filtered down to where status2 = success and item_id is not null. Then summed up the quantity for each product.
   Lastly, limit the result to 1 since we are interested in the most ordered item.
   Ans: Sony PlayStation 5 with a count of 1,172
*/

-- First Step (Please refer to the note starting from line 7 above.)
with event_data as (
select 
	customer_id
	,event_data
	,event_data ->> 'event_type' as event_type
	,event_data ->> 'timestamp' as timestamp
	,event_data ->> 'item_id' as item_id
	,event_data ->> 'quantity' as quantity
    ,event_data ->> 'status' as status
	,event_data ->> 'order_id' as order_id
from alt_school.events
where event_data ->> 'event_type' != 'visit'
order by customer_id, event_data ->> 'timestamp' ),
-- First step ends here
-------------------------------------------------------------------
-------------------------------------------------------------------
--- Second step (Please refer to the note starting from line 11 above.)
event_data_count as (
select 
	 customer_id
	,event_type
	,status
	,item_id
	,quantity
	,count(item_id) over(partition by customer_id,item_id )
from event_data
group by customer_id,event_type,status,item_id,quantity,timestamp
order by timestamp ),
-- excluding items that were added and also removed from cart
add_remove_cart_cleanup as (
select *
from event_data_count
where count <> 2
order by customer_id ),
-- Second step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
--- Third step (Please refer to the note starting from line 26 above.)
update_status as (
select *,
	   case 
		   when status is not null then status 
		   when status is null and count(*) filter (where status = 'cancelled') over (partition by customer_id) > 0 then 'cancelled'
		   when status is null and count(*) filter (where status = 'failed') over (partition by customer_id) > 0 then 'failed'
		   when status is null and count(*) filter (where status = 'success') over (partition by customer_id) > 0 then 'success'
		   else null 
	   end as status2
from add_remove_cart_cleanup
)
-- Third step ends here
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
--- Fourth and Final step (Please refer to the note starting from line 40 above.)
select 
	   p.id as product_id
	  ,p.name as product_name
	  ,sum(cast(quantity as int)) as num_times_in_successful_orders
from update_status e
left join alt_school.products p on cast(e.item_id as int) = p.id
where status2 = 'success' and item_id is not null
group by p.id,p.name
order by num_times_in_successful_orders desc
limit 1 ; 
--QUESTION 1 ENDS HERE
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

/* Part 2a CONT'D  NUMBER 2 QUESTION
       - without considering currency, and without using the line_item table, find the top 5 spenders 
         
My approach to the solution: THE SOLUTION USES CTEs IN A PROGRESSIVE WAY SUCH THAT EACH CTE LEVERAGES THE OUTPUT FROM A PREVIOUS CTE IF ANY
I will leverage on part of the solution above to answer this question. To get total spend, we need to ensure
that we are working with only successfully checked out items.
Therefore, the combination of the CTEs event_data, event_data_count and update_status will ensure
that we are working with items that have been checked out succefully.

1. To answer this question, I created an additional CTE, customer_spend, which joins the update_status CTE 
   to the customer and product tables so I can retrieve the customer location and price of items.
   Joining these 3 allows me to multiply the quantity of each item by it's price to get actual amount spent.

2. The final step then sums up the amount spent for each customer, grouped by the customer_id and location.
*/

-- First Step (Please refer to the note starting from line 7 above.)
with event_data as (
select 
	customer_id
	,event_data
	,event_data ->> 'event_type' as event_type
	,event_data ->> 'timestamp' as timestamp
	,event_data ->> 'item_id' as item_id
	,event_data ->> 'quantity' as quantity
    ,event_data ->> 'status' as status
	,event_data ->> 'order_id' as order_id
from alt_school.events
where event_data ->> 'event_type' != 'visit'
order by customer_id, event_data ->> 'timestamp' ),
-- First step ends here
-------------------------------------------------------------------
-------------------------------------------------------------------
--- Second step (Please refer to the note starting from line 11 above.)
event_data_count as (
select 
	 customer_id
	,event_type
	,status
	,item_id
	,quantity
	,count(item_id) over(partition by customer_id,item_id )
from event_data
group by customer_id,event_type,status,item_id,quantity,timestamp
order by timestamp ),
-- excluding items that were added and also removed from cart
add_remove_cart_cleanup as (
select *
from event_data_count
where count <> 2
order by customer_id ),
-- Second step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
--- Third step (Please refer to the note starting from line 26 above.)
update_status as (
select *,
	   case 
		   when status is not null then status 
		   when status is null and count(*) filter (where status = 'cancelled') over (partition by customer_id) > 0 then 'cancelled'
		   when status is null and count(*) filter (where status = 'failed') over (partition by customer_id) > 0 then 'failed'
		   when status is null and count(*) filter (where status = 'success') over (partition by customer_id) > 0 then 'success'
		   else null 
	   end as status2
from add_remove_cart_cleanup
),
-- Third step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
-- Fourth Step (Please refer to the note starting from line 124 above.)
customer_spend as (
select 
	e.customer_id
	,quantity
	,p.price
	,(p.price * cast(e.quantity as int)) as amount
	,c.location
from update_status e
left join alt_school.products p on cast(e.item_id as int) = p.id
left join alt_school.customers c on e.customer_id = c.customer_id
where item_id is not null and status2 = 'success' )
-- Fourth step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
-- Final step  (Please refer to the note starting from line 128 above.)
select 
	customer_id
	,location
	,sum(amount) as total_spend
from customer_spend
group by customer_id,location
order by total_spend desc
limit 5 
--QUESTION 2 ENDS HERE
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

/* Part 2b NUMBER 3 QUESTION
- using the events table, Determine the most common location (country) where successful checkouts occurred. 

 My approach to the solution: 
 THE SOLUTION USES CTEs IN A PROGRESSIVE WAY SUCH THAT EACH CTE LEVERAGES THE OUTPUT FROM A PREVIOUS CTE IF ANY
 Again, I will leverage on part of the solution in question 1 to answer this question.Ultimately, we are still
 interested in successful checkouts, however this time we want to count the number of checkouts in each location.

 1. In addition to the CTEs event_data, event_data_count, add_remove_cart_cleanup and update_status which gets me to the point of
    handling items that were added and removed, I have created another CTE,checkout_count, which allows me
    to assign values of 0 or 1 to a new column depending on whethere the status is successful or not.

 2. The result of the checkout_count CTE is now joined with the customer's table since location is of interest.
    The final result returns the top location.
    Ans: Korea with a count of 17 successful checkouts.
*/
with event_data as (
select 
	customer_id
	,event_data
	,event_data ->> 'event_type' as event_type
	,event_data ->> 'timestamp' as timestamp
	,event_data ->> 'item_id' as item_id
	,event_data ->> 'quantity' as quantity
    ,event_data ->> 'status' as status
	,event_data ->> 'order_id' as order_id
from alt_school.events
where event_data ->> 'event_type' != 'visit'
order by customer_id, event_data ->> 'timestamp' ),
-- First step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
--- Second step starts here (Please refer to the note starting from line 26 above.)
event_data_count as (
select 
	 customer_id
	,event_type
	,status
	,item_id
	,quantity
	,count(item_id) over(partition by customer_id,item_id )
from event_data
group by customer_id,event_type,status,item_id,quantity,timestamp
order by timestamp ),
-- excluding items that were added and also removed from cart
add_remove_cart_cleanup as (
select *
from event_data_count
where count <> 2
order by customer_id ),
-- Second step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
--- Third step (Please refer to the note starting from line 26 above.)
update_status as (
select *,
	   case 
		   when status is not null then status 
		   when status is null and count(*) filter (where status = 'cancelled') over (partition by customer_id) > 0 then 'cancelled'
		   when status is null and count(*) filter (where status = 'failed') over (partition by customer_id) > 0 then 'failed'
		   when status is null and count(*) filter (where status = 'success') over (partition by customer_id) > 0 then 'success'
		   else null 
	   end as status2
from add_remove_cart_cleanup
),
-- add checkout count which allows me to count one checkout per customer
checkout_count as (
select *
	,case
	    when status is null then 0
	    else 1
	end as checkout_count
from update_status e )

select 
	 c.location
	,sum(checkout_count) checkout_count
from checkout_count cc
left join alt_school.customers c on cc.customer_id = c.customer_id
where status2 = 'success'
group by c.location
order by checkout_count desc
limit 1
--QUESTION 3 ENDS HERE
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

/* Part 2b NUMBER 4 QUESTION
- using the events table, identify the customers who abandoned their carts and count the number of 
  events (excluding visits) that occurred before the abandonment.

 Based on the instruction, I will consider the following as abandoned carts
    -- Customers who add to cart
    -- Customers who remove from cart

My approach to the solution: 
 THE SOLUTION USES CTEs IN A PROGRESSIVE WAY SUCH THAT EACH CTE LEVERAGES THE OUTPUT FROM A PREVIOUS CTE IF ANY

1. Similar to previous solutions, we first extract all the possible events in the event_data column.
2. Then proceed to implement ways to ensure that the number of events before abandonment are captured.
  */
-- First Step -- First we expand each of the events columns into different columns.
with event_data as (
select 
	customer_id
	,event_data
	,event_data ->> 'event_type' as event_type
	,event_data ->> 'timestamp' as timestamp
	,event_data ->> 'item_id' as item_id
	,event_data ->> 'quantity' as quantity
    ,event_data ->> 'status' as status
	,event_data ->> 'order_id' as order_id
from alt_school.events
where event_data ->> 'event_type' != 'visit'
order by customer_id, event_data ->> 'timestamp' ),
-- First step ends here
-------------------------------------------------------------------
-------------------------------------------------------------------
--- Second step -- Here I begin to implement what would allow me pick only customers of interest.
-- If a customer_id ever check outs, I want to ensure that all the null values in the status
--- coulum for these customer_ids are filled with the status of the checkout. Doing this will eventually 
--- allow me to exclude them during the count of events for customers who abdandoned their cart.
-- This led to the creation of the status2 column.
event_data_count as (
select 
	 customer_id
	,event_type
	,status
    ,case 
		   when status is not null then status 
		   when status is null and count(*) filter (where status = 'cancelled') over (partition by customer_id) > 0 then 'cancelled'
		   when status is null and count(*) filter (where status = 'failed') over (partition by customer_id) > 0 then 'failed'
		   when status is null and count(*) filter (where status = 'success') over (partition by customer_id) > 0 then 'success'
		   else null 
	   end as status2
from event_data
group by customer_id,event_type,status,timestamp
order by timestamp )
-- Second step ends here
----------------------------------------------------------------------------
----------------------------------------------------------------------------
-- Finally, I proceed to count the num_events for each customer_id
select 
	customer_id
	,count(event_type) as num_events
from event_data_count
where status2 is null --- this filter ensures that we do not count customers who checked out
group by customer_id
order by num_events desc  
--QUESTION 4 ENDS HERE
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

/* Part 2b NUMBER 5 QUESTION
Find the average number of visits per customer, considering only customers 
who completed a checkout! return average_visits to 2 decimal place

My approach to the solution: 
 THE SOLUTION USES CTEs IN A PROGRESSIVE WAY SUCH THAT EACH CTE LEVERAGES THE OUTPUT FROM A PREVIOUS CTE IF ANY

1. Similar to previous solutions, we first extract all the possible events in the event_data column.
2. Create and implement strategies to count visit of only customers who checkout successfully.
*/

-- First Step, we first extract all the fields from the event_data column.
with event_data as (
select 
	customer_id
	,event_data
	,event_data ->> 'event_type' as event_type
	,event_data ->> 'timestamp' as timestamp
	,event_data ->> 'item_id' as item_id
	,event_data ->> 'quantity' as quantity
    ,event_data ->> 'status' as status
	,event_data ->> 'order_id' as order_id
from alt_school.events
order by customer_id, event_data ->> 'timestamp' ),
-- First step ends here
-------------------------------------------------------------------
-------------------------------------------------------------------
-- Second step - 
event_data_count as (
select 
	 customer_id
	,event_type
	,status
    ,case 
		   when status is not null then status 
		   when status is null and count(*) filter (where status = 'cancelled') over (partition by customer_id) > 0 then 'cancelled'
		   when status is null and count(*) filter (where status = 'failed') over (partition by customer_id) > 0 then 'failed'
		   when status is null and count(*) filter (where status = 'success') over (partition by customer_id) > 0 then 'success'
		   else null 
	   end as status2
from event_data
group by customer_id,event_type,status,timestamp
order by timestamp ),
-- Third step. Here we consider only customers with successful check outs and then count the visits per customer.
customer_visit_count as (
select 
	customer_id
   ,count(event_type) as visit_count
from event_data_count
where status2 = 'success' and event_type = 'visit'
group by customer_id )
-- Finally, we find the average number of visits and rounding it off to 2 decimal places.
select round(avg(visit_count),2)
from customer_visit_count
-- ANS - Average visist = 4.32
--QUESTION 5 ENDS HERE