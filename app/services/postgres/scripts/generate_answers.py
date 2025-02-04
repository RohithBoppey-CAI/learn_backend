# importing all the required modes

from ..schema import Menu, Members, Sales
from .connector import psql_execute_single
from .postgres_utils import exec_all

from sqlalchemy import select, func, and_

# the main file which will use the psql & sql alchmey to write the code and finally execute it using the psql function
# the next idea is that this query would then go into redis

'''
https://www.db-fiddle.com/f/2rM8RAnq7h5LLDTzZiRWcd/138
Link to edit the DB Fiddle.
'''

async def exec(final_query, cols):
    print(final_query)
    res = await psql_execute_single(final_query)
    res = exec_all(res, cols)
    # print(res)
    return res


def caller(num):
    obj = {
        1: Q1,
        2: Q2,
        3: Q3, 
        4: Q4,
        5: Q5,
        6: Q6,
        7: Q7,
        8: Q8,
        9: Q9,
        10: Q10,
    }    
    return obj[num]


"""
Find the total spend of each customer: When we join sales, menu, we get customer info as well, so we can do just join and sum by group by.

select customer_id, sum(price) from sales s join menu m on s.product_id = m.product_id
group by customer_id

| customer_id | sum |
| ----------- | --- |
| B           | 74  |
| C           | 36  |
| A           | 76  |


"""
async def Q1():
    joined_query = select(
        Sales.customer_id, Menu.product_id, Menu.price
        ).join(Menu, Menu.product_id == Sales.product_id).cte('joined')
    
    grouped_query = select(
        joined_query.c.customer_id, func.sum(joined_query.c.price)
    ).group_by(joined_query.c.customer_id)
    
    res = await exec(grouped_query, cols = ['customer_id', 'total_amount_spent'])
    return res    
    
    
    
"""
How many days has each customer visited the restaurant?

select s.customer_id, count(distinct s.order_date) 
from sales s 
group by s.customer_id

| customer_id | count |
| ----------- | ----- |
| A           | 4     |
| B           | 6     |
| C           | 2     |

"""
async def Q2():
    # for each user, we want the distinct of order data
    query = select(
        Sales.customer_id, func.count(Sales.order_date.distinct()).label('visits')
        ).group_by(Sales.customer_id)
    res = await exec(query, ['customer_id', 'number_of_visits'])
    return res
    


"""
What was the first item from the menu purchased by each customer?

-- use this if you want all the orders in the first day
-- with MostFrequent as (
--   select s.customer_id, m.product_name,
--   dense_rank() over(partition by s.customer_id order by s.order_date) rnk
--   from sales s join menu m
--   on s.product_id = m.product_id
-- ) 
-- select m.customer_id, m.product_name 
-- from MostFrequent m
-- where m.rnk = 1 

with MostFrequent as (
  select s.customer_id, m.product_name,
  row_number() over(partition by s.customer_id order by s.order_date) rnk
  from sales s join menu m
  on s.product_id = m.product_id
) 
select m.customer_id, m.product_name 
from MostFrequent m
where m.rnk = 1 

"""
def Q3():
    pass
    
    
    
"""
What is the most purchased item on the menu and how many times was it purchased by all customers?

with orderCounts as (
  select s.product_id, count(*)
  from sales s
  group by product_id
)
select m.product_name, o.count 
from orderCounts o join menu m
on o.product_id = m.product_id 
order by count desc
limit 1

"""
def Q4():
    pass
    
    
    
    
"""
Most selling item for each customer

--  1. customer, product_id counts can be written like this
 -- select s.customer_id, s.product_id, count(*) as num_count
 -- from sales s
 -- group by s.customer_id, s.product_id
 -- order by s.customer_id, s.product_id
 
 with ProductCounts as (
   select s.customer_id, s.product_id, count(*) as num_count
   from sales s
   group by s.customer_id, s.product_id
   order by s.customer_id, s.product_id
),
Ranked as (
  select p.customer_id, m.product_name, p.num_count,
  dense_rank() over (partition by p.customer_id order by p.num_count desc) rnk
  from ProductCounts p join menu m
  on p.product_id = m.product_id
  )
  select * from Ranked 
  where Ranked.rnk = 1

"""
def Q5():
    pass
    
    
    
    
"""
Which item was purchased first by the customer after they became a member?

with ordersAfterJoining as (
  select s.customer_id, s.order_date, s.product_id
  from sales s join members m
  on s.customer_id = m.customer_id
  where s.order_date > m.join_date
),

rankedOrder as (
  select *,
  row_number() over(partition by o.customer_id order by order_date)
  from ordersAfterJoining o
)

-- select * from ordersAfterJoining
select r.customer_id, r.order_date, r.product_id, m.product_name
from rankedOrder r join menu m
on r.product_id = m.product_id
where row_number = 1
order by customer_id


"""
def Q6():
    pass
    
    
    
    
    
"""
Find the total spend of each customer: When we join sales, menu, we get customer info as well, so we can do just join and sum by group by.

-- Which item was purchased just before the customer became a member?

with ordersAfterJoining as (
  select s.customer_id, s.order_date, s.product_id
  from sales s right join members m
  on s.customer_id = m.customer_id
  where s.order_date < m.join_date
),

rankedOrder as (
  select *,
  row_number() over(partition by o.customer_id order by order_date DESC) rn
  from ordersAfterJoining o
)

select r.customer_id, r.order_date, m.product_name  
from rankedOrder r join menu m
on r.product_id = m.product_id
where r.rn = 1

"""
def Q7():
    pass
    
    
    
    
    
"""
What is the total items and amount spent for each member before they became a member?

-- What is the total items and amount spent for each member before they became a member?
-- orders before joining

-- orderCounts as (
--   select o.customer_id, 
--   count(*) over(partition by o.customer_id) as order_count
--   from ordersBeforeJoining o 
-- )

with ordersBeforeJoining as (
  select s.customer_id, s.product_id, s.order_date
  from sales s join members m
  on s.customer_id = m.customer_id
  where s.order_date < m.join_date
),
orderCounts as (
  select o.customer_id, 
  count(*) as order_count
  from ordersBeforeJoining o
  group by o.customer_id
),
orderPrices as (
  select o.customer_id, sum(m.price) as total_amount_spent
  from ordersBeforeJoining o join menu m
  on o.product_id = m.product_id
  group by o.customer_id
)

select p.customer_id, order_count, total_amount_spent
from orderCounts c join orderPrices p 
on c.customer_id = p.customer_id
order by p.customer_id

-- select * from orderCounts;
-- select * from orderPrices;
-- select * from ordersBeforeJoining;

-- SIMPLE SHORTER SOLUTION

SELECT 
    s.customer_id,
    COUNT(s.product_id) AS total_qty,
    SUM(m1.price) AS total_spent
FROM 
    sales s 
JOIN 
    members m ON s.customer_id = m.customer_id
JOIN 
    menu m1 ON s.product_id = m1.product_id
WHERE 
    s.order_date < m.join_date
GROUP BY 
    s.customer_id;

"""
def Q8():
    pass
    
    
    
    
"""
If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

with menuPoints as (
  select *,
  (case
      when m.product_name = 'sushi' then m.price * 20
      else m.price * 10
  end) as points
  from menu m
)

select s.customer_id,
sum(m.price) as total_price,
sum(m.points) as total_points
from sales s join menuPoints m
on s.product_id = m.product_id
group by s.customer_id

"""
def Q9():
    pass
    
    
    
    
"""
In the first week after a customer joins the program (including their join date) they earn 2x points on all items, 
not just sushi - how many points do customer A and B have at the end of January?

-- In the first week after a customer joins the program (including their join date) they earn 2x points on all items

with PointsTable as (
    SELECT 
        s.customer_id, 
        m.product_id, 
        m.price,
        -- mem.join_date, 
        -- s.order_date,
        (
            CASE 
                WHEN s.order_date BETWEEN mem.join_date AND (mem.join_date + INTERVAL '7 days') THEN m.price * 20
                when m.product_name = 'sushi' then m.price * 20
                ELSE m.price * 10
            END
        ) AS points
    FROM 
        sales s 
    JOIN 
        menu m ON m.product_id = s.product_id
    JOIN 
        members mem ON mem.customer_id = s.customer_id
  	where s.order_date < '2021-02-01'
)

select p.customer_id, sum(p.points) as total_points
from PointsTable p
group by p.customer_id;

"""
def Q10():
    pass
    
    

    
    