# importing all the required modes

from ..schema import Menu, Members, Sales
from .connector import psql_execute_single
from .postgres_utils import exec_all

from sqlalchemy import select, func, and_, case, between, text, cast

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


When rank is used: this could change

| customer_id | product_name |
| ----------- | ------------ |
| A           | curry        |
| B           | curry        |
| C           | ramen        |

When row number is used: this means that on the same date, they ordered this: 

| customer_id | product_name |
| ----------- | ------------ |
| A           | curry        |
| A           | sushi        |
| B           | curry        |
| C           | ramen        |
| C           | ramen        |

"""
async def Q3():
    
    # select the name of the product that the customer has brought
    all_order_dates = select(Sales.customer_id, Menu.product_name, Sales.order_date.label('order_date'), 
                            #  rank column
                             func.rank().over(partition_by=Sales.customer_id, order_by=Sales.order_date).label('rnk')
                             ).join(Sales, Sales.product_id == Menu.product_id).cte('all_orders_ranked')
    
    final_query = select(all_order_dates.c.customer_id, all_order_dates.c.product_name).where(all_order_dates.c.rnk == 1)
    res = await exec(final_query, cols = ['customer_id', 'first_product'])
    return res
    
    
    
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

| product_name | count |
| ------------ | ----- |
| ramen        | 8     |
| curry        | 4     |
| sushi        | 3     |



"""
async def Q4():
    
    # first group the sales based on product id and count, then join the product table to get the name
    sales_query = select(Sales.product_id, func.count(Sales.product_id).label('qty_sold')).group_by(Sales.product_id).cte('sales_combined_query')
    
    # for the top selling one, now we have count of each product
    final_query = select(Menu.product_name, sales_query.c.qty_sold).join(Menu, Menu.product_id == sales_query.c.product_id).order_by(sales_query.c.qty_sold.desc())
    res = await exec(final_query, cols = ['product_name', 'qty_sold'])
    
    # put limit if needed
    return res
    
    
    
    
    
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

| customer_id | product_name | num_count | rnk |
| ----------- | ------------ | --------- | --- |
| A           | ramen        | 3         | 1   |
| B           | curry        | 2         | 1   |
| B           | sushi        | 2         | 1   |
| B           | ramen        | 2         | 1   |
| C           | ramen        | 3         | 1   |

"""
async def Q5():
    
    product_counts_per_customer = select(Sales.customer_id, Menu.product_id, func.count(Menu.product_id).label('qty_sold')
                                         ).join(Menu, Sales.product_id == Menu.product_id
                                               ).group_by(Sales.customer_id, Menu.product_id
                                               ).cte('per_cust')

    final_query = select(product_counts_per_customer.c.customer_id, Menu.product_name, product_counts_per_customer.c.qty_sold,
                         func.rank().over(partition_by=product_counts_per_customer.c.customer_id, order_by=product_counts_per_customer.c.qty_sold.desc()).label('rnk')
                         ).join(Menu, Menu.product_id == product_counts_per_customer.c.product_id).cte('ranked')
    
    final = select(final_query.c.customer_id, final_query.c.product_name, final_query.c.qty_sold, final_query.c.rnk).where(final_query.c.rnk == 1)
    
    res = await exec(final, cols = ['customer_id', 'product_name', 'qty_sold', 'rank'])
    return res
    
    
    
    
"""
Which item was purchased first by the customer after they became a member?

with ordersAfterJoining as (
  select s.customer_id, s.order_date, s.product_id
  from sales s join members m
  on s.customer_id = m.customer_id
  where s.order_date >= m.join_date
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

| customer_id | order_date | product_id | product_name |
| ----------- | ---------- | ---------- | ------------ |
| A           | 2021-01-07 | 2          | curry        |
| B           | 2021-01-11 | 1          | sushi        |

"""
async def Q6():
    
    # first find the orders users have placed after they joined
    orders_after_joining = select(Members.customer_id, Sales.order_date, Sales.product_id
                                  ).join(Sales, Sales.customer_id == Members.customer_id
                                         ).where(Members.join_date <= Sales.order_date).cte('orders_after_joining')
    
    final_order = select(orders_after_joining.c.customer_id, orders_after_joining.c.product_id, 
                         func.rank().over(partition_by=orders_after_joining.c.customer_id, order_by=orders_after_joining.c.order_date).label('rnk')
                         ).cte('ranked')
    
    final = select(final_order.c.customer_id, Menu.product_name
                   ).join(Menu, Menu.product_id == final_order.c.product_id
                          ).where(final_order.c.rnk == 1)
    
    res = await exec(final, ['customer_id', 'product_name'])
    return res

    
    
"""
Which item was purchased just before the customer became a member?

with ordersBeforeJoining as (
  select s.customer_id, s.order_date, s.product_id
  from sales s right join members m
  on s.customer_id = m.customer_id
  where s.order_date < m.join_date
),

rankedOrder as (
  select *,
  row_number() over(partition by o.customer_id order by order_date DESC) rn
  from ordersBeforeJoining o
)

select r.customer_id, r.order_date, m.product_name  
from rankedOrder r join menu m
on r.product_id = m.product_id
where r.rn = 1


| customer_id | order_date | product_name |
| ----------- | ---------- | ------------ |
| B           | 2021-01-04 | sushi        |
| A           | 2021-01-01 | sushi        |

"""
async def Q7():
    
    orders_before_joining = select(Members.customer_id, Sales.order_date, Sales.product_id
                                  ).join(Sales, Sales.customer_id == Members.customer_id
                                         ).where(Members.join_date > Sales.order_date).cte('orders_before_joining')
                    
    # use descending rank for -> because we want the max order before the join date   
    ranked = select(orders_before_joining.c.customer_id, orders_before_joining.c.product_id, 
                    func.row_number().over(partition_by=orders_before_joining.c.customer_id, order_by=orders_before_joining.c.order_date.desc()).label('rnk')
                    ).cte('ranked')
                                  
    final = select(ranked.c.customer_id, Menu.product_name
                   ).join(Menu, Menu.product_id == ranked.c.product_id
                          ).where(ranked.c.rnk == 1)
    
    res = await exec(final, cols = ['customer_id', 'product_name'])
    return res
    
    
    
    
    
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


| customer_id | total_qty | total_spent |
| ----------- | --------- | ----------- |
| B           | 3         | 40          |
| A           | 2         | 25          |


"""
async def Q8():
    
    # doing an all join
    final = select(
        Members.customer_id, func.count(Sales.product_id).label('total_qty'), func.sum(Menu.price).label("total_spent")
    ).join(Sales, Sales.product_id == Menu.product_id
           ).join(Members, Sales.customer_id == Members.customer_id
                  ).where(Members.join_date > Sales.order_date).group_by(Members.customer_id)

    res = await exec(final, cols = ['customer_id', 'total_qty', 'total_spent'])    
    return res
    
    
    
    
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

| customer_id | total_price | total_points |
| ----------- | ----------- | ------------ |
| B           | 74          | 940          |
| C           | 36          | 360          |
| A           | 76          | 860          |

"""
async def Q9():
    
    # make a new points table for the calculation
    added_menu_points = select(
        Sales.customer_id, case(
        (Menu.product_name == 'sushi', 20 * Menu.price),
            else_= 10 * Menu.price
        ).label('points'), Menu.price
    ).join(Menu, Menu.product_id == Sales.product_id).cte('menu_points')
    
    
    # now new mnu is calculated, aggregrate the price now for each customer
    new_points_per_customer = select(
        added_menu_points.c.customer_id, func.sum(added_menu_points.c.points).label('current_points'), func.sum(added_menu_points.c.price).label('old_price')
    ).group_by(added_menu_points.c.customer_id)
    
    res = await exec(new_points_per_customer, cols = ['customer_id', 'new_points', 'old_price'])    
    return res
    
    
    
    
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
                -- when m.product_name = 'sushi' then m.price * 20
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

| customer_id | total_points |
| ----------- | ------------ |
| A           | 1270         |
| B           | 840          |

"""
async def Q10():
    # first make the points table, the above logic can be still used now as well
    added_menu_points = select(
        Sales.customer_id, 
        case(
            (and_(
                Sales.order_date.between(
                    Members.join_date, 
                    Members.join_date + text("INTERVAL '7 days'")
                )
            ), 20 * Menu.price)
        ,else_=10 * Menu.price).label('points')
        , Menu.price, Members.join_date
    ).join(Menu, Menu.product_id == Sales.product_id
           ).join(Members, Members.customer_id == Sales.customer_id)

    
    new_points_per_customer = select(
        added_menu_points.c.customer_id, func.sum(added_menu_points.c.points).label('total_points'), func.sum(added_menu_points.c.price).label('old_price')
        ).group_by(added_menu_points.c.customer_id
               ).where(added_menu_points.c.join_date < text("'2021-02-01'"))
    
    res = await exec(new_points_per_customer, cols = ['customer_id', 'new_points', 'old_price'])    
    return res
    
    
    

    
    