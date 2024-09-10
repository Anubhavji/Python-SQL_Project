import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector

db  =  mysql.connector.connect(host = 'localhost',
                               username = 'root',
                               password = 'Anubhavrajput123890',
                               database = 'ecommerce')

cursor = db.cursor()

#  ................ queries ...................

#  calculate the moving average of order value for each customer over their order history

query = """select customer_id ,order_purchase_timestamp, payment,
avg(payment) over(partition by customer_id order by order_purchase_timestamp
rows between 2 preceding and current row) as mov_avg 
from
(select orders.customer_id, orders.order_purchase_timestamp,
payments.payment_value as payment
from payments join orders
on payments.order_id = orders.order_id) as a;"""

cursor.execute(query)
data = cursor.fetchall()
# df = pd.DataFrame(data)
# print(df)



# calculate the cumalative sales per month for each year

query = """select years , months,payment ,sum(payment)
over(order by years, months) cumulative_sales from 
(select year(orders.order_purchase_timestamp) as years,
month(orders.order_purchase_timestamp) as months,
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id
group by years,months order by years,months)as a"""
cursor.execute(query)
data = cursor.fetchall()

# df= pd.DataFrame(data)
# print(df)


#  calculate the year over year growth rate of total sales

query = """ with a as( select year(orders.order_purchase_timestamp) as years,
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id
group by years order by years)

select years,((payment-lag(payment-1) over (order by years))/lag(payment-1) over (order by years))*100 from a"""
cursor.execute(query)
data = cursor.fetchall()
# df = pd.DataFrame(data,columns=['year','yoy % growth'])
# print(df)



# calculate the retention rate of customers, defined as the percentage of customer who mame another purchase within 6 months of their first purchase

query = """with a as(select customers.customer_id,
min(orders.order_purchase_timestamp) first_order 
from customers join orders
on customers.customer_id = orders.customer_id
group by customers.customer_id),

b as(select a.customer_id,count(distinct orders.order_purchase_timestamp) next_order
from a join orders
on orders.customer_id= a.customer_id
and orders.order_purchase_timestamp > first_order and  
orders.order_purchase_timestamp < date_add(first_order,interval 6 month)  
group by a.customer_id)

select 100*(count(distinct a.customer_id)/count(distinct b.customer_id)) retention_rate
from a left join b
on a.customer_id = b.customer_id """
cursor.execute(query)
data  = cursor.fetchall()
# print(data)


# identify the top 3 customer who spent the most money in each year

query ="""select years,customer_id,payment, d_rank
from 
( select year(orders.order_purchase_timestamp) years, orders.order_id,orders.customer_id,
sum(payments.payment_value) payment ,
dense_rank() over(partition by year (orders.order_purchase_timestamp) order by sum(payments.payment_value) desc) d_rank
from orders join payments 
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp),orders.order_id,orders.customer_id) as a 
where d_rank <=3;"""

cursor.execute(query)
data  = cursor.fetchall()
df = pd.DataFrame(data,columns=["year","id","payment","rank"] )
sns.barplot(x="id",y="payment",data= df,hue="year")
plt.xticks(rotation = 90)
plt.show()