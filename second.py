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

#  List all unique cities where Customers are located.
query = """select distinct customer_city from customers"""

cursor.execute(query)
data = cursor.fetchall()




#  count the number or orders place in 2017
query = """select count(order_id) from orders where year(order_purchase_timestamp) = 2017"""

cursor.execute(query)
data = cursor.fetchall()


#  find the total sales per category

query = """select products.product_category category,round(sum(payments.payment_value),2) sales
from products join order_items on products.product_id =order_items.product_id join payments
on payments.order_id = order_items.order_id
group by category"""

cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns=["category","sales"])


# calculate the percentage of orders that were paid in installments

query = """select sum(case when payment_installments >1 then 1 else 0 end )/count(*)*100 from payments"""

cursor.execute(query)
data = cursor.fetchall()



#  count the number of customers from each state

query = """select customer_state ,count(customer_id) from customers group by customer_state"""

cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns=["state","customer_count"])
df = df.sort_values(by="customer_count",ascending=True)
plt.bar(df["state"],df["customer_count"])
plt.xticks(rotation=90)

plt.show()


