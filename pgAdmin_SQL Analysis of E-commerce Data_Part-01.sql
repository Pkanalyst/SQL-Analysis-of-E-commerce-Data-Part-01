create table customers (customer_id text,customer_unique_id text,customer_zip_code_prefix text,customer_city text,customer_state text);

copy customers (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\customers.csv' 
WITH (FORMAT csv, HEADER true);

select * from customers;


create table geolocation (geolocation_zip_code_prefix text,geolocation_lat decimal,geolocation_lng decimal,geolocation_city text,geolocation_state text);

copy geolocation (geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\geolocation.csv' 
WITH (FORMAT csv, HEADER true);

select * from geolocation;


create table order_items (order_id text,order_item_id text,product_id text,seller_id text,shipping_limit_date date,price decimal,freight_value decimal);

copy order_items (order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\order_items.csv' 
WITH (FORMAT csv, HEADER true);

select * from order_items;


create table order_reviews (review_id text,order_id text,review_score int,review_comment_title text,review_creation_date date,review_answer_timestamp timestamp);

copy order_reviews(review_id,order_id,review_score,review_comment_title,review_creation_date,review_answer_timestamp) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\order_reviews.csv' 
WITH (FORMAT csv, HEADER true);

select * from order_reviews;


create table orders (order_id text,customer_id text,order_status text,order_purchase_timestamp timestamp,order_approved_at timestamp,order_delivered_carrier_date timestamp,order_delivered_customer_date timestamp,order_estimated_delivery_date timestamp);

copy orders(order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\orders.csv' 
WITH (FORMAT csv, HEADER true);

select * from orders;


create table payments (order_id text,payment_sequential int,payment_type text,payment_installments int,payment_value decimal);

copy payments(order_id,payment_sequential,payment_type,payment_installments,payment_value) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\payments.csv' 
WITH (FORMAT csv, HEADER true);

select * from payments;


create table products (product_id text,product_category text,product_name_length decimal,product_description_length decimal,product_photos_qty int,product_weight_g decimal,product_length_cm decimal,product_height_cm decimal,product_width_cm decimal);

copy products(product_id,product_category,product_name_length,product_description_length,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\products.csv' 
WITH (FORMAT csv, HEADER true);

select * from products;


create table sellers (seller_id text,seller_zip_code_prefix text,seller_city text,seller_state text);

copy sellers(seller_id,seller_zip_code_prefix,seller_city,seller_state) 
FROM 'F:\Datasets_New\SQL Analysis of E-commerce Data\sellers.csv' 
WITH (FORMAT csv, HEADER true);

select * from sellers;




                                   Basic Questions (Single Table) 📈

								   
•	'Customer Analysis: How many unique customers are in the customer table?'
     select count(customer_id)  from  customers;
	 
•	'Product Information: What's the average product_photos_qty for all products?''
    select avg(product_photos_qty) from products;
	
•	'Order Status: How many orders have the status 'delivered' versus 'shipped'?'
     select count(order_status) from orders
	 where order_status='delivered';
	 
•	'Payment Type: What are the different payment_type values, and how many times was each used?'
     select payment_type as Paymen_Type,count(payment_type) from payments
	 group by Payment_Type;
	 
•	'Review Scores: What's the average review_score for all orders?''
     select avg(review_score) from order_reviews;     
________________________________________
                                    
									Intermediate Questions (Multiple Tables) 🔄
									
•	'Top Sellers: Who are the top 10 sellers based on the total value of their sales? '
     'You'll need to join the sellers, order_items, and orders tables.''

	 select S.seller_id,sum(OI.price) as Total_sales from sellers as S inner join order_items as OI
	 on S.seller_id = OI.seller_id
	 group by S.seller_id
	 order by Total_sales desc
	 limit 10;

	 select S.seller_id,sum(OI.price) as Total_sales from sellers as S left join order_items as OI
	 on S.seller_id = OI.seller_id
	 group by S.seller_id
	 order by Total_sales desc
	 limit 10;
	 
	 
•	''Revenue by State: What's the total revenue generated from each customer state? '
    'This requires joining customer, orders, and order_items.'

	select C.customer_state as Customer_State,sum(OI.price) from customers as C inner join orders as O
	on C.customer_id = O.customer_id inner join order_items as OI
	on O.order_id = OI.order_id
	group by Customer_State;

	select C.customer_state as Customer_State,sum(OI.price) from customers as C left join orders as O
	on C.customer_id = O.customer_id left join order_items as OI
	on O.order_id = OI.order_id
	group by Customer_State;

	
•	'Review-Sales Correlation: Do products with a higher average review_score also have higher sales volume?'
    'You can analyze this by joining products, order_items, and order_reviews.'
	
 with Total_revenue AS (select P.product_id,sum(OI.price) as Total_Revenue
      from products as P inner join order_items as OI
   on P.product_id = OI.product_id
   group by P.product_id),
 
   Avg_reviews AS (select P.product_id,avg(Orw.review_score) as Avg_rating
          from products as P inner join order_items as OI
   on P.product_id = OI.product_id inner join order_reviews as Orw
   on OI.order_id = Orw.order_id
   group by P.product_id)

   select TR.product_id,AR.Avg_rating,TR.Total_Revenue,
          dense_rank() over(order by TR.Total_Revenue desc ) as Revenue_rank
          from Total_Revenue as TR inner join Avg_reviews as AR
   on TR.product_id = AR.product_id 
   group by TR.product_id,Avg_rating,TR.Total_Revenue
   order by Avg_rating desc;
   
•	'Geographic Analysis: How many orders were made from a different state than the seller's state?'
    'This involves joining customer, orders, and sellers.''

	select count(O.order_id) from sellers as S inner join order_items as OI
	on S.seller_id = OI.seller_id inner join orders as O 
	on OI.order_id = O.order_id inner join customers as C
	on O.customer_id=C.customer_id
	where C.customer_state <> S.seller_state;
   
•   'Payment Installment Trends: What's the average payment_installments for orders with a value greater than $500?' 
    'Join orders and payment.''

	select O.order_id,avg(P.payment_installments) from orders as O inner join payments as P
	on O.order_id = P.order_id
	where P.Payment_value > 500
	group by O.order_id;
________________________________________

                           Advanced Questions (Complex Queries & Business Insights) 🧠
					
•   'Customer Lifetime Value (CLV): Identify the top 20% of customers (by total spend) and '
    'calculate their average review_score. '
    'This requires a subquery or a Common Table Expression (CTE) to first identify the high-value customers.'

(Approach: 1)

	 select C.customer_id,sum(OI.price) as total_spending,avg(review_score) as avg_review_score 
	      from customers as C inner join orders as O
	 on C.customer_id = O.customer_id inner join order_items as OI
	 on O.order_id = OI.order_id inner join order_reviews as Orw
	 on O.Order_id = Orw.order_id
	 group by C.customer_id
	 order by total_spending desc
	 limit (select count(customer_id)/5 from customers);

(Approach: 2)

	 with total_spending as (select C.customer_id as Customer_id,sum(OI.price) as total_spending 
	      from customers as C inner join orders as O
	 on C.customer_id = O.customer_id inner join order_items as OI
	 on O.order_id = OI.order_id 
	 group by C.customer_id),

	 avg_reviews as (select C.customer_id as customer_id,avg(review_score) as avg_review_score 
	      from customers as C inner join orders as O
	 on C.customer_id = O.customer_id inner join order_items as OI
	 on O.order_id = OI.order_id inner join order_reviews as Orw
	 on O.Order_id = Orw.order_id
	 group by C.customer_id)
	 

	 select TS.customer_id,TS.total_spending ,AR.avg_review_score
	      from total_spending as TS inner join avg_reviews as AR
	 on TS.customer_id = AR.customer_id 
	 group by TS.customer_id,TS.total_spending,AR.avg_review_score
	 order by total_spending desc
	 limit (select count(customer_id)/5 from customers);
	
•	'Delivery Performance Metrics: Calculate the average delivery time' 
    '(the difference between order_delivered_customer_date and order_purchase_timestamp) and' 
    'compare it to the order_estimated_delivery_date. Identify the percentage of orders that were late.'

	select avg(extract(epoch from(order_delivered_customer_date - order_purchase_timestamp)/3600)/24) 
	 as average_delivery_time  from orders;
	 
select (extract(epoch from(order_delivered_customer_date - order_purchase_timestamp)/3600)/24) 
	  - (extract(epoch from(order_estimated_delivery_date - order_purchase_timestamp)/3600)/24) as diff_btw_deliv_estim
	 from orders;

'(Final Solution:)'
	 select count(diff_btw_deliv_estim) Total_late_orders from
	      (select extract(epoch from(order_delivered_customer_date - order_purchase_timestamp)/3600)/24
-(extract(epoch from(order_estimated_delivery_date - order_purchase_timestamp)/3600)/24) as diff_btw_deliv_estim
	 from orders)
	 where diff_btw_deliv_estim>0 ;
	 
•	'Product Performance by Category: Find the product category with the highest average freight_value per item,'
    'and the category with the lowest average review_score. This will require complex joins and aggregations.'

	(select P.product_category,avg(OI.freight_value) as average_freight_value from products as P inner join order_items as OI 
	on P.product_id = OI.product_id
	group by P.product_category
	order by average_freight_value desc
	limit 1)
	
union all

	(select P.product_category,avg(OI.freight_value) as average_freight_value from products as P inner join order_items as OI 
	on P.product_id = OI.product_id
	group by P.product_category
	order by average_freight_value asc
	limit 1);