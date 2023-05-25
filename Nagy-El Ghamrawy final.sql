--Question 1
--1)
select cust , price_sum ,cust_rank
from 
(select cust , price_sum , dense_rank() over(order by price_sum desc) cust_rank from
(select customer_id cust , sum(price * quantity)  over(partition by customer_id) price_sum  from tableretail ))
where cust_rank <= 10
group by cust , price_sum ,cust_rank
order by price_sum desc;

---------------------------------------------------------------------------------
--2)
-- what is the top selling product by amount
select stockcode  , sum(quantity) amount from tableretail group by stockcode order by  sum(quantity) desc  ;
-------------------------------------------------------------
-- what is the top selling product by price
select stock , price_sum ,cust_rank
from 
(select stock , price_sum , dense_rank() over(order by price_sum desc) cust_rank from
(select stockcode stock , sum(price * quantity)  over(partition by stockcode) price_sum  from tableretail ))
where cust_rank <= 10
group by stock , price_sum ,cust_rank
order by price_sum desc;
select * from tableretail where customer_id =12875;
------------------------------------------------------------------------------
--3)
select to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'Mon')  from tableretail;
select to_char(sysdate , 'Month') from dual;
SELECT *
from 
(select  invoice_date ,total_sales , invoice_month, invoice_quarter, round(sum(total_sales) over(partition by invoice_month order by  invoice_date )) MTD , round(sum(total_sales) over(partition by invoice_quarter order by  invoice_date )) QTD
from
(select  to_date(to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'mm/dd/yyyy') , 'mm/dd/yyyy') invoice_date  , sum(price * quantity) total_sales, to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'mm/yyyy') invoice_month ,
to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'Q/yyyy') invoice_quarter  from tableretail
group by  to_date(to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'mm/dd/yyyy') , 'mm/dd/yyyy')  , to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'mm/yyyy') ,  to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'Q/yyyy')
order by   to_date(to_char(to_date(invoicedate , 'mm/dd/yyyy hh24:mi') , 'mm/dd/yyyy') , 'mm/dd/yyyy') ) )
order by invoice_date ;
-----------------------------------------------------------------------------------
---4)
--how many orders per month
select invoice_month ,invoice_count
from 
( 
select distinct(invoice_month) , count(invoice) over(partition by invoice_month) invoice_count from
(select invoice,  TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'Month') invoice_month from tableretail)
order by invoice_count );


--how many order per day
select invoice_day ,invoice_count
from 
( 
select distinct(invoice_day) , count(invoice) over(partition by invoice_day) invoice_count from
(select invoice,  TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'Day') invoice_day from tableretail)
order by invoice_count );


--how many order per hour
select invoice_hour ,invoice_count
from 
( 
select distinct(invoice_hour) , count(invoice) over(partition by invoice_hour) invoice_count from
(select invoice,  TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'hh12 am') invoice_hour from tableretail)
order by invoice_count );

--------------------------------------------------------------------------------------------------------------------
--5)
select *
from
(
select distinct(invoice_month) , sum(price*quantity) over (partition by  invoice_month) total_sum from ( 
select price , quantity ,  TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'Month/yyyy') invoice_month from tableretail
)
order by total_sum desc
);

---------------------------------------------------------------------------------------------------------------------------------
--Question2)
select   customer_id , recency , frequency , monetary , r_score , fm_score , CASE
    WHEN (r_score = 5 AND fm_score IN (4, 5)) OR
         (r_score = 4 AND fm_score = 5) THEN 'Champions'
    WHEN (r_score = 5 AND fm_score = 2) OR
         (r_score = 4 AND fm_score in(2,3)) OR
         (r_score = 3 AND fm_score =3) THEN 'Potential Loyalists'
    WHEN (r_score = 5 AND fm_score = 3) OR
         (r_score = 4 AND fm_score = 4) OR
         (r_score = 3 AND fm_score in(5,4)) THEN 'Loyal Customers'
    WHEN r_score = 5 AND fm_score = 1 THEN 'Recent Customers'
    WHEN (r_score = 4 AND fm_score = 1) OR
              (r_score= 3 AND fm_score=1) THEN 'Promising'
    WHEN (r_score = 3 AND fm_score = 2) OR
         (r_score = 2 AND fm_score = 3) OR
         (r_score = 2 AND fm_score = 2) THEN 'Customers Needing Attention'
    WHEN (r_score = 2 AND fm_score IN (4, 5)) OR
         (r_score = 1 AND fm_score = 3) THEN 'At Risk'
    WHEN (r_score = 1 AND fm_score IN (4, 5)) THEN 'Cant Lose Them'
    WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'
    WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
    Else 'About to sleep'
 END AS cust_segment
from (
select customer_id , recency , frequency , monetary , ntile(5) over(order by recency desc ) r_score  ,ntile(5) over(order by frequency desc) f_score , ntile(5) over(order by monetary desc) m_score   ,NTILE(5) OVER ( ORDER BY ( (frequency+monetary)/2  )desc)  fm_score 
from (
select  distinct customer_id , round(maxdate-last_date) recency  , count(invoice) over(partition by customer_id )  frequency , sum(price) over(partition by  customer_id) as monetary 
from (
select    distinct customer_id  ,price ,  invoice  ,max(to_date(invoicedate , 'MM/DD/YYYY hh24:mi')) over() maxdate , last_value( to_date(invoicedate , 'MM/DD/YYYY hh24:mi') ) over(partition by customer_id order by to_date(invoicedate , 'MM/DD/YYYY hh24:mi') rows between unbounded preceding and unbounded following  ) last_date
from tableretail) ) )
order by customer_id
;
---------------------------------------------------------------------------------------------------------------------
--Question3)
--1)
select customer_id , max(count_consecutive_days) max_consecutive_days
from(
select  customer_id, count(date_minus) count_consecutive_days
from(
select customer_id, calendar_dt, number_of_rows, calendar_dt - number_of_rows  date_minus
from
(
select customer_id  , calendar_dt , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY Calendar_Dt)  number_of_rows
from customer_data
)
)
group by customer_id,date_minus
order by count_consecutive_days desc
)
group by customer_id
order by customer_id;
---------------------------------------------------------------------------------------------------------
--2)
select avg(no_trans_) average_no_of_transactions , avg(no_of_days)  average_no_of_days
from(
select  distinct customer_id ,first_value(no_trans) over(partition by customer_id order by calendar_dt) no_trans_ ,  first_value(calendar_dt)over(partition by customer_id order by calendar_dt) - min_date no_of_days
from(
select  customer_data.*, min(calendar_dt)over(partition by customer_id order by calendar_dt) min_date ,
sum(amt_le) over(partition by customer_id order by calendar_dt)  as consecutive_sum,rank( ) over(partition by customer_id order by calendar_dt) no_trans
from customer_data)
where consecutive_sum>=250 
order by customer_id
);

