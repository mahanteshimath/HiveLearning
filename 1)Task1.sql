
cloudera



1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

2. Store raw data into hdfs location

3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table


CREATE DATABASE monty;


CREATE TABLE IF NOT EXISTS  monty.sales_order_csv
(ordernumber   int,
quantityordered   int,
priceeach   float,
orderlinenumber   int,
sales   int,
status   string,
qtr_id   int,
month_id   int,
year_id   int,
productline   string,
msrp   string,
productcode   string,
phone   string,
city   string,
state   string,
postalcode   string,
country   string,
territory   string,
contactlastname   string,
contactfirstname   string,
dealsize   string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");



4. Load data from hdfs path into "sales_order_csv" 
load data local inpath '/hiveclass/sales_order_data.csv' into table monty.sales_order_csv; 

5. Create an internal hive table which will store data in ORC format "sales_order_orc"

CREATE TABLE IF NOT EXISTS  monty.sales_order_orc
(ordernumber   int,
quantityordered   int,
priceeach   float,
orderlinenumber   int,
sales   int,
status   string,
qtr_id   int,
month_id   int,
year_id   int,
productline   string,
msrp   string,
productcode   string,
phone   string,
city   string,
state   string,
postalcode   string,
country   string,
territory   string,
contactlastname   string,
contactfirstname   string,
dealsize   string) STORED AS ORC;


6. Load data from "sales_order_csv" into "sales_order_orc"
Insert into table monty.sales_order_orc select * from monty.sales_order_csv;


Perform below menioned queries on "sales_order_orc" table :

a. Calculatye total sales per year

select YEAR_ID, sum(SALES) TOTAL_SALES from monty.sales_order_orc
group  by YEAR_ID;

b. Find a product for which maximum orders were placed

SELECT productline
FROM
 (SELECT productline,
         rank() over (order by cast(quantityordered as int) desc) as r 
  FROM monty.sales_order_orc) S 
WHERE S.r = 1;


SELECT a.productline,a.quantityordered FROM monty.sales_order_orc a left semi join  
(SELECT MAX(quantityordered) 
max_o FROM monty.sales_order_orc) b on (a.quantityordered=b.max_o);


c. Calculate the total sales for each quarter


select qtr_id, MIN(SALES) TOTAL_SALES from monty.sales_order_orc
group  by qtr_id;


d. In which quarter sales was minimum

SET mapreduce.job.reduces=3;

SELECT qtr_id, TOTAL_SALES FROM (
select qtr_id, sum(SALES) TOTAL_SALES from monty.sales_order_orc
group  by qtr_id
) M
SORT BY TOTAL_SALES asc
 LIMIT 1;


e. In which country sales was maximum and in which country sales was minimum

---Note: min and max sales asked in question not total sales
select country,max(SALES) max_sales from monty.sales_order_orc
group by country;


select country,min(SALES) min_sales from monty.sales_order_orc
group by country;

---If question asked total min and max sales country wise
select max_s.country, max_s.max_sales, min_s.min_sales from 
(select country,sum(SALES) max_sales from monty.sales_order_orc) max_s
inner join (select country,sum(SALES) min_sales from monty.sales_order_orc) min_s
on (max_s.country=min_s.country);

f. Calculate quartelry sales for each city


select city,qtr_id, sum(SALES) TOTAL_SALES from monty.sales_order_orc
group  by city,qtr_id;


h. Find a month for each year in which maximum number of quantities were sold

select distinct a.year_id,a.month_id,a.quantityordered from monty.sales_order_orc a
inner join
(select  year_id, month_id, max(quantityordered) as quantityordered  from monty.sales_order_orc group  by year_id,month_id) b  
on (a.year_id=b.year_id and a.month_id=b.month_id and a.quantityordered=b.quantityordered);




select year_id, month_id, quantityordered from (
select year_id, month_id, quantityordered ,rank() over (partition by year_id, month_id order by cast(quantityordered as int) desc) r from
( 
SELECT year_id, month_id, sum(quantityordered)  quantityordered from  monty.sales_order_orc 
group by  year_id,month_id   )  s) s
WHERE s.r = 1;
