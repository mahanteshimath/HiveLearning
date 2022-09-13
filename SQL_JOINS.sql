


create table monty.customers(id int, name string, age string, address string, salary int);

insert into table monty.customers 
select 1,'Mahantesh' , 32, 'Blr',2000 union all 
select 2,'Shashank' , 32, 'Blr',5000 union all 
select 3,'Rahul' , 32, 'Blr',6000 union all 
select 4,'Sudhanshu' , 32, 'Blr',7000 union all 
select 5,'Krish' , 32, 'Blr',8000;



create table monty.order( oid int, date date, customer_id int, amount int);
insert into table monty.order 
select 100, cast('2022-01-02' as date), 1,3000 union all 
select 200, cast('2022-02-02' as date), 2,6000 union all 
select 300, cast('2022-03-02' as date), 3,7000 union all 
select 400, cast('2022-04-02' as date), 4,8000 union all 
select 500, cast('2022-05-02' as date), 5,9000;

insert into table monty.order 
select 100, cast('2022-09-02' as date), 6,3000;


select * from monty.customers;--- 5 row

select * from monty.order;--6 rows

Inner JOIN,

select c.*,o.*  from monty.customers c
inner join monty.order o
on (o.customer_id=c.id); 

LEFT OUTER JOIN ,
select c.*,o.*  from monty.customers c
left join monty.order o
on (o.customer_id=c.id); 


RIGHT OUTER JOIN ,
select c.*,o.*  from monty.customers c
right outer join monty.order o
on (o.customer_id=c.id);

FULL OUTER JOIN

select c.*,o.*  from monty.customers c
full outer join monty.order o
on (o.customer_id=c.id);