create table grade (name char(20),id int,score float);
insert into grade values ('Data Structure', 1, 90.5);
insert into grade values ('Data Structure', 2, 95.0);
insert into grade values ('Calculus', 2, 92.0);
insert into grade values ('Calculus', 1, 88.5);
select * from grade;
select score,name,id from grade where score > 90;
select id from grade where name = 'Data Structure';
select name from grade where id = 2 and score > 90;