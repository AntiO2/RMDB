create table grade (name char(4),id int,score float);

insert into grade values ('Data', 1, 90.5);

insert into grade values ('Data', 2, 95.0);

insert into grade values ('Calc', 2, 92.0);

insert into grade values ('Calc', 1, 88.5);

select * from grade;

select score,name,id from grade where score > 90;

select id from grade where name = 'Data';

select name from grade where id = 2 and score > 90;