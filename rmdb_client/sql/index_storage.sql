create table warehouse (w_id int, name char(8));

insert into warehouse values (10 , 'qweruiop');

insert into warehouse values (534, 'asdfhjkl');

insert into warehouse values (100,'qwerghjk');

insert into warehouse values (500,'bgtyhnmj');

create index warehouse(w_id);

select * from warehouse where w_id = 10;

select * from warehouse where w_id < 534 and w_id > 100;

drop index warehouse(w_id);

create index warehouse(name);

select * from warehouse where name = 'qweruiop';

select * from warehouse where name > 'qwerghjk';

select * from warehouse where name > 'aszdefgh' and name < 'qweraaaa';

drop index warehouse(name);

create index warehouse(w_id,name);

select * from warehouse where w_id = 100 and name = 'qwerghjk';

select * from warehouse where w_id < 600 and name > 'bztyhnmj';