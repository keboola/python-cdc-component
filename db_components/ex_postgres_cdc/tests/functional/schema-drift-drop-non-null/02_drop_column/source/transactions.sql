alter table inventory.products2 drop column "name";
insert into inventory.products2 (id, description, weight) values (1001, 'Apple', 0.5);