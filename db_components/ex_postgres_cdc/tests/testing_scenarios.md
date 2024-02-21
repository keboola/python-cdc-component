
1. Dropping and adding column during execution => Final schema is containing all columns.

```SQL

commit

alter table inventory.customers drop column last_name;

INSERT INTO inventory.customers (first_name, email)
        VALUES ('Customer ', 'adsadasd@example.com');
commit
alter table inventory.customers add column last_name TEXT;

INSERT INTO inventory.customers (first_name, last_name, email)
        VALUES ('Customer ', 'l', 'dsd@example.com');
commit

```

2. Adding and dropping column during execution => Final schema is missing one column that appeared.


3. No state performs fullsync.

4. Binlog sync captures all events and deduplicates properly + all possible datatypes are properly converted.