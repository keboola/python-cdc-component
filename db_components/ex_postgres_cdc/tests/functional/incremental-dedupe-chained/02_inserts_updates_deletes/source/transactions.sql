-- Insert 3 sample records
INSERT INTO inventory.sales (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county, countycode, userstate, categorygroup)
VALUES ('Male', 'New York', 1, '10001', 'SKU1', '2023-01-01', 'Electronics', 199.99, 'New York', 'NY', 'NY', 'Electronics'),
       ('Female', 'Los Angeles', 5, '90001', 'SKU2', '2023-01-02', 'Books', 14.99, 'Los Angeles', 'CA', 'CA', 'Books'),
       ('Female', 'Chicago', 3, '60007', 'SKU3', '2023-01-03', 'Clothing', 49.99, 'Cook', 'IL', 'IL', 'Fashion');

-- Update each of the 3 records three times
UPDATE inventory.sales
SET price = 249.99
WHERE sku = 'SKU1';

UPDATE inventory.sales
SET price = 299.99
WHERE sku = 'SKU1';

UPDATE inventory.sales
SET price = 349.99
WHERE sku = 'SKU1';

UPDATE inventory.sales
SET price = 19.99
WHERE sku = 'SKU2';

UPDATE inventory.sales
SET price = 24.99
WHERE sku = 'SKU2';

UPDATE inventory.sales
SET price = 29.99
WHERE sku = 'SKU2';

UPDATE inventory.sales
SET price = 59.99
WHERE sku = 'SKU3';

UPDATE inventory.sales
SET price = 69.99
WHERE sku = 'SKU3';

UPDATE inventory.sales
SET price = 79.99
WHERE sku = 'SKU3';

-- Delete one of the records
DELETE FROM inventory.sales
WHERE sku = 'SKU3';