
INSERT INTO inventory.sales (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county, countycode, userstate, categorygroup)
VALUES ('Male', 'New York', 1, '10001', 'SKU10', '2023-01-01', 'Electronics', 199.99, 'New York', 'NY', 'NY', 'Electronics'),
      ('Female', 'Los Angeles', 5, '90001', 'SKU20', '2023-01-02', 'Books', 14.99, 'Los Angeles', 'CA', 'CA', 'Books');

UPDATE inventory.sales
SET price = 249.99
WHERE sku = 'SKU1';

DELETE FROM inventory.sales
WHERE sku = 'SKU2';