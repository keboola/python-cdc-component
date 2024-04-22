CREATE DATABASE IF NOT EXISTS inventory;
DROP TABLE IF EXISTS inventory.sales;

CREATE TABLE inventory.sales
(
    usergender    LONGTEXT,
    usercity      LONGTEXT,
    usersentiment INT,
    zipcode       LONGTEXT,
    sku           LONGTEXT,
    createdate    VARCHAR(64) NOT NULL PRIMARY KEY,
    category      LONGTEXT,
    price         DECIMAL(12, 5),
    county        LONGTEXT,
    countycode    LONGTEXT,
    userstate     LONGTEXT,
    categorygroup LONGTEXT
);

INSERT INTO inventory.sales (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                             countycode, userstate, categorygroup)
VALUES ('Female', 'Mize', -1, '39153', 'SKU1', '2013-09-23 22:38:29', 'Cameras', 708, 'Smith', '28129', 'Mississippi',
        'Electronics'),
       ('Male', 'The Lakes', 1, '89124', 'SKU2', '2013-09-23 22:38:30', 'Televisions', 1546, 'Clark', '32003', 'Nevada',
        'Electronics'),
       ('Male', 'Baldwin', 1, '21020', 'ZD111483', '2013-09-23 22:38:31', 'Loose Stones', 1262, 'Baltimore', '24005',
        'Maryland', 'Jewelry'),
       ('Female', 'Archbald', 1, '18501', 'ZD111395', '2013-09-23 22:38:32', 'Stereo', 104, 'Lackawanna', '42069',
        'Pennsylvania', 'Electronics'),
       ('Male', 'Berea', 0, '44127', 'ZD111451', '2013-09-23 22:38:33', 'Earings', 1007, 'Cuyahoga', '39035', 'Ohio',
        'Jewelry');