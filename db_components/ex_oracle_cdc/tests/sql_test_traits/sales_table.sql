CREATE TABLE TESTUSER01.SALES
(
    usergender    VARCHAR2(4000),
    usercity      VARCHAR2(4000),
    usersentiment NUMBER,
    zipcode       VARCHAR2(4000),
    sku           VARCHAR2(4000),
    createdate    VARCHAR2(64) NOT NULL PRIMARY KEY,
    category      VARCHAR2(4000),
    price         NUMBER(12, 5),
    county        VARCHAR2(4000),
    countycode    VARCHAR2(4000),
    userstate     VARCHAR2(4000),
    categorygroup VARCHAR2(4000)
);

INSERT INTO TESTUSER01.SALES (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                   countycode, userstate, categorygroup)
VALUES ('Female', 'Mize', -1, '39153', 'SKU1', '2013-09-23 22:38:29', 'Cameras', 708, 'Smith', '28129', 'Mississippi',
        'Electronics');

INSERT INTO TESTUSER01.SALES (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                   countycode, userstate, categorygroup)
VALUES ('Male', 'The Lakes', 1, '89124', 'SKU2', '2013-09-23 22:38:30', 'Televisions', 1546, 'Clark', '32003', 'Nevada',
        'Electronics');

INSERT INTO TESTUSER01.SALES (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                   countycode, userstate, categorygroup)
VALUES ('Male', 'Baldwin', 1, '21020', 'ZD111483', '2013-09-23 22:38:31', 'Loose Stones', 1262, 'Baltimore', '24005',
        'Maryland', 'Jewelry');

INSERT INTO TESTUSER01.SALES (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                   countycode, userstate, categorygroup)
VALUES ('Female', 'Archbald', 1, '18501', 'ZD111395', '2013-09-23 22:38:32', 'Stereo', 104, 'Lackawanna', '42069',
        'Pennsylvania', 'Electronics');

INSERT INTO TESTUSER01.SALES (usergender, usercity, usersentiment, zipcode, sku, createdate, category, price, county,
                   countycode, userstate, categorygroup)
VALUES ('Male', 'Berea', 0, '44127', 'ZD111451', '2013-09-23 22:38:33', 'Earings', 1007, 'Cuyahoga', '39035', 'Ohio',
        'Jewelry');

COMMIT;
