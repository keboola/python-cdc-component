CREATE DATABASE IF NOT EXISTS inventory;
DROP TABLE IF EXISTS inventory.all_data_types;

CREATE TABLE inventory.all_data_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_int INT,
    sample_tinyint TINYINT,
    sample_smallint SMALLINT,
    sample_mediumint MEDIUMINT,
    sample_bigint BIGINT,
    sample_float FLOAT,
    sample_double DOUBLE,
    sample_decimal DECIMAL(10,2),
    sample_date DATE,
    sample_datetime DATETIME,
    sample_timestamp TIMESTAMP,
    sample_time TIME,
    sample_year YEAR,
    sample_char CHAR(10),
    sample_varchar VARCHAR(100),
    sample_blob BLOB,
    sample_text TEXT,
    sample_tinyblob TINYBLOB,
    sample_tinytext TINYTEXT,
    sample_mediumblob MEDIUMBLOB,
    sample_mediumtext MEDIUMTEXT,
    sample_longblob LONGBLOB,
    sample_longtext LONGTEXT,
    sample_enum ENUM('x','y','z'),
    sample_set SET('a','b','c'),
    sample_bit BIT(8),
    sample_binary BINARY(8),
    sample_varbinary VARBINARY(100),
    sample_geometry GEOMETRY,
    sample_boolean BOOLEAN,
    sample_json JSON
);


INSERT INTO inventory.all_data_types (
    sample_int,
    sample_tinyint,
    sample_smallint,
    sample_mediumint,
    sample_bigint,
    sample_float,
    sample_double,
    sample_decimal,
    sample_date,
    sample_datetime,
    sample_timestamp,
    sample_time,
    sample_year,
    sample_char,
    sample_varchar,
    sample_blob,
    sample_text,
    sample_tinyblob,
    sample_tinytext,
    sample_mediumblob,
    sample_mediumtext,
    sample_longblob,
    sample_longtext,
    sample_enum,
    sample_set,
    sample_bit,
    sample_binary,
    sample_varbinary,
    sample_geometry,
    sample_boolean,
    sample_json
) VALUES (
    1, -- sample_int
    1, -- sample_tinyint
    1, -- sample_smallint
    1, -- sample_mediumint
    1, -- sample_bigint
    1.23, -- sample_float
    1.23, -- sample_double
    123.45, -- sample_decimal
    '2022-01-01', -- sample_date
    '2022-01-01 00:00:00', -- sample_datetime
    '2022-01-01 00:00:00', -- sample_timestamp
    '00:00:00', -- sample_time
    2022, -- sample_year
    'abcdefghij', -- sample_char
    'Hello, World!', -- sample_varchar
    'Hello, World!', -- sample_blob
    'Hello, World!', -- sample_text
    'Hello', -- sample_tinyblob
    'Hello', -- sample_tinytext
    'Hello, World!', -- sample_mediumblob
    'Hello, World!', -- sample_mediumtext
    'Hello, World!', -- sample_longblob
    'Hello, World!', -- sample_longtext
    'x', -- sample_enum
    'a,b', -- sample_set
    b'10101010', -- sample_bit
    b'10101010', -- sample_binary
    b'10101010', -- sample_varbinary
    ST_GeomFromText('POINT(1 1)'), -- sample_geometry
    TRUE, -- sample_boolean
    '{"key": "value"}' -- sample_json
),
-- Repeat the above VALUES clause 9 more times
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:30:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '01:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), FALSE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:10', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
),
(
    1, 1, 1, 1, 1, 1.23, 1.23, 123.45, '2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00', '00:00:00', 2022, 'abcdefghij', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello', 'Hello', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'Hello, World!', 'x', 'a,b', b'10101010', b'10101010', b'10101010', ST_GeomFromText('POINT(1 1)'), TRUE, '{"key": "value"}'
);