
DROP TABLE IF EXISTS inventory.all_data_types;

CREATE TABLE inventory.all_data_types (
    id SERIAL PRIMARY KEY,
    -- Numeric types
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_int INT,
    col_bigint BIGINT,
    col_decimal DECIMAL(5,2),
    col_numeric NUMERIC(5,2),
    col_real REAL,
    col_double_precision DOUBLE PRECISION,
    col_smallserial SMALLSERIAL,
    col_serial SERIAL,
    col_bigserial BIGSERIAL,

    -- Monetary types
    col_money MONEY,

    -- Character types
    col_character CHARACTER(10),
    col_char CHAR(10),
    col_character_varying CHARACTER VARYING(10),
    col_varchar VARCHAR(10),
    col_text TEXT,

    -- Binary data types
    col_bytea BYTEA,

    -- Date/Time types
    col_timestamp TIMESTAMP,
    col_timestamp_with_time_zone TIMESTAMP WITH TIME ZONE,
    col_date DATE,
    col_time TIME,
    col_time_with_time_zone TIME WITH TIME ZONE,
    col_interval INTERVAL,

    -- Boolean type
    col_boolean BOOLEAN,

    -- Geometric types
    col_point POINT,
    col_line LINE,
    col_lseg LSEG,
    col_box BOX,
    col_path PATH,
    col_polygon POLYGON,
    col_circle CIRCLE,

    -- Network address types
    col_cidr CIDR,
    col_inet INET,
    col_macaddr MACADDR,
    col_macaddr8 MACADDR8,

    -- Bit string types
    col_bit BIT(10),
    col_bit_varying BIT VARYING(10),

    -- Text search type
    col_tsvector TSVECTOR,
    col_tsquery TSQUERY,

    -- UUID type
    col_uuid UUID,

    -- XML type
    col_xml XML,

    -- JSON types
    col_json JSON,
    col_jsonb JSONB,

    -- Array type
    col_integer_array INTEGER[],

    -- Range type
    col_int4range INT4RANGE
--
--     -- Domain types
--     col_domain DOMAIN,

);

INSERT INTO inventory.all_data_types (
    col_smallint, col_integer, col_int, col_bigint, col_decimal, col_numeric, col_real, col_double_precision,
    col_smallserial, col_serial, col_bigserial, col_money, col_character, col_char, col_character_varying,
    col_varchar, col_text, col_bytea, col_timestamp, col_timestamp_with_time_zone, col_date, col_time,
    col_time_with_time_zone, col_interval, col_boolean, col_point, col_line, col_lseg, col_box, col_path,
    col_polygon, col_circle, col_cidr, col_inet, col_macaddr, col_macaddr8, col_bit, col_bit_varying,
    col_tsvector, col_tsquery, col_uuid, col_xml, col_json, col_jsonb, col_integer_array,  col_int4range
) VALUES (
    1, 2, 3, 4, 5.00, 6.00, 7.00, 8.00,
    9, 10, 11, 12.00, 'a', 'b', 'c',
    'd', 'e', E'\\xDEADBEEF', '2004-10-19 10:23:54', '2004-10-19 10:23:54+02', '2004-10-19', '10:23:54',
    '10:23:54+02', '1 year 2 months 3 days 4 hours 5 minutes 6 seconds', true, '(1,1)', '{1,2,3}',
    '((1,2),(3,4))', '((1,2),(3,4))', '((1,2),(3,4),(5,6))', '((1,2),(3,4),(5,6),(7,8))', '<(1,2),3>',
    '192.168.1.0/24', '192.168.1.1', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',  B'1010000000', B'1010000000',
    'a fat cat', 'a & !b', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '<foo>bar</foo>',
    '{"key": "value"}', '{"key": "value"}', ARRAY[1,2,3],  int4range(1,10)),
          (
    2, 3, 4, 5, 6.00, 7.00, 8.00, 9.00,
    10, 11, 12, 13.00, 'b', 'c', 'd',
    'e', 'f', E'\\xDEADBEEF', '2005-11-20 11:24:55', '2005-11-20 11:24:55+03', '2005-11-20', '11:24:55',
    '11:24:55+03', '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', false, '(2,2)', '{2,3,4}',
    '((2,3),(4,5))', '((2,3),(4,5))', '((2,3),(4,5),(6,7))', '((2,3),(4,5),(6,7),(8,9))', '<(2,3),4>',
    '192.168.2.0/24', '192.168.2.2', '08:00:2b:02:03:04', '08:00:2b:02:03:04:05:06', B'1100000000', B'1100000000',
    'a thin dog', 'a & !c', 'b1ffbc99-9c0b-4ef8-bb6d-6bb9bd380a12', '<bar>foo</bar>',
    '{"key1": "value1"}', '{"key1": "value1"}', ARRAY[2,3,4], int4range(2,11)
);