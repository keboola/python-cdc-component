INSERT INTO inventory.all_data_types (
    col_smallint, col_integer, col_int, col_bigint, col_decimal, col_numeric, col_real, col_double_precision,
    col_smallserial, col_serial, col_bigserial, col_money, col_character, col_char, col_character_varying,
    col_varchar, col_text, col_bytea, col_timestamp, col_timestamp_with_time_zone, col_date, col_time,
    col_time_with_time_zone, col_interval, col_boolean, col_point, col_line, col_lseg, col_box, col_path,
    col_polygon, col_circle, col_cidr, col_inet, col_macaddr, col_macaddr8, col_bit, col_bit_varying,
    col_tsvector, col_tsquery, col_uuid, col_xml, col_json, col_jsonb, col_integer_array,  col_int4range
) VALUES (
    3, 4, 5, 6, 7.00, 8.00, 9.00, 10.00,
    11, 12, 13, 14.00, 'c', 'd', 'e',
    'f', 'g', E'\\xDEADBEEF', '2006-12-21 12:25:56', '2006-12-21 12:25:56+04', '2006-12-21', '12:25:56',
    '12:25:56+04', '3 years 4 months 5 days 6 hours 7 minutes 8 seconds', true, '(3,3)', '{3,4,5}',
    '((3,4),(5,6))', '((3,4),(5,6))', '((3,4),(5,6),(7,8))', '((3,4),(5,6),(7,8),(9,10))', '<(3,4),5>',
    '192.168.3.0/24', '192.168.3.3', '08:00:2b:03:04:05', '08:00:2b:03:04:05:06:07', B'1110000000', B'1110000000',
    'a big mouse', 'a & !d', '123e4567-e89b-12d3-a456-426614174000', '<baz>qux</baz>',
    '{"key2": "value2"}', '{"key2": "value2"}', ARRAY[3,4,5],  int4range(3,12)
);

UPDATE inventory.all_data_types
SET col_smallint = 0
WHERE id = '2';
