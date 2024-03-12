-- Ensure we're working in a specific schema to avoid conflicts
SET search_path TO benchmark;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Drop tables if they already exist (for rerun-ability)
DROP TABLE IF EXISTS small_table, medium_table, large_table CASCADE;

-- Create a small table
CREATE TABLE small_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Populate the small table with some data
INSERT INTO small_table (name, created_at)
SELECT
    md5(random()::text),
    NOW() - (interval '1 day' * round(random() * 30))
FROM generate_series(1, 100) s; -- 100 rows

-- Create a medium-sized table
CREATE TABLE medium_table (
    id SERIAL PRIMARY KEY,
    description TEXT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Populate the medium table with more substantial data
INSERT INTO medium_table (description, amount, created_at)
SELECT
    md5(random()::text),
    round((random() * 10000)::numeric, 2),
    NOW() - (interval '1 day' * round(random() * 365))
FROM generate_series(1, 10000) s; -- 10,000 rows

-- Create a wider large table
CREATE TABLE large_table (
    id SERIAL PRIMARY KEY,
    text_data TEXT,
    numeric_data DECIMAL(12, 4),
    integer_data INTEGER,
    boolean_data BOOLEAN,
    date_data DATE,
    time_data TIME,
    timestamp_data TIMESTAMP NOT NULL,
    char_data CHAR(10),
    varchar_data VARCHAR(255),
    float_data FLOAT,
    json_data JSONB,
    array_data TEXT[],
    uuid_data UUID,
    event_time TIMESTAMP NOT NULL DEFAULT NOW(),
    data JSONB,
    tags TEXT[]
);

-- Populate the wider large table with a large amount of data
INSERT INTO large_table (
    text_data, numeric_data, integer_data, boolean_data, date_data, time_data, timestamp_data,
    char_data, varchar_data, float_data, json_data, array_data, uuid_data, event_time, data, tags
)
SELECT
    md5(random()::text),
    round((random() * 10000)::numeric, 2),
    (random() * 10000)::int,
    random() > 0.5,
    NOW()::DATE - (interval '1 day' * round(random() * 365)),
    NOW()::TIME - (interval '1 min' * round(random() * 1440)),
    NOW() - (interval '1 min' * round(random() * 525600)),
    left(md5(random()::text), 10),
    md5(random()::text),
    random() * 10000,
    jsonb_build_object('key', md5(random()::text), 'value', s),
    ARRAY[md5(random()::text), md5(random()::text)],
    uuid_generate_v4(),
    NOW() - (interval '1 min' * round(random() * 525600)),
    jsonb_build_object('key', md5(random()::text), 'value', s),
    ARRAY[md5(random()::text), md5(random()::text)]
FROM generate_series(1, 5000000) s; -- 5 million rows

-- Index creation for optimizing queries on the wider large table
CREATE INDEX ON large_table (event_time);
CREATE INDEX ON large_table USING gin (tags);
CREATE INDEX ON large_table USING gin (data);
CREATE INDEX ON large_table (uuid_data);

select count(*) from large_table;
select * from large_table limit 10;
CREATE TABLE public.debezium_signals (
   "id" varchar(42) NOT NULL PRIMARY KEY,
   "type" varchar(32) NOT NULL,
   "data" varchar(2048) NULL
);

# generate additional benchmarking tables


DO $$
DECLARE
    counter INTEGER := 0;
BEGIN
    WHILE counter < 150 LOOP
        -- Create a table with 10 columns of different types
        EXECUTE format('CREATE TABLE sample_table_%s (
            id SERIAL PRIMARY KEY,
            data1 TEXT,
            data2 INTEGER,
            data3 DECIMAL(10,2),
            data4 TIMESTAMP,
            data5 DATE,
            data6 FLOAT,
            data7 BOOLEAN,
            data8 UUID,
            data9 BYTEA
        );', counter);

        -- Insert 10,000 records into the table
        EXECUTE format('INSERT INTO sample_table_%s (data1, data2, data3, data4, data5, data6, data7, data8, data9)
        SELECT md5(random()::text), (random()*10000)::INTEGER, round(random()::numeric, 2),
        NOW(), NOW()::DATE, random(), random() > 0.5, uuid_generate_v4(), E\'\\\\xDEADBEEF\'
        FROM generate_series(1, 10000);', counter);

        counter := counter + 1;
    END LOOP;
END $$;

