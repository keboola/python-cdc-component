CREATE DATABASE IF NOT EXISTS inventory;
DROP TABLE IF EXISTS inventory.products;

CREATE TABLE IF NOT EXISTS inventory.products
(
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      DOUBLE
);

INSERT INTO inventory.products (id, name, description, weight)
VALUES
(101, 'scooter', 'Small 2-wheel scooter', 3.14),
(102, 'car battery', '12V car battery', 8.1),
(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
(104, 'hammer', '12oz carpenter\'s hammer', 0.75),
(105, 'hammer', '14oz carpenter\'s hammer', 0.875),
(106, 'hammer', '16oz carpenter\'s hammer', 1),
(107, 'rocks', 'box of assorted rocks', 5.3),
(108, 'jacket', 'water resistent black wind breaker', 0.1),
(109, 'spare tire', '24 inch spare tire', 22.2);