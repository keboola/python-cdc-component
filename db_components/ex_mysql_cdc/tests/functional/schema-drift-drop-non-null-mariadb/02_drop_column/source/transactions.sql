use inventory;
ALTER TABLE `inventory`.`products` DROP COLUMN `name`;
INSERT INTO `inventory`.`products` (`id`, `description`, `weight`) VALUES (1001, 'Apple', 0.5);