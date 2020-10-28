CREATE TABLE `word_count` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`text` VARCHAR(1024) NOT NULL DEFAULT '',
	PRIMARY KEY (`id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=1
;

CREATE TABLE `word_count_sum` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`word` VARCHAR(1024) NOT NULL DEFAULT '',
	`frequency` INT(11) NOT NULL DEFAULT '1',
	`create_time` DATETIME NULL DEFAULT NULL,
	PRIMARY KEY (`id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=1
;

INSERT INTO `word_count` (`text`) VALUES ('asd qwer');
INSERT INTO `word_count` (`text`) VALUES ('qwer zxcv bnm');
INSERT INTO `word_count` (`text`) VALUES ('asd asd');