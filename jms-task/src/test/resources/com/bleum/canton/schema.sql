DROP TABLE IF EXISTS `integration_test`.`jms_b_task`;
CREATE TABLE  `integration_test`.`jms_b_task` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `content_id` varchar(45) DEFAULT NULL,
  `type` int(10) unsigned NOT NULL,
  `status` int(10) unsigned NOT NULL,
  `created_time` datetime NOT NULL,
  `operated_time` datetime DEFAULT NULL,
  `message` longtext,
  `retry` int(10) unsigned DEFAULT NULL,
  `last_error` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;