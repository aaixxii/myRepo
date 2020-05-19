-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               8.0.19 - MySQL Community Server - GPL
-- Server OS:                    Win64
-- HeidiSQL Version:             11.0.0.5919
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dumping database structure for dm
CREATE DATABASE IF NOT EXISTS `dm` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `dm`;

-- Dumping structure for table dm.change_log
CREATE TABLE IF NOT EXISTS `change_log` (
  `change_log_id` bigint NOT NULL AUTO_INCREMENT,
  `segment_id` int NOT NULL,
  `request_type` int NOT NULL COMMENT '0 : New Segment, 1 : Add Template, 2 : Delete Template',
  `segment_version` bigint NOT NULL,
  `template_id` int NOT NULL,
  `template_blob` blob,
  `TS` timestamp NOT NULL,
  `p_no` int NOT NULL,
  PRIMARY KEY (`change_log_id`,`TS`,`p_no`),
  KEY `segment_id` (`segment_id`),
  KEY `operation` (`request_type`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
/*!50100 PARTITION BY LIST (`p_no`)
(PARTITION p0 VALUES IN (0) ENGINE = InnoDB,
 PARTITION p1 VALUES IN (1) ENGINE = InnoDB,
 PARTITION p2 VALUES IN (2) ENGINE = InnoDB,
 PARTITION p3 VALUES IN (3) ENGINE = InnoDB,
 PARTITION p4 VALUES IN (4) ENGINE = InnoDB,
 PARTITION p5 VALUES IN (5) ENGINE = InnoDB,
 PARTITION p6 VALUES IN (6) ENGINE = InnoDB) */;