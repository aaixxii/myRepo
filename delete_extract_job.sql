delimiter //
DROP PROCEDURE IF EXISTS `delete_extract_job` //

CREATE DEFINER=`aimuser`@`%` PROCEDURE `delete_extract_job`(
IN p_extract_job_id INT(38)
)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN 
  SET @@autocommit = 0;
  DELETE
    FROM FE_JOB_QUEUE
  WHERE JOB_ID = p_extract_job_id;
END
//
delimiter ;
