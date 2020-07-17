CREATE DEFINER=`aimuser`@`%` PROCEDURE `get_job_info_for_create_plans`(
 in limited_job_ids varchar(2048),
 out tab_name VARCHAR(50))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
  DECLARE t_error integer DEFAULT 0;
  DECLARE v_idx int DEFAULT 999;
  DECLARE v_tmp_str varchar(20);
  DECLARE v_id int;
  DECLARE v_result int;  
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
  DROP TEMPORARY TABLE IF EXISTS tab_get_job_info;
  CREATE TEMPORARY TABLE tab_get_job_info(job_id  bigint(38), family_id int, function_id int, container_id int, container_job_id bigint(38), priority int, failure_count int) engine=InnoDB;  
  DROP TEMPORARY TABLE IF EXISTS limited_ids_tab;
  CREATE TEMPORARY TABLE limited_ids_tab (
    id int
  ) ENGINE = InnoDB;
  CALL strToTable(limited_job_ids, 'limited_ids_tab', @err);
  INSERT INTO tab_get_job_info
  SELECT
    jq.JOB_ID,
    jq.FAMILY_ID,
    fq.FUNCTION_ID,
    ssj.CONTAINER_ID,
    ssj.CONTAINER_JOB_ID,
    jq.PRIORITY,
    jq.FAILURE_COUNT
  FROM JOB_QUEUE jq,
       FUSION_JOBS fq,
       CONTAINER_JOBS ssj
  WHERE jq.JOB_ID = fq.JOB_ID
  AND fq.FUSION_JOB_ID = ssj.FUSION_JOB_ID
  AND jq.JOB_STATE = 0
  AND jq.JOB_ID IN (SELECT
      id
    FROM limited_ids_tab)
  ORDER BY PRIORITY, FAILURE_COUNT DESC, JOB_ID;    
  IF t_error = 1 THEN
    set tab_name = '';
   ELSE     
     set tab_name =  'tab_get_job_info';
  END IF;
END