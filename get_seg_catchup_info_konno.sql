CREATE DEFINER=`s-konno`@`%` PROCEDURE `get_seg_catchup_info`(
	IN p_component_type INT,
	IN diff_tab VARCHAR(50),
    OUT tab_name VARCHAR(50)
	)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
mylable:BEGIN 
	DECLARE v_idx INT DEFAULT 999;
	DECLARE v_tmp_str VARCHAR(1024);
        DECLARE v_view_str VARCHAR(1024);
	DECLARE v_ch_log_id BIGINT(38);
        DECLARE his_count int;
	DECLARE seg_diff varchar(64);
	DECLARE v_segId BIGINT(38);
	DECLARE v_reportVer BIGINT(38);
	DECLARE v_lastVer BIGINT(38);
	DECLARE t_error integer DEFAULT 0;
	DECLARE not_found integer DEFAULT 0; 
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET not_found = 1;
    SET @@autocommit = 0;
	DROP TEMPORARY TABLE IF EXISTS history_ids;
	CREATE TEMPORARY TABLE history_ids(id BIGINT(38) PRIMARY KEY) ENGINE = InnoDB; 
    
	DROP TEMPORARY TABLE IF EXISTS catchup_info_tab;
	CREATE TEMPORARY TABLE catchup_info_tab (
      segmentId BIGINT(38),
      biometrocsId BIGINT(38),
      segVer BIGINT(38),
      changType INT4,
      bioData BLOB,
      extId  VARCHAR(36),
      eventId INT
	) ENGINE = InnoDB;  
    
          INSERT INTO history_ids(id)
            SELECT seglg.SEGMENT_CHANGE_ID
            FROM SEGMENT_CHANGE_LOG seglg, seg_diff_tab segdf             
            WHERE seglg.SEGMENT_ID = segdf.seg_id
            AND seglg.SEGMENT_VERSION <= segdf.last_ver
            AND seglg.SEGMENT_VERSION > segdf.report_ver
            ORDER BY seglg.SEGMENT_CHANGE_ID, seglg.SEGMENT_VERSION;
	
   IF his_count < 1 THEN
	  SET tab_name = '';
      LEAVE mylable;
   END IF;
	IF (p_component_type = 3) THEN
        INSERT INTO catchup_info_tab(segmentId,biometrocsId,segVer,changType,bioData,extId, eventId)
	  SELECT
        scl.SEGMENT_ID,
        scl.BIOMETRICS_ID,
        scl.SEGMENT_VERSION,
        scl.CHANGE_TYPE,
        CASE WHEN scl.CHANGE_TYPE = 1 THEN scl.TEMPLATE_DATA ELSE NULL END AS BIOMETRIC_DATA,
        scl.EXTERNAL_ID,
        NULL AS EVENT_ID
	  FROM SEGMENT_CHANGE_LOG scl
	  WHERE scl.SEGMENT_CHANGE_ID IN (SELECT id FROM  history_ids) 
	  ORDER BY scl.SEGMENT_ID, scl.SEGMENT_CHANGE_ID; 
   ELSEIF (p_component_type = 1) THEN
     INSERT INTO catchup_info_tab(segmentId,biometrocsId,segVer,changType,bioData,extId, eventId)
     SELECT
        SEGMENT_ID,
        BIOMETRICS_ID,
        SEGMENT_VERSION,
       CHANGE_TYPE,
	    NULL AS BIOMETRIC_DATA,
        NULL AS EXTERNAL_ID,
        NULL AS EVENT_ID
       FROM SEGMENT_CHANGE_LOG
       WHERE segment_change_id IN (SELECT id from history_ids)       
      ORDER BY segment_id, segment_change_id;     
  END IF;
   SET tab_name = 'catchup_info_tab';  
  DROP TEMPORARY TABLE IF EXISTS history_ids;
  DROP TEMPORARY TABLE IF EXISTS segment_diffs; 
END