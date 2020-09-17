CREATE DEFINER=`aimuser`@`%` PROCEDURE `add_biometrics`(IN p_external_id varchar(64),
IN p_container_id int,
IN p_biometric_data mediumblob,
IN p_no int,
OUT p_biometric_id bigint(38),
OUT p_seg_id bigint(38),
OUT p_seg_version bigint(38),
OUT p_new_seg_created boolean)
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
lab_begin:
  BEGIN
    DECLARE r_update_bio_id int;
    DECLARE last_bio_id bigint(38);
    DECLARE l_seg_created boolean;
    DECLARE r_seg_id bigint(38);
    DECLARE r_seg_version bigint(38);	
    DECLARE r_biometric_id bigint(38);
    DECLARE l_count bigint(38);
    DECLARE l_biometrics_id bigint(38);
    DECLARE l_data_len bigint(38);
    DECLARE tmp_container_id int(38);
    DECLARE tmp_max_seg_record_count int;
    DECLARE tmp_max_segment_size bigint(38);
    DECLARE tmp_one_templte_size bigint(38);
    DECLARE seg_semgnet_id bigint(38);
    DECLARE seg_binary_length bigint(38);
    DECLARE seg_record_count bigint(38);
    DECLARE seg_version bigint(38);
    DECLARE seg_revision bigint(38);
    DECLARE seg_bio_id_start bigint(38);
    DECLARE seg_bio_id_end bigint(38);
    DECLARE t_error integer DEFAULT 0;
    DECLARE not_found integer DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET not_found = 1;
    SET @@autocommit = 0;
    SELECT
      MAX_RECORD_COUNT,
      MAX_SEGMENT_SIZE INTO tmp_max_seg_record_count, tmp_max_segment_size
    FROM CONTAINERS
    WHERE CONTAINER_ID = p_container_id;
    SELECT
      KEY_VALUE INTO tmp_one_templte_size
    FROM SYSTEM_INIT
    WHERE KEY_NAME = 'ONE_TEMPLATET_BYTE_SIZE';
    SELECT
      COUNT(VACANCIES_ID) INTO l_count
    FROM SEGMENT_VACANCIES FOR UPDATE;
    IF l_count > 0 THEN
      SELECT SEGMENT_ID, BIOMETRICS_ID INTO r_seg_id,  r_biometric_id FROM SEGMENT_VACANCIES limit 1 FOR UPDATE;
      INSERT INTO PERSON_BIOMETRICS (BIOMETRICS_ID,
        CONTAINER_ID,
        EXTERNAL_ID,
        REGISTED_TS)
        VALUES (r_biometric_id, p_container_id, p_external_id, UNIX_TIMESTAMP(NOW()));  
     -- SELECT COUNT(*) into l_count FROM  SEGMENTS where SEGMENT_ID = r_seg_id;
	   update SEGMENTS
           SET 
            BINARY_LENGTH = BINARY_LENGTH + tmp_one_templte_size,
            RECORD_COUNT = RECORD_COUNT + 1,
            VERSION = VERSION + 1,            
            REVISION = REVISION + 1
            WHERE SEGMENT_ID=r_seg_id;
	   SELECT VERSION INTO r_seg_version FROM SEGMENTS WHERE SEGMENT_ID=r_seg_id;
           SET p_new_seg_created = FALSE;
	   SET p_seg_id = r_seg_id;
	   SET p_seg_version = r_seg_version;
	   SET p_biometric_id = r_biometric_id;	          
     INSERT INTO SEGMENT_CHANGE_LOG (SEGMENT_ID,
       SEGMENT_VERSION,
       CHANGE_TYPE,
       EXTERNAL_ID,
       TEMPLATE_DATA,
       UPDATE_TS,
       P_NO,
       BIOMETRICS_ID)
        VALUES (r_seg_id, r_seg_version, 1, p_external_id, p_biometric_data, CAST(NOW() AS date), p_no, r_biometric_id);   
      DELETE FROM SEGMENT_VACANCIES WHERE SEGMENT_ID=r_seg_id AND BIOMETRICS_ID=r_biometric_id;	    
      IF t_error = 1 THEN     
        rollback;
        SET p_seg_id = -9999;
        SET p_seg_version = -9999;
        SET p_biometric_id = -99999;
        SET p_new_seg_created = NULL;
      END IF;  
	 LEAVE lab_begin;
    END IF; 
    -- body
    SELECT
      COUNT(c.CONTAINER_ID) INTO l_count
    FROM CONTAINERS c
    WHERE c.CONTAINER_ID = p_container_id;
    IF l_count < 1 THEN
      rollback;
      SET t_error = 1;
      SET p_seg_id = -1;
      SET p_seg_version = -1;
      SET p_biometric_id = -1;
      SET p_new_seg_created = FALSE;
      LEAVE lab_begin;
    END IF; 
    
    SELECT get_last_bio_id() into last_bio_id;
    IF last_bio_id  is NULL || last_bio_id < 0 THEN
	  rollback;
      SET t_error = 1;
      SET p_seg_id = -1;
      SET p_seg_version = -1;
      SET p_biometric_id = -1;
      SET p_new_seg_created = FALSE;
      LEAVE lab_begin;    
    END IF; 
    
    INSERT INTO PERSON_BIOMETRICS (BIOMETRICS_ID,CONTAINER_ID,
    EXTERNAL_ID,
    REGISTED_TS)
      VALUES (last_bio_id,p_container_id, p_external_id, UNIX_TIMESTAMP(NOW()));
    SELECT
      MAX(BIOMETRICS_ID) INTO l_biometrics_id
    FROM PERSON_BIOMETRICS;
    SET r_biometric_id = l_biometrics_id;
    SELECT
      CONTAINER_ID,
      MAX_SEGMENT_SIZE,
      MAX_RECORD_COUNT INTO tmp_container_id, tmp_max_segment_size, tmp_max_seg_record_count
    FROM CONTAINERS
    WHERE CONTAINER_ID = p_container_id;

    SELECT
      seg.SEGMENT_ID,
      seg.BINARY_LENGTH,
      seg.RECORD_COUNT,
      seg.VERSION,
      seg.REVISION,
      seg.BIO_ID_END INTO seg_semgnet_id, seg_binary_length,
      seg_record_count,
    seg_version, seg_revision, seg_bio_id_end
    FROM SEGMENTS seg
    WHERE CONTAINER_ID = p_container_id
    AND SEGMENT_ID = (SELECT
        MAX(SEGMENT_ID)
      FROM SEGMENTS s
      WHERE s.CONTAINER_ID = p_container_id);

    IF seg_bio_id_end < l_biometrics_id THEN
      SET seg_bio_id_end = l_biometrics_id;
    END IF;
    
    
      IF seg_record_count + 1 <= tmp_max_seg_record_count THEN
      UPDATE SEGMENTS
      SET BINARY_LENGTH = seg_binary_length + tmp_one_templte_size,
          RECORD_COUNT = seg_record_count + 1,
          VERSION = seg_version + 1,
          REVISION = seg_revision + 1,
          BIO_ID_END = l_biometrics_id
      WHERE SEGMENT_ID = seg_semgnet_id
      AND REVISION = seg_revision;
      INSERT INTO SEGMENT_CHANGE_LOG (SEGMENT_ID,
      SEGMENT_VERSION,
      CHANGE_TYPE,
      EXTERNAL_ID,
      TEMPLATE_DATA,
      UPDATE_TS,
      P_NO,
      BIOMETRICS_ID)
        VALUES (seg_semgnet_id, seg_version + 1, 1, p_external_id, p_biometric_data, CAST(NOW() AS date), p_no, l_biometrics_id);
      SET r_seg_id = seg_semgnet_id;
      SET r_seg_version = seg_version + 1;
      SET l_seg_created = FALSE;
    ELSE
      INSERT INTO SEGMENTS (BIO_ID_START,
      BIO_ID_END,
      BINARY_LENGTH,
      RECORD_COUNT,
      VERSION,
      CONTAINER_ID,
      REVISION)
        VALUES (l_biometrics_id, l_biometrics_id + tmp_max_seg_record_count, tmp_one_templte_size + 1024, 1, -1, p_container_id, -1);
      SELECT
        LAST_INSERT_ID() INTO r_seg_id;
    INSERT INTO SEGMENT_CHANGE_LOG (SEGMENT_ID,
      SEGMENT_VERSION,
      CHANGE_TYPE,
      EXTERNAL_ID, 
      TEMPLATE_DATA,
      UPDATE_TS,
      P_NO,
      BIOMETRICS_ID)
        VALUES (r_seg_id, -1, 1, p_external_id, p_biometric_data,CAST(NOW() AS date), p_no, r_biometric_id);       
        
      SET r_seg_version = -1;
      SET l_seg_created = TRUE;	
    END IF;
    SET p_seg_id = r_seg_id;
    SET p_seg_version = r_seg_version;
    SET p_biometric_id = r_biometric_id;
    SET p_new_seg_created = l_seg_created;   
   
    SET r_update_bio_id = update_last_bio_id(last_bio_id);
    IF r_update_bio_id <=> 1 THEN
       SET t_error = 1;
    END IF; 
    IF t_error = 1 THEN
       rollback;
      SET p_seg_id = -9999;
      SET p_seg_version = -9999;
      SET p_biometric_id = -99999;
      SET p_new_seg_created = NULL;
    END IF;
  END