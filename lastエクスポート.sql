--------------------------------------------------------
--  ƒtƒ@ƒCƒ‹‚ðì¬‚µ‚Ü‚µ‚½ - ‹à—j“ú-11ŒŽ-09-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure XM_DB_EXPANDER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "XMPERF"."XM_DB_EXPANDER" 
(
  TEMPLATE_TYPE IN VARCHAR2, 
  BIN_ID IN NUMBER,
  DB_SIZE IN NUMBER
) IS 
  --l_template_id   BIO_TEMPLATE_DATA_INFO.TEMPLATE_DATA_ID%TYPE;
  l_template_data  BIO_TEMPLATE_DATA_INFO.TEMPLATE_DATA%TYPE;

  --l_bio_detail_info_rec  BIO_IDENTIFIER_DETAIL_INFO%ROWTYPE;
  l_event_info_rec  BIO_EVENT_INFO%ROWTYPE;
  l_semgnet_id NUMBER;
  l_bin_id  NUMBER;
  --l_sn_node_id VARCHAR2(15);
  l_last_sement_version NUMBER;

  l_init_biometricId  NUMBER;
  l_init_external_id VARCHAR2(36);
  l_init_event_id VARCHAR2(36);
  l_event_info_count NUMBER;
  l_last_segmet_version NUMBER;
  l_template_size NUMBER;
  l_exist_event_count NUMBER;

  --TYPE sn_array IS TABLE OF VARCHAR2(4000 BYTE) INDEX BY VARCHAR2(5);

  CURSOR event_c1(skip_num NUMBER) IS
  SELECT * FROM BIO_EVENT_INFO WHERE BIOMETRIC_ID > skip_num;
  event_rec event_c1%rowtype; 

  BEGIN  

   -- SELECT bin_id INTO l_bin_id FROM BIO_MATCHER_BIN_INFO WHERE TEMPLATE_TYPE=TEMPLATE_TYPE;
   l_bin_id:=BIN_ID;
    SELECT * into l_event_info_rec from BIO_EVENT_INFO WHERE BIOMETRIC_ID=(SELECT max(BIOMETRIC_ID) FROM BIO_EVENT_INFO);
   
    l_init_biometricId := l_event_info_rec.BIOMETRIC_ID;
    l_init_external_id := l_event_info_rec.EXTERNAL_ID;
    l_init_event_id := l_event_info_rec.EVENT_ID;
    l_template_size := l_event_info_rec.TEMPLATE_SIZE;
    --l_bin_id := l_event_info_rec.BIN_ID;
 
    --SELECT MATCHER_NODE_ID BULK COLLECT INTO sn_array FROM BIO_MATCHER_NODE_SEGMENT_INFO WHERE SEGMENT_ID=(select segment_id from BIO_IDENTIFIER_INFO where bin_id=l_bin_id); 
    SELECT max(SEGMENT_ID) INTO l_semgnet_id FROM BIO_IDENTIFIER_INFO WHERE BIN_ID=l_bin_id;  
     dbms_output.put_line (l_semgnet_id); 
   -- SELECT MATCHER_NODE_ID INTO l_sn_node_id FROM BIO_MATCHER_NODE_SEGMENT_INFO WHERE SEGMENT_ID=l_semgnet_id AND ASSIGNED_FLAG='Y'; 
    SELECT TEMPLATE_DATA into l_template_data FROM BIO_TEMPLATE_DATA_INFO WHERE TEMPLATE_DATA_ID= l_init_biometricId;
    
    SELECT max(BIOMETRIC_ID) INTO l_exist_event_count from BIO_EVENT_INFO WHERE BIN_ID=l_bin_id; 
    
    FOR i IN 1..DB_SIZE LOOP
      INSERT INTO  /*+ append parallel nologging */  BIO_EVENT_INFO(BIOMETRIC_ID,EXTERNAL_ID,EVENT_ID,BIN_ID,STATUS,PHASE,CREATE_DATETIME,UPDATE_DATETIME,TEMPLATE_DATA_KEY,TEMPLATE_SIZE,DATA_VERSION,ASSIGNED_SEGMENT_ID,SITE_ID)
      VALUES(l_init_biometricId + i,  l_init_external_id || i, l_init_event_id || i, l_event_info_rec.BIN_ID, l_event_info_rec.STATUS,l_event_info_rec.PHASE, SYSTIMESTAMP,SYSTIMESTAMP,
      l_init_biometricId + i,l_template_size, l_event_info_rec.DATA_VERSION + i, l_event_info_rec.ASSIGNED_SEGMENT_ID, l_event_info_rec.SITE_ID); 
     commit;
    END LOOP; 
     -- SELECT max(DATA_VERSION) into l_last_segmet_version FROM BIO_EVENT_INFO WHERE bin_id = l_bin_id;  
      dbms_output.put_line (l_last_segmet_version); 
    --UPDATE BIO_MATCHER_NODE_SEGMENT_INFO SET SEGMENT_VERSION=l_last_segmet_version ,UPDATE_DATETIME=SYSTIMESTAMP WHERE SEGMENT_ID=l_semgnet_id; 
    --commit;
 
    SELECT count(BIOMETRIC_ID) INTO l_event_info_count FROM BIO_EVENT_INFO; 
    OPEN event_c1(l_exist_event_count);
    LOOP
      FETCH event_c1 INTO event_rec;
      IF event_c1%notfound then
         dbms_output.put_line ('no data found in bio_evnet_info table!');
         RETURN;
      ELSE      
        INSERT INTO /*+ append parallel nologging */ BIO_TEMPLATE_DATA_INFO(TEMPLATE_DATA_ID, TEMPLATE_DATA,CREATE_DATETIME,UPDATE_DATETIME ) VALUES(event_rec.BIOMETRIC_ID,l_template_data, SYSTIMESTAMP, SYSTIMESTAMP);       
        commit;
      END IF;
    END LOOP;    
  
    EXCEPTION
      WHEN OTHERS THEN
       dbms_output.put_line ('An error occurred while do XM_DB_EXPANDER'); 
END XM_DB_EXPANDER;

/
