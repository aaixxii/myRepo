--------------------------------------------------------
--  ÉtÉ@ÉCÉãÇçÏê¨ÇµÇ‹ÇµÇΩ - ã‡ójì˙-11åé-30-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SAKURA_MANKAI_BULK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "XMPERF"."SAKURA_MANKAI_BULK" AUTHID CURRENT_USER IS
  l_bin_id  NUMBER; 
  l_semgnet_id NUMBER;  
  l_expand_size  NUMBER;  
  l_init_biometricId  NUMBER;
  l_init_external_id VARCHAR2(36);
  l_init_event_id VARCHAR2(36);
  l_event_info_count NUMBER; 
  l_base_biometricId  NUMBER;
  l_base_seg_ver  NUMBER;
  l_exist_event_count NUMBER;
  l_insert_count NUMBER;
  g_idx NUMBER :=1; 
  l_buffer    RAW(10);
  l_strat     NUMBER :=5;
  l_len       NUMBER :=16;
  l_biometricId  NUMBER;
  l_template_data_edit blob; 
 
--  TYPE seg_count_rec IS RECORD (
--     seg_id	BIO_EVENT_INFO. ASSIGNED_SEGMENT_ID%TYPE,
--     count 	NUMBER
--	);
--  TYPE seg_count_tab is table of seg_count_rec;
--  seg_counts seg_count_tab; 
  
  type xm_perf_tab is table of XM_PERF_TEST%rowtype;
  xm_perf_info xm_perf_tab;
  xm_perf_info_count NUMBER; 
  l_template_data  BIO_TEMPLATE_DATA_INFO.TEMPLATE_DATA%TYPE;  
  l_event_info_rec  BIO_EVENT_INFO%ROWTYPE;
  TYPE l_event_info_tab is table of BIO_EVENT_INFO%ROWTYPE;
  l_event_info_exist l_event_info_tab;  
  v_base_size NUMBER;
 
BEGIN 
    SELECT MAX(BIOMETRIC_ID), MAX(EXTERNAL_ID), MAX(EVENT_ID) INTO l_init_biometricId, l_init_external_id,  l_init_event_id from BIO_EVENT_INFO;
    SELECT COUNT(*) into xm_perf_info_count FROM xm_perf_test;
    IF (xm_perf_info_count < 1) THEN
       dbms_output.put_line('No data in xm_perf_test, please input data to xm_perf_test for tesst.');
       return;    
    END IF;
    xm_perf_info := xm_perf_tab();
    xm_perf_info.extend(xm_perf_info_count);
    l_event_info_exist := l_event_info_tab();
    SELECT * BULK COLLECT INTO xm_perf_info FROM xm_perf_test;
    FOR i IN 1..xm_perf_info.count LOOP
       l_bin_id := xm_perf_info(i).BIN_ID;
       l_semgnet_id:= xm_perf_info(i).SEGMENT_ID;   
       l_expand_size := xm_perf_info(i).EXPAND_SIZE; 
       l_insert_count :=0;
       l_base_seg_ver := 0;
       SELECT count(BIOMETRIC_ID), max(BIOMETRIC_ID),max(DATA_VERSION) INTO l_exist_event_count, l_base_biometricId, l_base_seg_ver from BIO_EVENT_INFO WHERE BIN_ID=l_bin_id  AND ASSIGNED_SEGMENT_ID=l_semgnet_id; 
       IF (l_exist_event_count < 1) THEN
          dbms_output.put_line('No data to process,  for binId:' || to_char(l_bin_id) ||' semgnetId:' ||l_semgnet_id  || ' .skip');
          CONTINUE;
       END IF;       
       SELECT * BULK COLLECT  INTO l_event_info_exist FROM bio_event_info WHERE BIN_ID = l_bin_id AND ASSIGNED_SEGMENT_ID=l_semgnet_id;       
       while (l_expand_size > 0) LOOP
        FOR j IN l_event_info_exist.FIRST..l_event_info_exist.LAST LOOP 
          
           IF (l_insert_count >= l_expand_size) THEN           
              CONTINUE;
           END IF;
           l_event_info_rec := l_event_info_exist(j);
           SELECT TEMPLATE_DATA into l_template_data FROM BIO_TEMPLATE_DATA_INFO WHERE TEMPLATE_DATA_ID= l_event_info_rec.BIOMETRIC_ID; 
           l_event_info_rec.BIOMETRIC_ID := l_init_biometricId + g_idx;
           l_event_info_rec.EXTERNAL_ID := l_init_external_id || g_idx;
           l_event_info_rec.EVENT_ID := l_init_event_id || g_idx;
           l_event_info_rec.DATA_VERSION := l_base_seg_ver + g_idx;
           l_event_info_rec.PHASE := 'PS';
           l_event_info_rec.CREATE_DATETIME := SYSTIMESTAMP;
           l_event_info_rec.UPDATE_DATETIME := SYSTIMESTAMP;
           l_event_info_rec.TEMPLATE_DATA_KEY := l_init_biometricId + g_idx;
           INSERT INTO /*+ append parallel nologging */ BIO_EVENT_INFO VALUES l_event_info_rec;
           INSERT INTO /*+ append parallel nologging */ BIO_TEMPLATE_DATA_INFO(TEMPLATE_DATA_ID, TEMPLATE_DATA,CREATE_DATETIME,UPDATE_DATETIME ) 
                  VALUES(TO_CHAR(l_init_biometricId + g_idx), l_template_data, SYSTIMESTAMP, SYSTIMESTAMP) RETURNING TEMPLATE_DATA INTO l_template_data_edit;
           l_buffer := DEC_TO_LE_RAW5_BYTE(l_init_biometricId + g_idx);
           l_len := utl_raw.length(l_buffer);
           DBMS_LOB.WRITE(l_template_data_edit, l_len,  l_strat, l_buffer);          
           l_insert_count :=  l_insert_count + 1;
           l_expand_size := l_expand_size -1;
           g_idx := g_idx +1;          
           IF (mod(l_insert_count,1000) = 0) THEN
              commit;
           END IF;         
        END LOOP; 
      END LOOP; 
      UPDATE BIO_MATCHER_SEGMENT_INFO SET SEGMENT_VERSION= l_base_seg_ver ,UPDATE_DATETIME=SYSTIMESTAMP WHERE SEGMENT_ID=l_semgnet_id;
      commit;
      dbms_output.put_line ('BIN_ID:' || to_char(l_bin_id) ||' SEGMENT_ID:' ||l_semgnet_id || ' add count:' || to_char( l_insert_count)); 
    END LOOP;
    EXCEPTION
    WHEN OTHERS THEN
       dbms_output.Put_line ('Error Code:' 
                                   || To_char(SQLCODE) 
                                   || '  Error Message:' 
                                   || SQLERRM);
       ROLLBACK;   
END SAKURA_MANKAI_BULK;

/
