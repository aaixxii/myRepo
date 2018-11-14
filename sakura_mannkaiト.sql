--------------------------------------------------------

 BEGIN     
    SELECT TEMPLATE_DATA INTO l_template_data_edit FROM BIO_TEMPLATE_DATA_INFO WHERE TEMPLATE_DATA_ID=14 FOR UPDATE;
    
    --v_len := DBMS_LOB.getlength(l_template_data_edit); 
    l_data := 10004;
     l_init_external_id:='10007          ';
     l_init_external_id:= SUBSTR(l_init_external_id,1,8);
  
  
   l_buffer :=utl_raw.cast_to_raw(l_init_external_id);
    --l_buffer := utl_raw.cast_from_binary_integer(l_data);
--     dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer));
--   -- dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer));
  -- v_len := DBMS_LOB.getlength(l_buffer); 
--    
--  -- l_buffer:= utl_raw.cast_from_number(l_count);  
--   DBMS_LOB.WRITE(l_template_data_edit, v_len,  v_strat, l_buffer);
--   
--    commit;
--    v_len :=8;
--    DBMS_LOB.READ(l_template_data_edit, v_len,  v_strat, l_buffer2);
--    
--      dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer2));
--      
--       DBMS_LOB.READ(l_template_data_edit, v_len,  v_strat, l_buffer);
--  
    v_len := DBMS_LOB.getlength(l_buffer);
    
    
    DBMS_LOB.WRITE(l_template_data_edit, v_len,  v_strat, l_buffer);
    
   v_len :=8;
     DBMS_LOB.READ(l_template_data_edit, v_len,  v_strat, l_buffer2);
      dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer2));
  
    
     --dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer));
     --select to_char(l_buffer,'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') into l_data from dual;
     
 
  
  -- dbms_output.put_line(l_data);
--     dbms_output.put_line(to_char(l_init_biometricId + i));
--    
--    l_buffer :=utl_raw.cast_to_raw(to_char(l_init_biometricId + i));
--    -- DBMS_LOB.READ(l_template_data_edit, v_len,  v_strat, l_buffer);    
--     dbms_output.put_line(l_buffer);
--         
--    v_len := 8; 
--    --DBMS_LOB.ERASE(l_template_data_edit, v_len, v_strat);
--     v_len :=  8; 
--      dbms_output.put_line(v_len);
--    DBMS_LOB.WRITE(l_template_data_edit, v_len, v_strat, l_buffer);    

HEXTORAW ( string ) 

RAWTOHEX ( raw ) 

utl_raw.cast_to_varchar2

BITAND

select to_char(4567,'xxxx') from dual;
select to_char(123,'xxx') from dual;

dbms_output.put_line(utl_raw.cast_to_varchar2(l_buffer));
l_buffer := utl_raw.cast_from_binary_integer(l_data);
--    





