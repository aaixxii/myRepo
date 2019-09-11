CREATE DEFINER=`aimuser`@`%` PROCEDURE `check_container_formats`(
     in p_function_id int,
     in  p_candidate_containers VARCHAR(1024)
)
    READS SQL DATA
    SQL SECURITY INVOKER
BEGIN
     -- p_candidate_containers="1,2,3,321,322,323"；     
      DECLARE l_actual_format_name VARCHAR(255);
      DECLARE l_target_format_name VARCHAR(255);
      DECLARE l_target_format_id int;
      DECLARE l_function_name VARCHAR(20);	
      declare v_idx int default 999 ;
      declare v_tmp_str varchar(20);
      declare v_id int;
      declare v_format_id int;
	  DECLARE t_error INTEGER DEFAULT 0;  
      declare t_err_msg varchar(2048);
      declare id_cur cursor for
		select c.container_id 
        from containers c
        right join arr_container_ids a
        on c.container_id =a.id ;  
	  declare cur cursor for 
            select c.CONTAINER_ID,a.id, c.format_id
        from CONTAINERS c
        right join arr_container_ids a
        on a.id = c.CONTAINER_ID 
        and c.format_id  = l_target_format_id;
	  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error=1;
	  DROP TEMPORARY TABLE IF EXISTS arr_container_ids;
      create temporary table arr_container_ids(id int) engine=memory; 
	  while v_idx > 0 do
         SET v_idx = INSTR(p_candidate_containers,',');
         SET v_tmp_str = substr(p_candidate_containers,1,t_idx-1);    
         insert into arr_container_ids (id)  values( CAST(v_tmp_str AS UNSIGNED));
         set p_candidate_containers=substr(p_candidate_containers,v_idx +1 ,LENGTH(p_candidate_containers)); 
      end while;
	 SELECT  f.target_format_id, f.function_name INTO l_target_format_id, l_function_name              
      FROM function_types f
      WHERE f.function_id = p_function_id;
	  set r_err_str = "";
        open id_cur;
        loop
		 FETCH id_cur INTO v_id;
         if (v_id is null) then
           set r_err_str = CONCAT(r_err_str ,'invalid container_id:' , CAST(v_id as char));
		 end if;
		end loop;  
        close id_cur;
        set v_id= -1;
      open cur;
      lable_loop: loop
      FETCH cur INTO v_id, v_format_id;
      if v_format_id is null then
          set r_err_str = CONCAT(r_err_str, 'container_id: ',CAST(v_id as char), 'function_id: ',CAST(p_function_id as char),' has invalid format_id:' , CAST(v_format_id as char));
	 end if;
     end loop;
     close cur;
END