package jp.co.nec.aim.mm.procedure;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.StoredProcedure;

import com.google.common.base.Joiner;

import jp.co.nec.aim.mm.identify.planner.JobInfoFromDB;

/**
 * 
 * @author xiazp
 *
 */
public class GetJobInfoForCreatePlansProcedure extends StoredProcedure {
	private static final String GET_JOB_INFO_FOR_CREATE_PLANS = "get_job_info_for_create_plans";
	private JdbcTemplate jdbcTemplate;
	private String limitedJobId;
	private Long[] jobIdArray;

	public GetJobInfoForCreatePlansProcedure(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
		setDataSource(dataSource);
		setSql(GET_JOB_INFO_FOR_CREATE_PLANS);
		declareParameter(new SqlParameter("limited_job_ids", Types.VARCHAR));				
		declareParameter(new SqlOutParameter("tab_name", Types.VARCHAR));	

		compile();
	}

	/**
	 * 
	 * @param jobIdList
	 * @return
	 * @throws DataAccessException
	 * @throws SQLException
	 */	
	public List<JobInfoFromDB> getJobInfoFromDB(Long[] jobIds)
			throws SQLException {
		if (jobIds == null) {
			throw new IllegalArgumentException("jobIds == null");
		}
		setJobIdArray(jobIds);
		limitedJobId = Joiner.on(",").skipNulls().join(jobIds);		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("limited_job_ids", limitedJobId);
		Map<String, Object> resultMap = execute(map);
		String tableName = (String) resultMap.get("tab_name");
		if (StringUtils.isBlank(tableName)) return null;
		String sql = "select * from " + tableName;
		List<Map<String, Object>> resMap = jdbcTemplate.queryForList(sql);		
		final List<JobInfoFromDB> jobInfoFromDBlist = new ArrayList<>();
		resMap.forEach(one -> {
			Long jobId = (Long) one.get("job_id");
			Integer famliyId = (Integer)one.get("family_id");
			Integer fuctionId = (Integer)one.get("function_id");
			Integer containerId = (Integer)one.get("container_id");
			Long containerJobId = (Long) one.get("container_job_id");
			JobInfoFromDB jobInfoFromDB = new JobInfoFromDB(jobId, famliyId, fuctionId, containerId, containerJobId);
			jobInfoFromDBlist.add(jobInfoFromDB);
		});
		jdbcTemplate.execute("DROP TEMPORARY TABLE IF EXISTS " + tableName);
		return jobInfoFromDBlist;
	}

	public Long[] getJobIdArray() {
		return jobIdArray;
	}

	public void setJobIdArray(Long[] jobIdArray) {
		this.jobIdArray = jobIdArray;
	}
	
	

	public String getLimitedJobId() {
		return limitedJobId;
	}

	public void setLimitedJobId(String limitedJobId) {
		this.limitedJobId = limitedJobId;
	}
}
