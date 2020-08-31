package jp.co.nec.aim.mm.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * SegmentDiffDao
 * 
 * @author liuyq
 * 
 */
public class SegmentDiffDao {

	/** log instance **/
	private static final Logger log = LoggerFactory
			.getLogger(SegmentDiffDao.class);
	/** JdbcTemplate instance **/
	private JdbcTemplate jdbcTemplate;

	/** GET_MU_SEG_UPDATES_SQL **/	
	private static final String GET_MU_SEG_UPDATES_SQL = ""
	    + "SELECT "
	    + "    a.MU_ID AS unitId, "
	    + "    a.SEGMENT_ID AS segmentId, "
	    + "    a.AIM_RANK AS `rank`, "
	    + "    a.STATUS AS `status`, "
	    + "    a.SEGMENT_VERSION AS reportVersion, "
	    + "    a.SEGMENT_QUEUED_VERSION AS reportQueuedVersion, "
	    + "    s.VERSION AS latestVersion "
	    + "FROM  (SELECT sel.MU_ID, sel.SEGMENT_ID, sel.STATUS, sel.SEGMENT_VERSION, sel.SEGMENT_QUEUED_VERSION , msl.AIM_RANK "
	    + "       FROM MU_SEG_REPORTS sel  LEFT JOIN MU_SEGMENTS msl ON sel.MU_ID = msl.MU_ID AND sel.SEGMENT_ID = msl.SEGMENT_ID "
	    + "      UNION "
	    + "       SELECT  msr.MU_ID, msr.SEGMENT_ID, ser.STATUS, ser.SEGMENT_VERSION, ser.SEGMENT_QUEUED_VERSION , msr.AIM_RANK "
	    + "       FROM  MU_SEG_REPORTS ser  RIGHT JOIN MU_SEGMENTS msr ON ser.MU_ID = msr.MU_ID  AND ser.SEGMENT_ID = msr.SEGMENT_ID) a "
	    + "LEFT JOIN SEGMENTS s ON a.SEGMENT_ID = s.SEGMENT_ID "
	    + "LEFT JOIN MATCH_UNITS mu ON mu.MU_ID = a.MU_ID "
	    + "WHERE 1 = 1 "
	    + " AND (s.VERSION IS NULL OR a.AIM_RANK IS NULL "
	    + " OR a.AIM_RANK IS NULL "
	    + " OR a.STATUS IS NULL "
	    + " OR a.SEGMENT_QUEUED_VERSION IS NULL "
	    + " OR a.SEGMENT_QUEUED_VERSION != s.VERSION) "
	    + " AND mu.MU_ID = ?";

	/** GET_MU_SEG_UPDATES_SQL **/
	private static final String GET_MU_SEG_INFO_SQL = "SELECT a.MU_ID as unitId, "
			+ "       s.SEGMENT_ID as segmentId,"
			+ "       a.AIM_RANK as `rank`,"
			+ "       s.VERSION as latestVersion "
			+ " FROM SEGMENTS s "
			+ " LEFT JOIN MU_SEGMENTS a ON  s.SEGMENT_ID = a.SEGMENT_ID"
			+ " LEFT JOIN MATCH_UNITS mu ON mu.MU_ID = a.MU_ID"
			+ " WHERE s.SEGMENT_ID = ? ";

	

	/**
	 * SegmentDiffDao constructor
	 * 
	 * @param manager
	 *            EntityManager instance
	 * @param dataSource
	 *            DataSource dataSource
	 */
	public SegmentDiffDao(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	/**
	 * get the MU segment version information that report version <br>
	 * is different from the latest segment version
	 * 
	 * @param muId
	 *            match unit id
	 * @return MapReportCombined list
	 */
	public List<MapReportCombined> getMuDiffInfos(long muId) {
		List<MapReportCombined> results = jdbcTemplate.query(
				GET_MU_SEG_UPDATES_SQL, new Object[] { new Long(muId) },
				new MapReportCombined());
		log.debug("Got " + results.size()
				+ " segment update query results for muId " + muId);
		return results;
	}

	/**
	 * getSegDiffInfos
	 * 
	 * @param segmentId
	 *            segmentId
	 * @return MapReportCombined list
	 */
	public List<MapReportCombined> getSegMuDiffInfos(long segmentId) {
		List<MapReportCombined> results = jdbcTemplate.query(
				GET_MU_SEG_INFO_SQL, new Object[] { new Long(segmentId) },
				new MapReportCombinePart());
		log.debug("Got " + results.size()
				+ " segment update query results for segment " + segmentId);
		return results;
	}

	/**
	 * MapReportCombinePart
	 * 
	 * @author liuyq
	 * 
	 */
	public static class MapReportCombinePart extends MapReportCombined {

		@Override
		public MapReportCombined mapRow(ResultSet rs, int rowNum)
				throws SQLException {
			final MapReportCombined info = new MapReportCombined();
			info.setSegmentId(rs.getLong("SEGMENTID"));

			String unitId = rs.getString("UNITID");
			info.setUnitId((unitId == null) ? null : Long.valueOf(unitId));

			String rank = rs.getString("RANK");
			info.setRank((rank == null) ? null : Integer.valueOf(rank));

			String latestVersion = rs.getString("LATESTVERSION");
			info.setLatestVersion((latestVersion == null) ? null : Integer
					.valueOf(latestVersion));
			return info;
		}

	}

	/**
	 * MapReportCombined
	 * 
	 * @author liuyq
	 * 
	 */
	public static class MapReportCombined implements
			RowMapper<MapReportCombined> {
		private Long unitId;
		private long segmentId;
		private Integer rank;
		private Integer status;
		private Integer reportVersion;
		private Integer reportQueuedVersion;
		private Integer latestVersion;
		//private String sourceURLBase;

		public Long getUnitId() {
			return unitId;
		}

		public void setUnitId(Long unitId) {
			this.unitId = unitId;
		}

		public long getSegmentId() {
			return segmentId;
		}

		public void setSegmentId(long segmentId) {
			this.segmentId = segmentId;
		}

		public Integer getRank() {
			return rank;
		}

		public void setRank(Integer rank) {
			this.rank = rank;
		}

		public Integer getStatus() {
			return status;
		}

		public void setStatus(Integer status) {
			this.status = status;
		}

		public Integer getReportVersion() {
			return reportVersion;
		}

		public void setReportVersion(Integer reportVersion) {
			this.reportVersion = reportVersion;
		}

		public Integer getReportQueuedVersion() {
			return reportQueuedVersion;
		}

		public void setReportQueuedVersion(Integer reportQueuedVersion) {
			this.reportQueuedVersion = reportQueuedVersion;
		}

		public Integer getLatestVersion() {
			return latestVersion;
		}

		public void setLatestVersion(Integer latestVersion) {
			this.latestVersion = latestVersion;
		}
	

		@Override
		public MapReportCombined mapRow(ResultSet rs, int rowNum)
				throws SQLException {
			final MapReportCombined info = new MapReportCombined();

			String unitId = rs.getString("UNITID");
			info.setUnitId((unitId == null) ? null : Long.valueOf(unitId));

			info.setSegmentId(rs.getLong("SEGMENTID"));

			String rank = rs.getString("RANK");
			info.setRank((rank == null) ? null : Integer.valueOf(rank));

			String status = rs.getString("STATUS");
			info.setStatus((status == null) ? null : Integer.valueOf(status));

			String reportQueuedVersion = rs.getString("REPORTQUEUEDVERSION");
			info.setReportQueuedVersion((reportQueuedVersion == null) ? null
					: Integer.valueOf(reportQueuedVersion));

			String reportVersion = rs.getString("REPORTVERSION");
			info.setReportVersion((reportVersion == null) ? info
					.getReportQueuedVersion() : Integer.valueOf(reportVersion));

			String latestVersion = rs.getString("LATESTVERSION");
			info.setLatestVersion((latestVersion == null) ? null : Integer
					.valueOf(latestVersion));		
			return info;
		}
	}

}
