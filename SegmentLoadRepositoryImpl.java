package com.nec.aim.dm.dmservice.persistence;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.nec.aim.dm.dmservice.entity.SegmentLoading;

@Repository
public class SegmentLoadRepositoryImpl implements SegmentLoadRepository {
	private static final String insertSql = "insert into SEGMENT_LOADING(SB_ID, SEGMENT_ID, STATUS,LAST_VERSION, LAST_TS) values (?,?,?,?,CURRENT_TIMESTAMP())";
	private static final String updateSqlWithMailFlag = "update SEGMENT_LOADING set STATUS=?,LAST_VERSION=?, LAST_TS=CURRENT_TIMESTAMP() where SB_ID=? and SEGMENT_ID =?";
	private static final String updateSqlWithNoMailFlag = "update SEGMENT_LOADING set STATUS=1, LAST_VERSION=?, LAST_TS=CURRENT_TIMESTAMP() where SB_ID=? and SEGMENT_ID =?";
	private static final String updateAfterdeleteBioSql = "update SEGMENT_LOADING set STATUS=1, LAST_VERSION=?, LAST_TS=CURRENT_TIMESTAMP() where SB_ID=? and SEGMENT_ID =?";
	private static final String updateAfterNew = "update SEGMENT_LOADING set LAST_VERSION =? where SB_ID =? and SEGMENT_ID=?";
	private static final String getLastVerSql = "select LAST_VERSION from SEGMENT_LOADING where SB_ID = ? and SEGMENT_ID =? for update";
	private static final String getActiveStoragesUrlSql = "select NSM.URL from STORAGE_BOX NSM, SEGMENT_LOADING SL where SL.SB_ID=NSM.SB_ID and NSM.STATUS = 1 and SL.SEGMENT_ID=? for update";
	
	private ReentrantLock slLock = new ReentrantLock();

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void insertSegmentLoad(SegmentLoading segLoad) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		if (slLock.tryLock()) {
			try {
				jdbcTemplate.update(insertSql, new Object[] { segLoad.getNsmId(), segLoad.getSegmentId(), segLoad.getStatus(),
						segLoad.getLastVersion() });
			} finally {
				slLock.unlock();
			}
		}		
	}

	@Override
	public void updateSegmentLoadWithMailFlag(SegmentLoading segLoad) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		if (slLock.tryLock()) {
			try {
				jdbcTemplate.update(updateSqlWithMailFlag, new Object[] { segLoad.getStatus(), segLoad.getLastVersion(),
						segLoad.getNsmId(), segLoad.getSegmentId() });
			} finally {
				slLock.unlock();
			}
		}		
	}

	@Override
	public void updateSegmentLoadNoMailFlag(SegmentLoading segLoad) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		if (slLock.tryLock()) {
			try {
				jdbcTemplate.update(updateSqlWithNoMailFlag,
						new Object[] { segLoad.getLastVersion(), segLoad.getNsmId(), segLoad.getSegmentId() });
			} finally {
				slLock.unlock();
			}
		}		
	}

	@Override
	public void updateAfterDelWithNoMailFlag(SegmentLoading segLoad) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		if (slLock.tryLock()) {
			try {
				jdbcTemplate.update(updateAfterdeleteBioSql, new Object[] { segLoad.getLastVersion(), segLoad.getNsmId(), segLoad.getSegmentId() });
			} finally {
				slLock.unlock();
			}
		}
	}

	@Override
	public void updateAfterNew(long version, int sb_id, long segmentId) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		if (slLock.tryLock()) {
			try {
				jdbcTemplate.update(updateAfterNew, new Object[] { version, sb_id, segmentId });
			} finally {
				slLock.unlock();
			}
		}		
	}

	@Override
	public long getLastVersion(int storageId, long segmentId) throws SQLException {
		jdbcTemplate.execute("SET @@autocommit=0");
		Long lastVer = null;
		if (slLock.tryLock()) {
			try {
				lastVer = jdbcTemplate.queryForObject(getLastVerSql, new Object[] { storageId, segmentId }, Long.class);
			} finally {
				slLock.unlock();
			}
		}		
		return lastVer.longValue();
	}

	@Override
	public List<String> getActiveStorageUrl(long segmentId) throws SQLException {
		List<String> result = null;
		if (slLock.tryLock()) {
			try {
				result = jdbcTemplate.queryForList(getActiveStoragesUrlSql, new Object[] { segmentId }, String.class);
			} finally {
				slLock.unlock();
			}
		}
		return result;		
	}
}
