package com.nec.biomatcher.comp.bioevent.dataAccess.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.dao.DataAccessException;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.nec.biomatcher.comp.bioevent.dataAccess.BiometricEventCriteria;
import com.nec.biomatcher.comp.bioevent.dataAccess.BiometricEventDao;
import com.nec.biomatcher.comp.bioevent.exception.BiometricEventServiceException;
import com.nec.biomatcher.comp.common.query.HbmCriteriaBuilder;
import com.nec.biomatcher.comp.entities.dataAccess.BiometricEventInfo;
import com.nec.biomatcher.core.framework.common.PFLogger;
import com.nec.biomatcher.core.framework.common.pagination.OrderedColumn;
import com.nec.biomatcher.core.framework.common.pagination.PageRequest;
import com.nec.biomatcher.core.framework.common.pagination.PageResult;
import com.nec.biomatcher.core.framework.dataAccess.AbstractHibernateDao;
import com.nec.biomatcher.core.framework.dataAccess.DaoException;
import com.nec.biomatcher.core.framework.dataAccess.HibernateDaoException;
import com.nec.biomatcher.spec.transfer.event.BiometricEventPhase;
import com.nec.biomatcher.spec.transfer.event.BiometricEventStatus;

/**
 * The Class BiometricEventHibernateImpl.
 */
public class BiometricEventHibernateImpl extends AbstractHibernateDao implements BiometricEventDao {
	private static final Logger logger = Logger.getLogger(BiometricEventHibernateImpl.class);

	public int deleteBiometricEventInfo(Long biometricId) throws DaoException {
		try {
			String hql = "delete from BiometricEventInfo where biometricId = :biometricId";

			int deletedCount = this.currentSession().createQuery(hql).setLong("biometricId", biometricId)
					.executeUpdate();

			return deletedCount;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	/**
	 * Gets the biometric event info for update.
	 *
	 * @param externalId
	 *            the external id
	 * @param eventId
	 *            the event id
	 * @param binId
	 *            the bin id
	 * @return the biometric event info for update
	 * @throws DaoException
	 *             the dao exception
	 */
	public BiometricEventInfo getBiometricEventInfoForUpdate(String externalId, String eventId, Integer binId)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));
			criteria.add(Restrictions.eq("eventId", eventId));
			criteria.add(Restrictions.eq("binId", binId));
			criteria.addOrder(Order.desc("createDateTime"));

			return getEntityForUpdate(criteria);
		} catch (DataAccessException ex) {
			throw new DaoException(ex);
		}
	}

	public BiometricEventInfo getBiometricEventInfo(String externalId, String eventId, Integer binId)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));
			criteria.add(Restrictions.eq("eventId", eventId));
			criteria.add(Restrictions.eq("binId", binId));
			criteria.addOrder(Order.desc("createDateTime"));

			return getEntity(criteria);
		} catch (DataAccessException ex) {
			throw new DaoException(ex);
		}
	}

	public BiometricEventInfo getLatestBiometricEventInfoByExternalId(String externalId, Integer binId)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));
			criteria.add(Restrictions.eq("binId", binId));
			criteria.addOrder(Order.desc("createDateTime"));

			return getEntity(criteria);
		} catch (DataAccessException ex) {
			throw new DaoException(ex);
		}
	}

	public BiometricEventInfo getBiometricEventInfoForUpdate(String externalId, String eventId, Integer binId,
			BiometricEventStatus status) throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));
			criteria.add(Restrictions.eq("binId", binId));

			if (StringUtils.isNotBlank(eventId)) {
				criteria.add(Restrictions.eq("eventId", eventId));
			}

			if (status != null) {
				criteria.add(Restrictions.eq("status", status));
			}

			// criteria.addOrder(Order.desc("createDateTime"));

			return getEntityForUpdate(criteria);
		} catch (DataAccessException ex) {
			throw new DaoException(ex);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoList(String externalId, String eventId, Integer binId)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));

			if (StringUtils.isNotBlank(eventId)) {
				criteria.add(Restrictions.eq("eventId", eventId));
			}

			if (binId != null) {
				criteria.add(Restrictions.eq("binId", binId));
			}

			criteria.addOrder(Order.desc("createDateTime"));

			return getEntityList(criteria);
		} catch (DataAccessException ex) {
			throw new DaoException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	public List<BiometricEventInfo> getBiometricEventInfoListByAnyForUpdate(String externalId, String eventId,
			Collection<Integer> binIds) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));

			if (StringUtils.isNotBlank(eventId)) {
				criteria.add(Restrictions.eq("eventId", eventId));
			}

			if (CollectionUtils.isNotEmpty(binIds)) {
				criteria.add(Restrictions.in("binId", binIds));
			}

			criteria.setLockMode(LockMode.PESSIMISTIC_WRITE);

			return criteria.list();
		} catch (DataAccessException ex) {
			throw new HibernateDaoException(ex);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListByAnyForUpdate(String externalId, String eventId,
			Integer binId) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("externalId", externalId));

			if (StringUtils.isNotBlank(eventId)) {
				criteria.add(Restrictions.eq("eventId", eventId));
			}

			if (binId != null) {
				criteria.add(Restrictions.eq("binId", binId));
			}

			criteria.setLockMode(LockMode.PESSIMISTIC_WRITE);

			return criteria.list();
		} catch (DataAccessException ex) {
			throw new HibernateDaoException(ex);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListByBinId(Integer binId, Long dataVersion, int maxRecords)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("binId", binId));
			criteria.add(Restrictions.gt("dataVersion", dataVersion));
			criteria.addOrder(Order.asc("dataVersion"));

			return getEntityList(criteria, -1, maxRecords);
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListForVersioning(Integer segmentId, int maxRecords)
			throws DaoException {
		try {

			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId)).add(Restrictions.eq("dataVersion", -1L))
					.add(Restrictions.eq("phase", BiometricEventPhase.PENDING_SYNC))
					.addOrder(Order.asc("updateDateTime"))
					// .setLockMode(LockMode.PESSIMISTIC_WRITE)
					.setReadOnly(true).setMaxResults(maxRecords);

			return (List<BiometricEventInfo>) criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void updateEventDataVersion(Map<Long, Long> eventVersionMap) throws DaoException {
		try {
			String hql = "update BiometricEventInfo set dataVersion=:dataVersion, updateDateTime=:updateDateTime where biometricId = :biometricId";
			Date updateDateTime = new Date();
			Session session = this.currentSession();

			for (Entry<Long, Long> entry : eventVersionMap.entrySet()) {
				// TODO: Need to see if we can optimize by creating the query
				// once and reuse the query object
				session.createQuery(hql).setLong("biometricId", entry.getKey()).setLong("dataVersion", entry.getValue())
						.setTimestamp("updateDateTime", updateDateTime).executeUpdate();
			}
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void updateEventDataVersion(Map<Long, Long> eventVersionMap, BiometricEventPhase biometricEventPhase)
			throws DaoException {
		try {
			String hql = "update BiometricEventInfo set dataVersion=:dataVersion, phase=:phase, updateDateTime=:updateDateTime where biometricId = :biometricId";
			Date updateDateTime = new Date();
			Session session = this.currentSession();

			for (Entry<Long, Long> entry : eventVersionMap.entrySet()) {
				// TODO: Need to see if we can optimize by creating the query
				// once and reuse the query object
				session.createQuery(hql).setLong("biometricId", entry.getKey()).setLong("dataVersion", entry.getValue())
						.setParameter("phase", biometricEventPhase).setTimestamp("updateDateTime", updateDateTime)
						.executeUpdate();
			}
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForSync(Integer segmentId, Long dataVersion,
			int maxRecords) throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("assignedSegmentId", segmentId));
			criteria.add(Restrictions.gt("dataVersion", dataVersion));
			// criteria.add(Restrictions.eq("phase",
			// BiometricEventPhase.PENDING_SYNC));
			criteria.addOrder(Order.asc("dataVersion"));

			return getEntityList(criteria, -1, maxRecords);
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForRemoteSync(Integer segmentId,
			Long dataVersion, String siteId, BiometricEventStatus biometricEventStatus, int maxRecords)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.eq("assignedSegmentId", segmentId));
			criteria.add(Restrictions.gt("dataVersion", dataVersion));
			criteria.add(Restrictions.eq("siteId", siteId));

			if (biometricEventStatus != null) {
				criteria.add(Restrictions.gt("status", biometricEventStatus));
			}

			criteria.addOrder(Order.asc("dataVersion"));

			return getEntityList(criteria, -1, maxRecords);
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			int maxRecords) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.gt("biometricId", afterBiometricId)).add(Restrictions.gt("dataVersion", -1L))
					.add(Restrictions.eq("status", BiometricEventStatus.ACTIVE)).addOrder(Order.asc("biometricId"));

			criteria.setMaxResults(maxRecords);

			return criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.between("dataVersion", fromDataVersion, toDataVersion))
					.add(Restrictions.in("phase",
							Lists.newArrayList(BiometricEventPhase.PENDING_SYNC, BiometricEventPhase.SYNC_COMPLETED)))
					.addOrder(Order.asc("dataVersion"));

			return (List<BiometricEventInfo>) criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion, BiometricEventPhase phase, int maxRecords) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.between("dataVersion", fromDataVersion, toDataVersion))
					.add(Restrictions.eq("phase", phase)).addOrder(Order.asc("dataVersion")).setMaxResults(maxRecords);

			return (List<BiometricEventInfo>) criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionList(Integer segmentId,
			List<Long> dataVersionList, BiometricEventPhase phase) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.in("dataVersion", dataVersionList)).add(Restrictions.eq("phase", phase))
					.addOrder(Order.asc("dataVersion"));

			return (List<BiometricEventInfo>) criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListForSegmentation(Integer binId, int maxRecords)
			throws DaoException {
		try {
			DetachedCriteria criteria = DetachedCriteria.forClass(BiometricEventInfo.class);
			criteria.add(Restrictions.isNull("dataVersion"));
			criteria.add(Restrictions.eq("binId", binId));
			criteria.addOrder(Order.asc("biometricId"));

			return getEntityList(criteria, -1, maxRecords);
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public int updateCurrentBiometricId(Integer segmentId, Long currentBiometricId) throws DaoException {
		try {
			String hql = "update BiometricIdInfo set currentBiometricId=:currentBiometricId where segmentId=:segmentId";
			int updateCount = this.currentSession().createQuery(hql).setInteger("segmentId", segmentId)
					.setLong("currentBiometricId", currentBiometricId).executeUpdate();

			return updateCount;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public int deleteBiometricIdDetailInfoList(List<Long> biometricIdList) throws DaoException {
		try {
			int updateCount = 0;
			String hql = "delete from BiometricIdDetailInfo where biometricId in (:biometricIdList)";
			Session session = this.currentSession();

			for (List<Long> subBiometricIdList : Lists.partition(biometricIdList, 1000)) {
				updateCount += session.createQuery(hql).setParameterList("biometricIdList", subBiometricIdList)
						.executeUpdate();
			}

			return updateCount;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public Map<Integer, Long> getCurrentEventSegmentVersionMap() throws DaoException {
		try {
			List list = this.currentSession()
					.createCriteria(BiometricEventInfo.class).setProjection(Projections.projectionList()
							.add(Projections.groupProperty("assignedSegmentId")).add(Projections.max("dataVersion")))
					.list();

			if (list.size() == 0) {
				return Collections.EMPTY_MAP;
			}

			Map<Integer, Long> segmentIdVersionMap = new HashMap<Integer, Long>();
			Iterator iter = list.iterator();
			while (iter.hasNext()) {
				Object[] obj = (Object[]) iter.next();
				for (int i = 0; i < obj.length; i++) {
					if (obj.length == 2 && obj[0] != null && obj[1] != null) {
						segmentIdVersionMap.put(((Number) obj[0]).intValue(), ((Number) obj[1]).longValue());
					}
				}
			}

			return segmentIdVersionMap;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public Long getMaxEventSegmentVersionBySegmentId(Integer segmentId) throws DaoException {
		try {
			Number maxSegmentVersion = (Number) this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId)).setProjection(Projections.max("dataVersion"))
					.uniqueResult();

			return maxSegmentVersion != null ? maxSegmentVersion.longValue() : null;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public int resetPendingSyncEventDataVersion(Integer segmentId, Long lastCheckSegmentVersionId,
			long latestMatcherNodeSegmentVersionId) throws DaoException {
		try {
			long startDataVersion = Math.max(0, lastCheckSegmentVersionId);
			long endDataVersion = latestMatcherNodeSegmentVersionId;

			String hql = "update BiometricEventInfo set dataVersion=-1 where assignedSegmentId=:segmentId and (dataVersion between :startDataVersion and :endDataVersion) and phase=:currentPhase";
			int updateCount = this.currentSession().createQuery(hql).setInteger("segmentId", segmentId)
					.setLong("startDataVersion", startDataVersion).setLong("endDataVersion", endDataVersion)
					.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC).executeUpdate();

			return updateCount;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void notifySyncCompleted(Integer segmentId, long lastSyncVersion) throws DaoException {
		int updateCount = 0;
		PFLogger.start();
		try {
			if (lastSyncVersion <= -1) {
				return;
			}

			String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where assignedSegmentId=:segmentId and phase=:currentPhase and dataVersion <=:lastSyncVersion and dataVersion>-1";
			updateCount = this.currentSession().createQuery(hql).setInteger("segmentId", segmentId)
					.setLong("lastSyncVersion", lastSyncVersion)
					.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC)
					.setParameter("newPhase", BiometricEventPhase.SYNC_COMPLETED)
					.setTimestamp("updateDateTime", new Date()).executeUpdate();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		} finally {
			PFLogger.end(500, "After updating phase to SYNC_COMPLETED for segmentId : " + segmentId
					+ ", lastSyncVersion: " + lastSyncVersion + ", updateCount: " + updateCount);
		}
	}

	public void notifySyncCompleted(Integer segmentId, RangeSet<Long> segmentVersionRangeSet,
			Set<Long> corruptedSegmentVersionIdList) throws DaoException {
		try {
			if (corruptedSegmentVersionIdList != null && corruptedSegmentVersionIdList.size() > 0) {
				List<List<Long>> corruptedSegmentVersionIdPartList = Lists
						.partition(new ArrayList<>(corruptedSegmentVersionIdList), 1000);
				for (List<Long> corruptedSegmentVersionIdPart : corruptedSegmentVersionIdPartList) {
					if (corruptedSegmentVersionIdPart.size() > 0) {
						String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where assignedSegmentId=:segmentId and dataVersion in (:dataVersion) and phase=:currentPhase";
						int updateCount = this.currentSession().createQuery(hql).setInteger("segmentId", segmentId)
								.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC)
								.setParameter("newPhase", BiometricEventPhase.SYNC_ERROR)
								.setTimestamp("updateDateTime", new Date())
								.setParameterList("dataVersion", corruptedSegmentVersionIdPart).executeUpdate();
						logger.trace(
								"In notifySyncCompleted: After updating corrupted segment version records for segmentId : "
										+ segmentId + ", updateCount: " + updateCount);
					}
				}
			}

			if (segmentVersionRangeSet != null) {
				for (Range<Long> segmentVersionRange : segmentVersionRangeSet.asRanges()) {
					if (segmentVersionRange.hasLowerBound() == false || segmentVersionRange.hasUpperBound() == false) {
						continue;
					}
					long startDataVersion = segmentVersionRange.lowerEndpoint();

					// When getting range with discreet longs, upperbound value
					// is returned with upperbound+1, so we need to decrement it
					// while using it
					long endDataVersion = segmentVersionRange.upperEndpoint() - 1;

					if (startDataVersion <= endDataVersion) {
						String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where assignedSegmentId=:segmentId and (dataVersion between :startDataVersion and :endDataVersion) and phase=:currentPhase";
						int updateCount = this.currentSession().createQuery(hql).setInteger("segmentId", segmentId)
								.setLong("startDataVersion", startDataVersion).setLong("endDataVersion", endDataVersion)
								.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC)
								.setParameter("newPhase", BiometricEventPhase.SYNC_COMPLETED)
								.setTimestamp("updateDateTime", new Date()).executeUpdate();
						logger.trace("In notifySyncCompleted: After updating phase to SYNC_COMPLETED for segmentId : "
								+ segmentId + ", startDataVersion: " + startDataVersion + ", endDataVersion: "
								+ endDataVersion + ", updateCount: " + updateCount);
					}
				}
			}
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public int notifySyncCompleted(Long biometricId, Long dataVersion, BiometricEventPhase currentPhase,
			BiometricEventPhase newPhase) throws DaoException {
		try {
			String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where biometricId=:biometricId and dataVersion=:dataVersion and phase=:currentPhase";

			int updateCount = this.currentSession().createQuery(hql).setLong("biometricId", biometricId)
					.setLong("dataVersion", dataVersion).setParameter("currentPhase", currentPhase)
					.setParameter("newPhase", newPhase).setTimestamp("updateDateTime", new Date()).executeUpdate();

			if (logger.isTraceEnabled()) {
				logger.trace(
						"In notifySyncCompleted: After updating to newPhase: " + newPhase.name() + " for biometricId: "
								+ biometricId + ", dataVersion: " + dataVersion + ", updateCount: " + updateCount);
			}

			return updateCount;
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void notifyCorruptedBiometricIdList(List<Long> corruptedBiometricIdList) throws DaoException {
		try {
			if (corruptedBiometricIdList.size() > 0) {
				List<List<Long>> corruptedBiometricIdPartList = Lists
						.partition(new ArrayList<>(corruptedBiometricIdList), 1000);
				for (List<Long> corruptedBiometricIdPart : corruptedBiometricIdPartList) {
					if (corruptedBiometricIdPart.size() > 0) {
						String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where biometricId in (:biometricId) and phase=:currentPhase";
						int updateCount = this.currentSession().createQuery(hql)
								.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC)
								.setParameter("newPhase", BiometricEventPhase.SYNC_ERROR)
								.setTimestamp("updateDateTime", new Date())
								.setParameterList("biometricId", corruptedBiometricIdPart).executeUpdate();
						logger.trace("In notifySyncCompleted: After updating corrupted biometricIds, updateCount: "
								+ updateCount);
					}
				}
			}
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void notifyCorruptedEventsByDataVersion(Integer segmentId, List<Long> corruptedDataVersionIdList)
			throws DaoException {
		try {
			if (corruptedDataVersionIdList.size() > 0) {
				List<List<Long>> corruptedDataVersionIdPartList = Lists
						.partition(new ArrayList<>(corruptedDataVersionIdList), 1000);
				for (List<Long> corruptedDataVersionIdPart : corruptedDataVersionIdPartList) {
					if (corruptedDataVersionIdPart.size() > 0) {
						String hql = "update BiometricEventInfo set phase=:newPhase, segmentSyncDateTime=:updateDateTime, updateDateTime=:updateDateTime where assignedSegmentId=:assignedSegmentId and dataVersion in (:dataVersion) and phase=:currentPhase";
						int updateCount = this.currentSession().createQuery(hql)
								.setInteger("assignedSegmentId", segmentId)
								.setParameter("currentPhase", BiometricEventPhase.PENDING_SYNC)
								.setParameter("newPhase", BiometricEventPhase.SYNC_ERROR)
								.setTimestamp("updateDateTime", new Date())
								.setParameterList("dataVersion", corruptedDataVersionIdPart).executeUpdate();
						logger.trace("In notifySyncCompleted: After updating corrupted biometricIds, updateCount: "
								+ updateCount);
					}
				}
			}
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public void releaseOldBiometricIdsByAcquireHost(String acquireHost) throws DaoException {
		try {
			String hql = "update BiometricIdDetailInfo set reuseFlag=:newReuseFlag, acquireHost=:newAcquireHost, acquireDateTime=:newAcquireDateTime where acquireHost=:acquireHost and reuseFlag=:reuseFlag";
			int updateCount = this.currentSession().createQuery(hql).setBoolean("newReuseFlag", Boolean.TRUE)
					.setString("newAcquireHost", null).setTimestamp("newAcquireDateTime", null)
					.setString("acquireHost", acquireHost).setBoolean("reuseFlag", Boolean.FALSE).executeUpdate();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public PageResult<BiometricEventInfo> getBiometricEventInfoList(BiometricEventCriteria biometricEventCriteria,
			PageRequest pageRequest) throws DaoException {
		try {
//			Number count = 0;
//			if (pageRequest.isCalculateRecordCount()) {
//				Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class);
//
//				HbmCriteriaBuilder hbmCriteria = new HbmCriteriaBuilder(criteria);
//
//				hbmCriteria.addCriteria("biometricId", biometricEventCriteria.getBiometricId())
//						.addCriteria("externalId", biometricEventCriteria.getExternalId())
//						.addCriteria("eventId", biometricEventCriteria.getEventId())
//						.addCriteria("binId", biometricEventCriteria.getBinId())
//						.addCriteria("status", biometricEventCriteria.getStatus())
//						.addCriteria("phase", biometricEventCriteria.getPhase())
//						.addCriteria("assignedSegmentId", biometricEventCriteria.getAssignedSegmentId())
//						.addCriteria("dataVersion", biometricEventCriteria.getDataVersion())
//						.addCriteria("updateDateTime", biometricEventCriteria.getUpdateDateTime());
//
//				count = (Number) criteria.setProjection(Projections.rowCount()).uniqueResult();
//				
//			}

			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class);

			HbmCriteriaBuilder hbmCriteria = new HbmCriteriaBuilder(criteria);
			hbmCriteria.addCriteria("biometricId", biometricEventCriteria.getBiometricId())
					.addCriteria("externalId", biometricEventCriteria.getExternalId())
					.addCriteria("eventId", biometricEventCriteria.getEventId())
					.addCriteria("binId", biometricEventCriteria.getBinId())
					.addCriteria("status", biometricEventCriteria.getStatus())
					.addCriteria("phase", biometricEventCriteria.getPhase())
					.addCriteria("assignedSegmentId", biometricEventCriteria.getAssignedSegmentId())
					.addCriteria("dataVersion", biometricEventCriteria.getDataVersion())
					.addCriteria("updateDateTime", biometricEventCriteria.getUpdateDateTime());

			if (pageRequest.getOrderedColumns() != null) {
				for (OrderedColumn column : pageRequest.getOrderedColumns()) {
					criteria.addOrder(column.isAscending() ? Order.asc(column.getColumnName())
							: Order.desc(column.getColumnName()));
				}
			}
			criteria.setFirstResult(pageRequest.getFirstRowIndex())
			.setMaxResults(pageRequest.getMaxRecords());
			
			@SuppressWarnings("unchecked")
			List<BiometricEventInfo> resultList = criteria.list();	

			PageResult<BiometricEventInfo> pageResult = new PageResult<>();
			pageResult.setTotalRecords(resultList.size());
			pageResult.setResultList(resultList);

			return pageResult;

		} catch (HibernateException e) {
			throw new DaoException(e);
		}
	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, int maxRecords)
			throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.eq("status", BiometricEventStatus.ACTIVE)).addOrder(Order.asc("biometricId"))
					.setMaxResults(maxRecords).setFlushMode(FlushMode.MANUAL);

			return criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			Long afterDataVersion, int maxRecords) throws DaoException {
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)
					.add(Restrictions.eq("assignedSegmentId", segmentId))
					.add(Restrictions.gt("biometricId", afterBiometricId))
					.add(Restrictions.gt("dataVersion", afterDataVersion))
					.add(Restrictions.eq("status", BiometricEventStatus.ACTIVE)).addOrder(Order.asc("biometricId"))
					.setMaxResults(maxRecords).setFlushMode(FlushMode.MANUAL);

			return criteria.list();
		} catch (Throwable th) {
			throw new HibernateDaoException(th);
		}
	}
	
	@Override
	public PageResult<BiometricEventInfo> getBiometricEventInfoListByStatus(BiometricEventStatus status, PageRequest pageRequest)
			throws BiometricEventServiceException {
		int maxCount = pageRequest.getMaxRecords();
		try {
			Criteria criteria = this.currentSession().createCriteria(BiometricEventInfo.class)					
					.add(Restrictions.eq("status", BiometricEventStatus.ACTIVE))
					.setFirstResult(pageRequest.getFirstRowIndex())
					.addOrder(Order.asc(pageRequest.getSortorder()))
					.setMaxResults(maxCount).setFlushMode(FlushMode.MANUAL);
			List<BiometricEventInfo> resultList = criteria.list();
			PageResult<BiometricEventInfo> pageResult = new PageResult<>();
			pageResult.setTotalRecords(resultList.size());
			pageResult.setResultList(resultList);
			return pageResult;
			
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getBiometricEventInfoList: " + th.getMessage(), th);
		}
	}


}
