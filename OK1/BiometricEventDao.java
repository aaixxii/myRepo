package com.nec.biomatcher.comp.bioevent.dataAccess;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.RangeSet;
import com.nec.biomatcher.comp.bioevent.exception.BiometricEventServiceException;
import com.nec.biomatcher.comp.entities.dataAccess.BiometricEventInfo;
import com.nec.biomatcher.core.framework.common.pagination.PageRequest;
import com.nec.biomatcher.core.framework.common.pagination.PageResult;
import com.nec.biomatcher.core.framework.dataAccess.DaoException;
import com.nec.biomatcher.core.framework.dataAccess.HibernateDao;
import com.nec.biomatcher.spec.transfer.event.BiometricEventPhase;
import com.nec.biomatcher.spec.transfer.event.BiometricEventStatus;

/**
 * The Interface BiometricEventDao.
 */
public interface BiometricEventDao extends HibernateDao {

	public int deleteBiometricEventInfo(Long biometricId) throws DaoException;

	public BiometricEventInfo getBiometricEventInfo(String externalId, String eventId, Integer binId)
			throws DaoException;

	public BiometricEventInfo getLatestBiometricEventInfoByExternalId(String externalId, Integer binId)
			throws DaoException;

	public BiometricEventInfo getBiometricEventInfoForUpdate(String externalId, String eventId, Integer binId)
			throws DaoException;

	public BiometricEventInfo getBiometricEventInfoForUpdate(String externalId, String eventId, Integer binId,
			BiometricEventStatus status) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoList(String externalId, String eventId, Integer binId)
			throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListByAnyForUpdate(String externalId, String eventId,
			Collection<Integer> binIds) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListByAnyForUpdate(String externalId, String eventId,
			Integer binId) throws DaoException;

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, int maxRecords)
			throws DaoException;

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			Long afterDataVersion, int maxRecords) throws DaoException;

	/**
	 * Gets the biometric event info list by bin id.
	 *
	 * @param binId
	 *            the bin id
	 * @param dataVersion
	 *            the data version
	 * @param maxRecords
	 *            the max records
	 * @return the biometric event info list by bin id
	 * @throws DaoException
	 *             the dao exception
	 */
	public List<BiometricEventInfo> getBiometricEventInfoListByBinId(Integer binId, Long dataVersion, int maxRecords)
			throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListForVersioning(Integer segmentId, int maxRecords)
			throws DaoException;

	public void updateEventDataVersion(Map<Long, Long> eventVersionMap) throws DaoException;

	public void updateEventDataVersion(Map<Long, Long> eventVersionMap, BiometricEventPhase biometricEventPhase)
			throws DaoException;

	/**
	 * Gets the biometric event info list by segment id.
	 *
	 * @param segmentId
	 *            the segment id
	 * @param dataVersion
	 *            the data version
	 * @param maxRecords
	 *            the max records
	 * @return the biometric event info list by segment id
	 * @throws DaoException
	 *             the dao exception
	 */
	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForSync(Integer segmentId, Long dataVersion,
			int maxRecords) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForRemoteSync(Integer segmentId,
			Long dataVersion, String siteId, BiometricEventStatus biometricEventStatus, int maxRecords)
			throws DaoException;

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			int maxRecords) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion, BiometricEventPhase phase, int maxRecords) throws DaoException;

	/**
	 * Gets the biometric event info list for segmentation.
	 *
	 * @param binId
	 *            the bin id
	 * @param maxRecords
	 *            the max records
	 * @return the biometric event info list for segmentation
	 * @throws DaoException
	 *             the dao exception
	 */
	public List<BiometricEventInfo> getBiometricEventInfoListForSegmentation(Integer binId, int maxRecords)
			throws DaoException;

	public int updateCurrentBiometricId(Integer segmentId, Long currentBiometricId) throws DaoException;

	public int deleteBiometricIdDetailInfoList(List<Long> biometricIdList) throws DaoException;

	public Map<Integer, Long> getCurrentEventSegmentVersionMap() throws DaoException;

	public Long getMaxEventSegmentVersionBySegmentId(Integer segmentId) throws DaoException;

	public int resetPendingSyncEventDataVersion(Integer segmentId, Long lastCheckSegmentVersionId,
			long latestMatcherNodeSegmentVersionId) throws DaoException;

	public void notifySyncCompleted(Integer segmentId, long lastSyncVersion) throws DaoException;

	public void notifySyncCompleted(Integer segmentId, RangeSet<Long> segmentVersionRangeSet,
			Set<Long> corruptedSegmentVersionIdList) throws DaoException;

	public int notifySyncCompleted(Long biometricId, Long dataVersion, BiometricEventPhase currentPhase,
			BiometricEventPhase newPhase) throws DaoException;

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionList(Integer segmentId,
			List<Long> dataVersionList, BiometricEventPhase phase) throws DaoException;

	public void notifyCorruptedBiometricIdList(List<Long> corruptedBiometricIdList) throws DaoException;

	public void notifyCorruptedEventsByDataVersion(Integer segmentId, List<Long> corruptedDataVersionIdList)
			throws DaoException;

	public void releaseOldBiometricIdsByAcquireHost(String acquireHost) throws DaoException;

	public PageResult<BiometricEventInfo> getBiometricEventInfoList(BiometricEventCriteria biometricEventCriteria,
			PageRequest pageRequest) throws DaoException;
	
	public PageResult<BiometricEventInfo> getBiometricEventInfoListByStatus(BiometricEventStatus status, PageRequest pageRequest)
			throws BiometricEventServiceException;
}
