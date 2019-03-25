package com.nec.biomatcher.comp.bioevent.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.springframework.beans.factory.InitializingBean;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.nec.biomatcher.comp.bioevent.BiometricEventService;
import com.nec.biomatcher.comp.bioevent.dataAccess.BiometricEventCriteria;
import com.nec.biomatcher.comp.bioevent.dataAccess.BiometricEventDao;
import com.nec.biomatcher.comp.bioevent.exception.BiometricEventServiceException;
import com.nec.biomatcher.comp.common.locking.BioLockingService;
import com.nec.biomatcher.comp.common.parameter.BioParameterService;
import com.nec.biomatcher.comp.config.BioMatcherConfigService;
import com.nec.biomatcher.comp.entities.dataAccess.BioMatcherSegmentInfo;
import com.nec.biomatcher.comp.entities.dataAccess.BiometricEventInfo;
import com.nec.biomatcher.comp.entities.dataAccess.BiometricIdDetailInfo;
import com.nec.biomatcher.comp.manager.dataAccess.BioMatchManagerDao;
import com.nec.biomatcher.comp.template.packing.model.MeghaEventHeader;
import com.nec.biomatcher.comp.template.packing.model.MeghaTemplateHeader;
import com.nec.biomatcher.comp.template.packing.util.EventKey;
import com.nec.biomatcher.comp.template.packing.util.MeghaTemplateParser;
import com.nec.biomatcher.comp.template.packing.util.MeghaTemplateUtil;
import com.nec.biomatcher.comp.template.storage.TemplateDataService;
import com.nec.biomatcher.comp.util.BiometricIdGenerator;
import com.nec.biomatcher.comp.util.ConcurrentSequenceGenerator;
import com.nec.biomatcher.core.framework.common.GsonSerializer;
import com.nec.biomatcher.core.framework.common.HoldFlagUtil;
import com.nec.biomatcher.core.framework.common.PFLogger;
import com.nec.biomatcher.core.framework.common.StringUtil;
import com.nec.biomatcher.core.framework.common.pagination.PageRequest;
import com.nec.biomatcher.core.framework.common.pagination.PageResult;
import com.nec.biomatcher.core.framework.dataAccess.DaoException;
import com.nec.biomatcher.spec.transfer.biometrics.DeleteBiometricEventDto;
import com.nec.biomatcher.spec.transfer.biometrics.InsertBiometricEventDto;
import com.nec.biomatcher.spec.transfer.biometrics.UpdateBiometricEventUserFlagDto;
import com.nec.biomatcher.spec.transfer.event.BiometricEventPhase;
import com.nec.biomatcher.spec.transfer.event.BiometricEventStatus;
import com.nec.biomatcher.spec.transfer.model.InsertTemplateInfo;
import com.nec.biomatcher.spec.transfer.model.UpdateUserFlagMode;

/**
 * The Class BiometricEventServiceImpl.
 */
public class BiometricEventServiceImpl implements BiometricEventService, InitializingBean {
	private static final Logger logger = Logger.getLogger(BiometricEventServiceImpl.class);

	private static final int EXTERNAL_ID_STRIPED_LOCK_COUNT = 300;

	private static final String EXTERNAL_ID_STRIPED_LOCK_PREFIX = "EXTERNAL_ID_STRIPED_LOCK";

	private static final Set<String> templateHeaderFields = Collections
			.unmodifiableSet(Sets.newHashSet("gender", "yob", "race", "userFlags", "regionFlags"));

	/** The biometric event dao. */
	private BiometricEventDao biometricEventDao;

	private BioMatchManagerDao bioMatchManagerDao;

	private BioParameterService bioParameterService;

	/** The biometric id generator. */
	private BiometricIdGenerator biometricIdGenerator;

	private TemplateDataService templateDataService;

	/** The concurrent sequence generator. */
	private ConcurrentSequenceGenerator concurrentSequenceGenerator;

	private BioMatcherConfigService bioMatcherConfigService;

	private BioLockingService bioLockingService;

	public List<BiometricEventInfo> insertEvent(InsertBiometricEventDto insertBiometricEventDto, String sourceSiteId)
			throws BiometricEventServiceException {
		NDC.push(insertBiometricEventDto.getExternalId());
		try {

			assert StringUtils.length(insertBiometricEventDto.getExternalId()) > 0 : "Invalid externalId";

			assert StringUtils.length(insertBiometricEventDto
					.getExternalId()) <= MeghaTemplateHeader.EXTERNAL_ID_MAX_SIZE : "Invalid externalId";

			assert StringUtils.length(
					insertBiometricEventDto.getEventId()) <= MeghaEventHeader.EVENT_ID_MAX_SIZE : "Invalid eventId";

			if (CollectionUtils.isEmpty(insertBiometricEventDto.getInsertTemplateInfoList())) {
				throw new BiometricEventServiceException(
						"InsertTemplateInfoList is not set in insert request for ExternalId: "
								+ insertBiometricEventDto.getExternalId());
			}

			final Map<Integer, MeghaTemplateParser> binIdMeghaTemplateParserMap = bioMatcherConfigService
					.getBinIdMeghaTemplateParserMap();

			lockOnExternalId(insertBiometricEventDto.getExternalId());

			List<BiometricEventInfo> modifiedBiometricEventInfoList = new ArrayList<>();

			if (Boolean.TRUE.equals(insertBiometricEventDto.getUpdateFlag())) {
				for (InsertTemplateInfo insertTemplateInfo : insertBiometricEventDto.getInsertTemplateInfoList()) {
					if (insertTemplateInfo.getBinId() == null) {
						throw new BiometricEventServiceException(
								"BinId is not set in InsertTemplateInfo request for ExternalId: "
										+ insertBiometricEventDto.getExternalId() + ", EventId: "
										+ insertBiometricEventDto.getEventId());
					}

					MeghaTemplateParser meghaTemplateParser = binIdMeghaTemplateParserMap
							.get(insertTemplateInfo.getBinId());
					if (meghaTemplateParser == null) {
						throw new Exception(
								"Cannot get MeghaTemplateParser for binId: " + insertTemplateInfo.getBinId());
					}

					if (meghaTemplateParser.getMaxEventCount() == 1) {
						logger.debug("Update flag is true, so will be deleting the events with ExternalId: "
								+ insertBiometricEventDto.getExternalId() + ", EventId: "
								+ insertBiometricEventDto.getEventId() + ", binId: " + insertTemplateInfo.getBinId());

						DeleteBiometricEventDto deleteBiometricEventDto = new DeleteBiometricEventDto();
						deleteBiometricEventDto.setExternalId(insertBiometricEventDto.getExternalId());
						deleteBiometricEventDto.setEventId(insertBiometricEventDto.getEventId());
						deleteBiometricEventDto.setBinIdSet(Sets.newHashSet(insertTemplateInfo.getBinId()));

						List<BiometricEventInfo> deletedBiometricEventInfoList = deleteEventInternal(
								deleteBiometricEventDto, sourceSiteId);
						logger.debug("After deleting biometricEvents: " + deletedBiometricEventInfoList.size());

						modifiedBiometricEventInfoList.addAll(deletedBiometricEventInfoList);
					}
				}
			}

			String localSiteId = bioParameterService.getParameterValue("LOCAL_SITE_ID", "DEFAULT", "DEFAULT");

			List<Long> acquiredBiometricIdDetailInfoList = new ArrayList<>();
			for (InsertTemplateInfo insertTemplateInfo : insertBiometricEventDto.getInsertTemplateInfoList()) {
				if (insertTemplateInfo.getBinId() == null) {
					throw new BiometricEventServiceException(
							"BinId is not set in InsertTemplateInfo request for ExternalId: "
									+ insertBiometricEventDto.getExternalId() + ", EventId: "
									+ insertBiometricEventDto.getEventId());
				}

				if (insertTemplateInfo.getTemplateData() == null || insertTemplateInfo.getTemplateData().length == 0) {
					throw new BiometricEventServiceException(
							"TemplateData is not set in InsertTemplateInfo request for ExternalId: "
									+ insertBiometricEventDto.getExternalId() + ", EventId: "
									+ insertBiometricEventDto.getEventId() + ", BinId: "
									+ insertTemplateInfo.getBinId());
				}

				try {
					final MeghaTemplateParser meghaTemplateParser = binIdMeghaTemplateParserMap
							.get(insertTemplateInfo.getBinId());

					ByteBuffer inputTemplateBuffer = meghaTemplateParser
							.validateAndPadTemplateData(insertTemplateInfo.getTemplateData());
					inputTemplateBuffer.order(MeghaTemplateUtil.DEFAULT_BYTE_ORDER);

					BiometricEventInfo biometricEventInfo = null;

					if (meghaTemplateParser.getMaxEventCount() > 1) {
						biometricEventInfo = biometricEventDao.getBiometricEventInfoForUpdate(
								insertBiometricEventDto.getExternalId(), null, insertTemplateInfo.getBinId(),
								BiometricEventStatus.ACTIVE);
					}

					if (biometricEventInfo == null) {
						biometricEventInfo = new BiometricEventInfo();
						biometricEventInfo.setExternalId(insertBiometricEventDto.getExternalId());
						biometricEventInfo.setBinId(insertTemplateInfo.getBinId());
						biometricEventInfo.setStatus(BiometricEventStatus.ACTIVE);
						biometricEventInfo.setCreateDateTime(new Date());

						if (meghaTemplateParser.getMaxEventCount() == 1
								&& StringUtils.isNotBlank(insertBiometricEventDto.getEventId())) {
							biometricEventInfo.setEventId(insertBiometricEventDto.getEventId());
						}

						BiometricIdDetailInfo biometricIdDetailInfo = biometricIdGenerator
								.acquireBiometricId(insertTemplateInfo.getBinId());
						biometricEventInfo.setBiometricId(biometricIdDetailInfo.getBiometricId());
						biometricEventInfo.setAssignedSegmentId(biometricIdDetailInfo.getSegmentId());

						logger.info("In insertEvent, inserting biometricEventInfo: "
								+ biometricEventInfo.getBiometricId() + ", externalId: "
								+ insertBiometricEventDto.getExternalId() + ", eventId: "
								+ insertBiometricEventDto.getEventId() + ", binId: " + biometricEventInfo.getBinId());

						meghaTemplateParser.updateTemplateTypeCode(inputTemplateBuffer);
						meghaTemplateParser.updateMaxEventCount(inputTemplateBuffer);
						meghaTemplateParser.setBiometricId(biometricEventInfo.getBiometricId(), inputTemplateBuffer);
						meghaTemplateParser.setExternalId(insertBiometricEventDto.getExternalId(), inputTemplateBuffer);

						if (StringUtils.isNotBlank(insertBiometricEventDto.getEventId())) {
							meghaTemplateParser.updateEventId(insertBiometricEventDto.getEventId(),
									inputTemplateBuffer);
						}
						meghaTemplateParser.updateSanity(inputTemplateBuffer);

						byte templateData[] = inputTemplateBuffer.array();

						String templateDataKey = templateDataService.saveTemplateData(biometricEventInfo.getBinId(),
								biometricEventInfo.getBiometricId(), templateData);

						biometricEventInfo.setTemplateSize(templateData.length);
						biometricEventInfo.setTemplateDataKey(templateDataKey);

						biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
						biometricEventInfo.setUpdateDateTime(new Date());
						biometricEventInfo.setDataVersion(-1L);
						biometricEventInfo.setSegmentSyncDateTime(null);
						biometricEventInfo.setSiteId(sourceSiteId);

						long dbAccessStartTimeMilli = System.currentTimeMillis();
						try {
							if (biometricIdDetailInfo.getOldReusedFlag()) {
								biometricEventDao.deleteBiometricEventInfo(biometricEventInfo.getBiometricId());
							}

							biometricEventDao.saveEntity(biometricEventInfo);

							biometricEventDao.flush();
						} finally {
							long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
							if (logger.isTraceEnabled() || dbAccessTimeTakenMilli > 100) {
								logger.info("In insertEvent: TimeTakenMilli: " + dbAccessTimeTakenMilli);
							}
						}

						acquiredBiometricIdDetailInfoList.add(biometricIdDetailInfo.getBiometricId());

						modifiedBiometricEventInfoList.add(biometricEventInfo);
					} else {
						logger.info("In insertEvent, updating biometricEventInfo: "
								+ biometricEventInfo.getBiometricId() + ", externalId: "
								+ insertBiometricEventDto.getExternalId() + ", eventId: "
								+ insertBiometricEventDto.getEventId() + ", binId: " + biometricEventInfo.getBinId());

						byte currentTemplateData[] = templateDataService
								.getTemplateData(biometricEventInfo.getTemplateDataKey());

						if (meghaTemplateParser.getTemplateDataSize() != currentTemplateData.length) {
							throw new BiometricEventServiceException("Current template data size should be : "
									+ meghaTemplateParser.getTemplateDataSize() + " but it is : "
									+ currentTemplateData.length + " for binId : " + biometricEventInfo.getBinId()
									+ ", biometricId: " + biometricEventInfo.getBiometricId() + ", templateDataKey: "
									+ biometricEventInfo.getTemplateDataKey());
						}

						ByteBuffer currentTemplateBuffer = ByteBuffer.wrap(currentTemplateData)
								.order(MeghaTemplateUtil.DEFAULT_BYTE_ORDER);

						Map<EventKey, Integer> currentTemplateEventKeyIndexMap = meghaTemplateParser
								.getEventKeyIndexMap(currentTemplateBuffer);

						if (!StringUtils.equals(localSiteId, sourceSiteId)) {
							// Means data is coming from some other site, so we
							// cannot determine which event to keep and which
							// event to remove,
							// so we will clear current events and add events
							// from source site

							for (Entry<EventKey, Integer> currentTemplateEventIdIndexEntry : currentTemplateEventKeyIndexMap
									.entrySet()) {
								meghaTemplateParser.deleteEventDataByIndex(currentTemplateEventIdIndexEntry.getValue(),
										currentTemplateBuffer);
							}

							currentTemplateEventKeyIndexMap.clear();
						}

						if (StringUtils.isNotBlank(insertBiometricEventDto.getEventId())) {
							meghaTemplateParser.updateEventId(insertBiometricEventDto.getEventId(),
									inputTemplateBuffer);
						}

						Map<EventKey, ByteBuffer> inputEventKeyEventBufferMap = meghaTemplateParser
								.getEventKeyEventBufferMap(inputTemplateBuffer);

						meghaTemplateParser.copyTemplateHeaderFields(templateHeaderFields, inputTemplateBuffer,
								currentTemplateBuffer);

						for (Entry<EventKey, ByteBuffer> inputEventKeyEventBufferEntry : inputEventKeyEventBufferMap
								.entrySet()) {
							EventKey inputEventKey = inputEventKeyEventBufferEntry.getKey();
							ByteBuffer inputEventBuffer = inputEventKeyEventBufferEntry.getValue();

							Integer currentEventIndex = currentTemplateEventKeyIndexMap.get(inputEventKey);

							if (currentEventIndex != null) {
								meghaTemplateParser.setEventDataAtIndex(currentEventIndex, inputEventBuffer,
										currentTemplateBuffer);
							} else {
								Integer nextFreeEventSlotIndex = meghaTemplateParser
										.getNextFreeEventSlotIndex(currentTemplateBuffer);
								if (nextFreeEventSlotIndex == null) {
									throw new BiometricEventServiceException(
											"Unable to set event data to template, no free event slots available, maximum number of allowed events : "
													+ meghaTemplateParser.getMaxEventCount()
													+ ", Current usedEventIndexList: " + meghaTemplateParser
															.getUsedEventSlotIndexList(currentTemplateBuffer));
								}

								meghaTemplateParser.setEventDataAtIndex(nextFreeEventSlotIndex, inputEventBuffer,
										currentTemplateBuffer);
							}
						}

						meghaTemplateParser.updateSanity(currentTemplateBuffer);

						biometricEventInfo.setTemplateSize(currentTemplateData.length);
						biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
						biometricEventInfo.setUpdateDateTime(new Date());
						biometricEventInfo.setDataVersion(-1L);
						biometricEventInfo.setSegmentSyncDateTime(null);
						biometricEventInfo.setSiteId(sourceSiteId);

						long dbAccessStartTimeMilli = System.currentTimeMillis();
						try {
							biometricEventDao.updateEntity(biometricEventInfo);

							biometricEventDao.flush();
						} finally {
							long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
							if (logger.isTraceEnabled() || dbAccessTimeTakenMilli > 100) {
								logger.info("In insertEvent: TimeTakenMilli: " + dbAccessTimeTakenMilli);
							}
						}

						templateDataService.updateTemplateData(biometricEventInfo.getTemplateDataKey(),
								currentTemplateData);

						modifiedBiometricEventInfoList.add(biometricEventInfo);
					}
				} catch (Throwable th) {
					logger.error(
							"Error in insertEvent for externalId: " + insertBiometricEventDto.getExternalId()
									+ ", eventId: " + insertBiometricEventDto.getEventId() + " : " + th.getMessage(),
							th);
					throw th;
				}
			}

			biometricIdGenerator.commitAcquiredBiometricIdList(acquiredBiometricIdDetailInfoList);

			return modifiedBiometricEventInfoList;
		} catch (BiometricEventServiceException ex) {
			throw ex;
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error during insertEvent "
					+ GsonSerializer.toJsonLog(insertBiometricEventDto, sourceSiteId) + " : " + th.getMessage(), th);
		} finally {
			NDC.pop();
		}
	}

	public List<BiometricEventInfo> deleteEvent(DeleteBiometricEventDto deleteBiometricEventDto, String sourceSiteId)
			throws BiometricEventServiceException {
		NDC.push(deleteBiometricEventDto.getExternalId());
		try {
			lockOnExternalId(deleteBiometricEventDto.getExternalId());

			return deleteEventInternal(deleteBiometricEventDto, sourceSiteId);
		} catch (BiometricEventServiceException ex) {
			throw ex;
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error during deleteEvent "
					+ GsonSerializer.toJsonLog(deleteBiometricEventDto, sourceSiteId) + ": " + th.getMessage(), th);
		} finally {
			NDC.pop();
		}
	}

	public List<BiometricEventInfo> updateUserFlag(UpdateBiometricEventUserFlagDto updateBiometricEventUserFlagDto,
			String sourceSiteId) throws BiometricEventServiceException {
		NDC.push(updateBiometricEventUserFlagDto.getExternalId());
		try {
			String externalId = updateBiometricEventUserFlagDto.getExternalId();
			String eventId = StringUtils.trimToNull(updateBiometricEventUserFlagDto.getEventId());
			String csvUserFlags = updateBiometricEventUserFlagDto.getUserFlags();
			UpdateUserFlagMode updateUserFlagMode = updateBiometricEventUserFlagDto.getUpdateUserFlagMode();

			lockOnExternalId(updateBiometricEventUserFlagDto.getExternalId());

			List<BiometricEventInfo> modifiedBiometricEventInfoList = new ArrayList<BiometricEventInfo>();

			for (Integer binId : updateBiometricEventUserFlagDto.getBinIdList()) {
				updateUserFlag(externalId, eventId, binId, csvUserFlags, updateUserFlagMode,
						modifiedBiometricEventInfoList, sourceSiteId);
			}

			return modifiedBiometricEventInfoList;
		} catch (BiometricEventServiceException ex) {
			throw ex;
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error during updateUserFlag "
					+ GsonSerializer.toJsonLog(updateBiometricEventUserFlagDto, sourceSiteId) + ": " + th.getMessage(),
					th);
		} finally {
			NDC.pop();
		}
	}

	private void updateUserFlag(String externalId, String eventId, Integer binId, String csvUserFlags,
			UpdateUserFlagMode updateUserFlagMode, List<BiometricEventInfo> modifiedBiometricEventInfoList,
			String sourceSiteId) throws BiometricEventServiceException {
		try {
			MeghaTemplateParser meghaTemplateParser = bioMatcherConfigService.getBinIdMeghaTemplateParserMap()
					.get(binId);
			if (meghaTemplateParser == null) {
				throw new Exception("Cannot get MeghaTemplateParser for binId: " + binId);
			}

			final int userFlagStartPos = meghaTemplateParser.getUserFlagsOffsetInfo().START_POSITION;
			final int userFlagByteCount = meghaTemplateParser.getMeghaTemplateConfig().getUserFlagByteCount();
			final int totalEventCount = meghaTemplateParser.getMaxEventCount();
			List<BiometricEventInfo> biometricEventInfoList = null;

			if (totalEventCount == 1) {
				biometricEventInfoList = biometricEventDao.getBiometricEventInfoListByAnyForUpdate(externalId, eventId,
						binId);
			} else {
				biometricEventInfoList = biometricEventDao.getBiometricEventInfoListByAnyForUpdate(externalId, null,
						binId);
			}

			for (BiometricEventInfo biometricEventInfo : biometricEventInfoList) {
				if (BiometricEventStatus.DELETED.equals(biometricEventInfo.getStatus())) {
					continue;
				}

				byte[] currentTemplateData = templateDataService
						.getTemplateData(biometricEventInfo.getTemplateDataKey());

				byte tempHoldFlag[] = new byte[userFlagByteCount];

				if (updateUserFlagMode == UpdateUserFlagMode.ASSIGN
						|| updateUserFlagMode == UpdateUserFlagMode.UNASSIGN) {

					System.arraycopy(currentTemplateData, userFlagStartPos, tempHoldFlag, 0, userFlagByteCount);
					HoldFlagUtil.setHoldFlags(tempHoldFlag, csvUserFlags,
							(updateUserFlagMode == UpdateUserFlagMode.ASSIGN), MeghaTemplateUtil.DEFAULT_BYTE_ORDER);
					System.arraycopy(tempHoldFlag, 0, currentTemplateData, userFlagStartPos, userFlagByteCount);
				} else if (updateUserFlagMode == UpdateUserFlagMode.OVERWRITE) {
					HoldFlagUtil.setHoldFlags(tempHoldFlag, csvUserFlags, true, MeghaTemplateUtil.DEFAULT_BYTE_ORDER);
					System.arraycopy(tempHoldFlag, 0, currentTemplateData, userFlagStartPos, userFlagByteCount);
				}

				templateDataService.updateTemplateData(biometricEventInfo.getTemplateDataKey(), currentTemplateData);

				biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
				biometricEventInfo.setSegmentSyncDateTime(null);
				biometricEventInfo.setUpdateDateTime(new Date());
				biometricEventInfo.setDataVersion(-1L);
				biometricEventInfo.setSiteId(sourceSiteId);

				long dbAccessStartTimeMilli = System.currentTimeMillis();
				try {
					biometricEventDao.updateEntity(biometricEventInfo);
				} finally {
					long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
					if (logger.isTraceEnabled() || dbAccessTimeTakenMilli > 100) {
						logger.info("In updateUserFlag: TimeTakenMilli: " + dbAccessTimeTakenMilli);
					}
				}

				modifiedBiometricEventInfoList.add(biometricEventInfo);
			}

			biometricEventDao.flush();

			if (biometricEventInfoList.size() > 0) {
				biometricEventDao.evictAll(biometricEventInfoList);
			}
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during updateUserFlag: externalId: " + externalId + ", eventId: " + eventId + ", binId: "
							+ binId + ", csvUserFlags: " + csvUserFlags + ", updateUserFlagMode: " + updateUserFlagMode
							+ ", sourceSiteId: " + sourceSiteId + ": " + th.getMessage(),
					th);
		}
	}

	public List<BiometricEventInfo> deleteEventInternal(DeleteBiometricEventDto deleteBiometricEventDto,
			String sourceSiteId) throws BiometricEventServiceException {
		try {
			return deleteEventsByExternalIdEventId(deleteBiometricEventDto.getExternalId(),
					StringUtils.trimToNull(deleteBiometricEventDto.getEventId()), deleteBiometricEventDto.getBinIdSet(),
					sourceSiteId);
		} catch (BiometricEventServiceException ex) {
			throw ex;
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during deleteEventInternal "
							+ GsonSerializer.toJsonLog(deleteBiometricEventDto, sourceSiteId) + ": " + th.getMessage(),
					th);
		}
	}

	private List<BiometricEventInfo> deleteEventsByExternalIdEventId(String externalId, String eventId,
			Set<Integer> binIdSet, String sourceSiteId) throws BiometricEventServiceException {
		try {
			List<BiometricEventInfo> modifiedBiometricEventInfoList = new ArrayList<BiometricEventInfo>();

			List<BiometricIdDetailInfo> releasedBiometricIdList = new ArrayList<BiometricIdDetailInfo>();

			final Map<Integer, MeghaTemplateParser> binIdMeghaTemplateParserMap = bioMatcherConfigService
					.getBinIdMeghaTemplateParserMap();

			List<BiometricEventInfo> biometricEventInfoList = biometricEventDao
					.getBiometricEventInfoListByAnyForUpdate(externalId, null, binIdSet);
			for (BiometricEventInfo biometricEventInfo : biometricEventInfoList) {
				if (BiometricEventStatus.DELETED.equals(biometricEventInfo.getStatus())) {
					continue;
				}

				MeghaTemplateParser meghaTemplateParser = binIdMeghaTemplateParserMap
						.get(biometricEventInfo.getBinId());
				if (meghaTemplateParser == null) {
					throw new Exception("Cannot get MeghaTemplateParser for binId: " + biometricEventInfo.getBinId());
				}

				if (meghaTemplateParser.getMaxEventCount() == 1 || StringUtils.isBlank(eventId)) {
					if (StringUtils.isBlank(eventId) || eventId.equals(biometricEventInfo.getEventId())) {

						templateDataService.deleteTemplateData(biometricEventInfo.getTemplateDataKey());

						biometricEventInfo.setStatus(BiometricEventStatus.DELETED);
						releasedBiometricIdList.add(new BiometricIdDetailInfo(biometricEventInfo.getBinId(),
								biometricEventInfo.getAssignedSegmentId(), biometricEventInfo.getBiometricId()));

						biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
						biometricEventInfo.setSegmentSyncDateTime(null);
						biometricEventInfo.setUpdateDateTime(new Date());
						biometricEventInfo.setDataVersion(-1L);
						biometricEventInfo.setSiteId(sourceSiteId);

						long dbAccessStartTimeMilli = System.currentTimeMillis();
						try {
							biometricEventDao.updateEntity(biometricEventInfo);

							biometricEventDao.flush();
						} finally {
							long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
							if (logger.isTraceEnabled() || dbAccessTimeTakenMilli > 100) {
								logger.info("In deleteEventsByExternalIdEventId: TimeTakenMilli: "
										+ dbAccessTimeTakenMilli);
							}
						}

						modifiedBiometricEventInfoList.add(biometricEventInfo);
					}

					continue;
				}

				// ExternalId and EventId is passed for deletion

				byte[] currentTemplateData = templateDataService
						.getTemplateData(biometricEventInfo.getTemplateDataKey());

				ByteBuffer currentTemplateBuffer = ByteBuffer.wrap(currentTemplateData)
						.order(MeghaTemplateUtil.DEFAULT_BYTE_ORDER);

				boolean isTemplateModified = meghaTemplateParser.deleteEventDataByEventId(eventId,
						currentTemplateBuffer);

				List<Integer> usedEventIndexList = meghaTemplateParser.getUsedEventSlotIndexList(currentTemplateBuffer);

				if (usedEventIndexList.size() == 0) {
					// No events in template, so we can delete the template
					// itself

					biometricEventInfo.setStatus(BiometricEventStatus.DELETED);
					releasedBiometricIdList.add(new BiometricIdDetailInfo(biometricEventInfo.getBinId(),
							biometricEventInfo.getAssignedSegmentId(), biometricEventInfo.getBiometricId()));

					biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
					biometricEventInfo.setSegmentSyncDateTime(null);
					biometricEventInfo.setUpdateDateTime(new Date());
					biometricEventInfo.setDataVersion(-1L);
					biometricEventInfo.setSiteId(sourceSiteId);

					long dbAccessStartTimeMilli = System.currentTimeMillis();
					try {
						biometricEventDao.updateEntity(biometricEventInfo);
					} finally {
						long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
						logger.info(
								"In deleteEventsByExternalIdEventId: After deleting the biometricEventInfo: biometricId: "
										+ biometricEventInfo.getBiometricId() + ", externalId: "
										+ biometricEventInfo.getExternalId() + ", eventId: "
										+ biometricEventInfo.getEventId() + ", dbAccessTimeTakenMilli: "
										+ dbAccessTimeTakenMilli);
					}

					templateDataService.deleteTemplateData(biometricEventInfo.getTemplateDataKey());

					modifiedBiometricEventInfoList.add(biometricEventInfo);
				} else {
					if (isTemplateModified) {
						templateDataService.updateTemplateData(biometricEventInfo.getTemplateDataKey(),
								currentTemplateData);

						biometricEventInfo.setPhase(BiometricEventPhase.PENDING_SYNC);
						biometricEventInfo.setSegmentSyncDateTime(null);
						biometricEventInfo.setUpdateDateTime(new Date());
						biometricEventInfo.setDataVersion(-1L);
						biometricEventInfo.setSiteId(sourceSiteId);

						long dbAccessStartTimeMilli = System.currentTimeMillis();
						try {
							biometricEventDao.updateEntity(biometricEventInfo);
						} finally {
							long dbAccessTimeTakenMilli = System.currentTimeMillis() - dbAccessStartTimeMilli;
							logger.info(
									"In deleteEventsByExternalIdEventId: After updating the biometricEventInfo: biometricId: "
											+ biometricEventInfo.getBiometricId() + ", externalId: "
											+ biometricEventInfo.getExternalId() + ", eventId: "
											+ biometricEventInfo.getEventId() + ", dbAccessTimeTakenMilli: "
											+ dbAccessTimeTakenMilli);
						}

						modifiedBiometricEventInfoList.add(biometricEventInfo);
					} else {
						logger.info("In deleteEventsByExternalIdEventId: Biometric event with externalId : "
								+ externalId + ", biometricId: " + biometricEventInfo.getBiometricId()
								+ " template does not contain eventId: " + eventId);
					}
				}
			}

			biometricEventDao.flush();

			if (biometricEventInfoList.size() > 0) {
				biometricEventDao.evictAll(biometricEventInfoList);
			}

			if (releasedBiometricIdList.size() > 0) {
				biometricIdGenerator.releaseBiometricIds(releasedBiometricIdList);
			}

			return modifiedBiometricEventInfoList;
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error during deleteEvent for externalId: " + externalId
					+ ", eventId: " + eventId + ", binIdSet: " + binIdSet + " : " + th.getMessage(), th);
		}
	}

	private void lockOnExternalId(String externalId) throws Exception {
		if (StringUtils.isBlank(externalId)) {
			throw new BiometricEventServiceException("externalId is null");
		}

		HashFunction hashFunction = Hashing.murmur3_128();

		Hasher hasher = hashFunction.newHasher();

		hasher.putBytes(externalId.getBytes(Charsets.UTF_8));

		long hash = hasher.hash().asLong();

		String stripedLockKey = EXTERNAL_ID_STRIPED_LOCK_PREFIX + "_"
				+ Math.abs(Math.floorMod(hash, EXTERNAL_ID_STRIPED_LOCK_COUNT));

		long lockStartTimestampMilli = System.currentTimeMillis();
		try {
			bioLockingService.acquireTransactionLock(stripedLockKey);
		} finally {
			long lockWaitMilli = System.currentTimeMillis() - lockStartTimestampMilli;
			if (lockWaitMilli > 400) {
				logger.info("LockOnExternalId contention for externalId: " + externalId + ", lockWaitMilli: "
						+ lockWaitMilli + ", lockHash: " + hash + ", stripedLockKey: " + stripedLockKey);
			}
		}
	}

	public List<BiometricEventInfo> assignDataVersionToBiometricEvents(Integer segmentId, boolean isConversionMode)
			throws BiometricEventServiceException {
		int acquiredEventCount = 0;
		Long lastAssignedDataVersion = null;

		Stopwatch stopwatch = Stopwatch.createStarted();
		try {
			biometricEventDao.getEntityForUpdate(BioMatcherSegmentInfo.class, segmentId);

			List<BiometricEventInfo> biometricEventInfoList = biometricEventDao
					.getBiometricEventInfoListForVersioning(segmentId, 500);
			acquiredEventCount = biometricEventInfoList.size();

			if (acquiredEventCount > 0) {

				long dataVersionSequenceId = concurrentSequenceGenerator.bulkNext("SegmentDataVersionId_" + segmentId,
						acquiredEventCount);

				Map<Long, Long> eventVersionMap = new HashMap<>(acquiredEventCount);

				for (BiometricEventInfo biometricEventInfo : biometricEventInfoList) {
					eventVersionMap.put(biometricEventInfo.getBiometricId(), dataVersionSequenceId);

					biometricEventInfo.setDataVersion(dataVersionSequenceId);

					lastAssignedDataVersion = dataVersionSequenceId;

					dataVersionSequenceId = dataVersionSequenceId + 1;
				}

				if (isConversionMode) {
					biometricEventDao.updateEventDataVersion(eventVersionMap, BiometricEventPhase.SYNC_COMPLETED);
				} else {
					biometricEventDao.updateEventDataVersion(eventVersionMap);
				}

				bioMatchManagerDao.updateMatcherSegmentVersion(segmentId, lastAssignedDataVersion);
			}

			return biometricEventInfoList;
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during assignDataVersionToBiometricEvents : " + th.getMessage(), th);
		} finally {
			stopwatch.stop();

			if (logger.isTraceEnabled() || stopwatch.elapsed(TimeUnit.SECONDS) > 5) {
				logger.info("In assignDataVersionToBiometricEvents: TimeTakenMilli: "
						+ stopwatch.elapsed(TimeUnit.MILLISECONDS) + ", segmentId: " + segmentId
						+ ", acquiredEventCount: " + acquiredEventCount + ", lastAssignedDataVersion: "
						+ lastAssignedDataVersion);
			}
		}
	}

	public int checkAndResetPendingSyncEventDataVersion(Integer segmentId, long latestMatcherNodeSegmentVersionId,
			AtomicBoolean notifySyncCompletedFlag, long previousSyncCompMatcherNodeSegmentVersionId)
			throws BiometricEventServiceException {
		int updateCount = 0;
		long lastCheckSegmentVersionId = -1L;
		Stopwatch stopwatch = Stopwatch.createStarted();
		try {
			biometricEventDao.getEntityForUpdate(BioMatcherSegmentInfo.class, segmentId);

			lastCheckSegmentVersionId = StringUtil.stringToLong(
					bioParameterService.getVariableValue("LAST_CHECK_SEGMENT_DATA_VERSION_ID_" + segmentId, "VARIABLE"),
					-1);

			if (lastCheckSegmentVersionId > latestMatcherNodeSegmentVersionId) {
				logger.warn("In checkAndResetPendingSyncEventDataVersion: lastCheckSegmentVersionId: "
						+ lastCheckSegmentVersionId + " is grater than latestMatcherNodeSegmentVersionId: "
						+ latestMatcherNodeSegmentVersionId);
				lastCheckSegmentVersionId = -1;

				bioParameterService.saveVariableValue("LAST_CHECK_SEGMENT_DATA_VERSION_ID_" + segmentId, "VARIABLE",
						String.valueOf(lastCheckSegmentVersionId));
			}

			if (latestMatcherNodeSegmentVersionId > 0) {
				if (lastCheckSegmentVersionId < latestMatcherNodeSegmentVersionId) {
					updateCount = biometricEventDao.resetPendingSyncEventDataVersion(segmentId,
							lastCheckSegmentVersionId + 1L, latestMatcherNodeSegmentVersionId);

					if (updateCount > 0) {
						logger.warn("In checkAndResetPendingSyncEventDataVersion: segmentId: " + segmentId
								+ ", updateCount: " + updateCount + ", lastCheckSegmentVersionId: "
								+ lastCheckSegmentVersionId + ", latestMatcherNodeSegmentVersionId: "
								+ latestMatcherNodeSegmentVersionId);
					}

					bioParameterService.saveVariableValue("LAST_CHECK_SEGMENT_DATA_VERSION_ID_" + segmentId, "VARIABLE",
							String.valueOf(latestMatcherNodeSegmentVersionId));
				} else if (lastCheckSegmentVersionId == latestMatcherNodeSegmentVersionId) {
					if (previousSyncCompMatcherNodeSegmentVersionId != latestMatcherNodeSegmentVersionId) {
						biometricEventDao.notifySyncCompleted(segmentId, latestMatcherNodeSegmentVersionId);
						notifySyncCompletedFlag.set(true);
					}
				}
			}

			return updateCount;
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during checkAndResetPendingSyncEventDataVersion : " + th.getMessage(), th);
		} finally {
			stopwatch.stop();

			if (logger.isTraceEnabled() || stopwatch.elapsed(TimeUnit.MINUTES) > 1) {
				logger.info("In checkAndResetPendingSyncEventDataVersion: TimeTakenMilli: "
						+ stopwatch.elapsed(TimeUnit.MILLISECONDS) + ", segmentId: " + segmentId
						+ ", lastCheckSegmentVersionId: " + lastCheckSegmentVersionId
						+ ", latestMatcherNodeSegmentVersionId: " + latestMatcherNodeSegmentVersionId
						+ ", updateCount: " + updateCount);
			}
		}
	}

	public void notifySyncCompleted(Integer segmentId, long lastSyncVersion) throws BiometricEventServiceException {
		try {
			biometricEventDao.notifySyncCompleted(segmentId, lastSyncVersion);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in notifySyncCompleted: segmentId: " + segmentId
					+ ", lastSyncVersion: " + lastSyncVersion + " : " + th.getMessage(), th);
		}
	}

	public int notifySyncCompleted(Long biometricId, Long dataVersion, BiometricEventPhase currentPhase,
			BiometricEventPhase newPhase) throws BiometricEventServiceException {
		try {
			return biometricEventDao.notifySyncCompleted(biometricId, dataVersion, currentPhase, newPhase);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in notifySyncCompleted: biometricId: " + biometricId + ", dataVersion: " + dataVersion
							+ ", currentPhase: " + currentPhase + ", newPhase: " + newPhase + " : " + th.getMessage(),
					th);
		}
	}

	public void notifySyncCompleted(Integer segmentId, RangeSet<Long> segmentVersionRangeSet,
			Set<Long> corruptedSegmentVersionIdList) throws BiometricEventServiceException {
		try {
			biometricEventDao.getEntityForUpdate(BioMatcherSegmentInfo.class, segmentId);

			biometricEventDao.notifySyncCompleted(segmentId, segmentVersionRangeSet, corruptedSegmentVersionIdList);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in notifySyncCompleted: " + th.getMessage(), th);
		}
	}

	public void notifyCorruptedBiometricIdList(List<Long> corruptedBiometricIdList)
			throws BiometricEventServiceException {
		try {
			biometricEventDao.notifyCorruptedBiometricIdList(corruptedBiometricIdList);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in notifyCorruptedBiometricIdList: " + th.getMessage(), th);
		}
	}

	public void notifyCorruptedEventsByDataVersion(Integer segmentId, List<Long> corruptedDataVersionIdList)
			throws BiometricEventServiceException {
		try {
			biometricEventDao.notifyCorruptedEventsByDataVersion(segmentId, corruptedDataVersionIdList);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in notifyCorruptedEventsByDataVersion: segmentId: " + segmentId + " : " + th.getMessage(),
					th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListByBinId(Integer binId, Long dataVersion, int maxRecords)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListByBinId(binId, dataVersion, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getBiometricEventInfoListByBinId: " + th.getMessage(),
					th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForSync(Integer segmentId, Long dataVersion,
			int maxRecords) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListBySegmentIdForSync(segmentId, dataVersion, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListBySegmentIdForSync: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdForRemoteSync(Integer segmentId,
			Long dataVersion, String siteId, BiometricEventStatus biometricEventStatus, int maxRecords)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListBySegmentIdForRemoteSync(segmentId, dataVersion, siteId,
					biometricEventStatus, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListBySegmentIdForRemoteSync: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			int maxRecords) throws BiometricEventServiceException {
		PFLogger.start();
		try {
			return biometricEventDao.getActiveBiometricEventInfoListBySegmentId(segmentId, afterBiometricId,
					maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getActiveBiometricEventInfoListBySegmentId: " + th.getMessage(), th);
		} finally {
			PFLogger.end("For segmentId: " + segmentId + ", afterBiometricId: " + afterBiometricId + ", maxRecords: "
					+ maxRecords);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListBySegmentIdVersionRange(segmentId, fromDataVersion,
					toDataVersion);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListBySegmentIdVersionRange: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionRange(Integer segmentId,
			Long fromDataVersion, Long toDataVersion, BiometricEventPhase phase, int maxRecords)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListBySegmentIdVersionRange(segmentId, fromDataVersion,
					toDataVersion, phase, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListBySegmentIdVersionRange: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListBySegmentIdVersionList(Integer segmentId,
			List<Long> dataVersionList, BiometricEventPhase phase) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListBySegmentIdVersionList(segmentId, dataVersionList, phase);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListBySegmentIdVersionList: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListForSegmentation(Integer binId, int maxRecords)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoListForSegmentation(binId, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getBiometricEventInfoListByBinId: " + th.getMessage(),
					th);
		}
	}

	public BiometricEventInfo getBiometricEventInfo(Long biometricId) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getEntity(BiometricEventInfo.class, biometricId);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during getBiometricEventInfo : " + biometricId + " : " + th.getMessage(), th);
		}
	}

	public BiometricEventInfo getBiometricEventInfoByExternalId(String externalId, String eventId, Integer binId)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfo(externalId, eventId, binId);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getBiometricEventInfoByExternalId: " + th.getMessage(),
					th);
		}
	}

	public BiometricEventInfo getLatestBiometricEventInfoByExternalId(String externalId, Integer binId)
			throws BiometricEventServiceException {
		try {
			return biometricEventDao.getLatestBiometricEventInfoByExternalId(externalId, binId);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getLatestBiometricEventInfoByExternalId: " + th.getMessage(), th);
		}
	}

	public List<BiometricEventInfo> getBiometricEventInfoListByExternalId(String externalId, String eventId,
			Integer binId) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoList(externalId, eventId, binId);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getBiometricEventInfoListByExternalId: " + th.getMessage(), th);
		}
	}

	public Map<Integer, Long> getCurrentEventSegmentVersionMap() throws BiometricEventServiceException {
		try {
			return biometricEventDao.getCurrentEventSegmentVersionMap();
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getCurrentEventSegmentVersionMap: " + th.getMessage(),
					th);
		}
	}

	public Long getMaxEventSegmentVersionBySegmentId(Integer segmentId) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getMaxEventSegmentVersionBySegmentId(segmentId);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getMaxEventSegmentVersionBySegmentId: " + th.getMessage(), th);
		}
	}

	public PageResult<BiometricEventInfo> getBiometricEventInfoList(BiometricEventCriteria biometricEventCriteria,
			PageRequest pageRequest) throws BiometricEventServiceException {
		try {
			return biometricEventDao.getBiometricEventInfoList(biometricEventCriteria, pageRequest);
		} catch (Throwable th) {
			throw new BiometricEventServiceException("Error in getBiometricEventInfoList: " + th.getMessage(), th);
		}
	}

	public void setBiometricEventDao(BiometricEventDao biometricEventDao) {
		this.biometricEventDao = biometricEventDao;
	}

	public void setConcurrentSequenceGenerator(ConcurrentSequenceGenerator concurrentSequenceGenerator) {
		this.concurrentSequenceGenerator = concurrentSequenceGenerator;
	}

	public void setBiometricIdGenerator(BiometricIdGenerator biometricIdGenerator) {
		this.biometricIdGenerator = biometricIdGenerator;
	}

	public void setTemplateDataService(TemplateDataService templateDataService) {
		this.templateDataService = templateDataService;
	}

	public void setBioMatcherConfigService(BioMatcherConfigService bioMatcherConfigService) {
		this.bioMatcherConfigService = bioMatcherConfigService;
	}

	public void setBioMatchManagerDao(BioMatchManagerDao bioMatchManagerDao) {
		this.bioMatchManagerDao = bioMatchManagerDao;
	}

	public void setBioParameterService(BioParameterService bioParameterService) {
		this.bioParameterService = bioParameterService;
	}

	public void setBioLockingService(BioLockingService bioLockingService) {
		this.bioLockingService = bioLockingService;
	}

	public List<BiometricEventInfo> getBiometricEventInfoListForVersioning(Integer segmentId, int maxRecords)
			throws BiometricEventServiceException {
		try {
			List<BiometricEventInfo> biometricEventInfoList = biometricEventDao
					.getBiometricEventInfoListForVersioning(segmentId, maxRecords);
			return biometricEventInfoList;
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error during getBiometricEventInfoListForVersioning : " + th.getMessage(), th);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		List<String> currentLockKeyList = bioLockingService.getLockKeysWithPrefix(EXTERNAL_ID_STRIPED_LOCK_PREFIX);

		for (int i = 0; i <= EXTERNAL_ID_STRIPED_LOCK_COUNT; i++) {
			String stripedLockKey = EXTERNAL_ID_STRIPED_LOCK_PREFIX + "_" + i;

			if (currentLockKeyList == null || !currentLockKeyList.contains(stripedLockKey)) {
				bioLockingService.createLockIfNotExists(stripedLockKey);
			}
		}

	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, int maxRecords)
			throws BiometricEventServiceException {
		PFLogger.start();
		try {
			return biometricEventDao.getActiveBiometricEventInfoListBySegmentId(segmentId, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getActiveBiometricEventInfoListBySegmentId: " + th.getMessage(), th);
		} finally {
			PFLogger.end("For segmentId: " + segmentId + ", maxRecords: " + maxRecords);
		}
	}

	public List<BiometricEventInfo> getActiveBiometricEventInfoListBySegmentId(Integer segmentId, Long afterBiometricId,
			Long afterDataVersion, int maxRecords) throws BiometricEventServiceException {
		PFLogger.start();
		try {
			return biometricEventDao.getActiveBiometricEventInfoListBySegmentId(segmentId, afterBiometricId,
					afterDataVersion, maxRecords);
		} catch (Throwable th) {
			throw new BiometricEventServiceException(
					"Error in getActiveBiometricEventInfoListBySegmentId: " + th.getMessage(), th);
		} finally {
			PFLogger.end("For segmentId: " + segmentId + ", afterBiometricId: " + afterBiometricId + ", maxRecords: "
					+ maxRecords);
		}
	}

}
