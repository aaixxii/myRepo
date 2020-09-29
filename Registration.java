package jp.co.nec.aim.mm.acceptor;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jp.co.nec.aim.message.proto.AIMEnumTypes.ComponentType;
import jp.co.nec.aim.message.proto.AIMMessages.PBSegmentSyncInfo;
import jp.co.nec.aim.message.proto.AIMMessages.PBSegmentSyncRequest;
import jp.co.nec.aim.mm.constants.AimError;
import jp.co.nec.aim.mm.constants.MMConfigProperty;
import jp.co.nec.aim.mm.dao.RUCDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.dao.UnitDao;
import jp.co.nec.aim.mm.entities.MatchUnitEntity;
import jp.co.nec.aim.mm.entities.UnitState;
import jp.co.nec.aim.mm.exception.AimRuntimeException;
import jp.co.nec.aim.mm.exception.UnreachableCodeException;
import jp.co.nec.aim.mm.jms.JmsSender;
import jp.co.nec.aim.mm.jms.NotifierEnum;
import jp.co.nec.aim.mm.partition.PartitionUtil;
import jp.co.nec.aim.mm.procedure.AddBiometricsProcedure;
import jp.co.nec.aim.mm.procedure.DeleteBiometricsProcedure;
import jp.co.nec.aim.mm.segment.sync.SegSyncInfos;
import jp.co.nec.aim.mm.segment.sync.SegUpdatesManager;
import jp.co.nec.aim.mm.segment.sync.SyncThreadExecutor;
import jp.co.nec.aim.mm.util.CollectionsUtil;
import jp.co.nec.aim.mm.util.HttpPoster;
import jp.co.nec.aim.mm.util.OpLockHelper;
import jp.co.nec.aim.mm.util.RecordChecker;

/**
 * Registration Main work flow
 * 
 * @author liuyq
 */
public class Registration {

	/** log instance **/
	private static Logger log = LoggerFactory.getLogger(Registration.class);
	private DataSource dataSource; // DataSource instance	
	private SegUpdatesManager segUpdate; // SegUpdatesManager instance
	private UnitDao unitDao; // unit DAO
	private RUCDao rucDao; // RUC DAO	
	private SystemConfigDao configDao; // SystemConfig Dao

	/** MU_SEGMENT_UPDATE_URL **/
	private static final String MU_SEGMENT_UPDATE_URL = "matchunit/SegmentUpdateJob";	
	/**
	 * Registration default constructor
	 */
	public Registration() {
	}

	/**
	 * Registration constructor
	 * 
	 * @param dataSource
	 *            DataSource instance
	 * @param em
	 *            EntityManager instance
	 */
	public Registration(DataSource dataSource, EntityManager em) {
		this.dataSource = dataSource;
		this.segUpdate = new SegUpdatesManager(em, dataSource);
		this.unitDao = new UnitDao(em);
		this.rucDao = new RUCDao(dataSource);		
		this.configDao = new SystemConfigDao(em);	
	}

	/**
	 * insert the template and sync data
	 * 
	 * @param eventId
	 *            eventId optional
	 * @param externalId
	 *            externalId required
	 * @param syncRequests
	 *            SyncRequest list
	 */
	public Map<Long, List<SegSyncInfos>> insert(String externalId, List<AimSyncRequest> syncRequests) {			
		// insert the Template into the target container
		Map<Long, List<SegSyncInfos>> syncMap = insertTemplate(externalId, syncRequests); //syncMap key is segmentId
		// If the new segment was generated, send the event to SLB
		// SLB will wake up specified MU and DM, MU and DM will sync
		// the Data immediately.
		// if segment was only updated, we will push the different of
		// the updated segment.
		//pushOrCallSlb(syncMap);
		return syncMap;
	}
	
	public SegSyncInfos insert(String externalId, AimSyncRequest syncRequest) {			
		// insert the Template into the target container
		 SegSyncInfos segSyncInfo = insertTemplate(externalId, syncRequest); 
		// If the new segment was generated, send the event to SLB
		// SLB will wake up specified MU and DM, MU and DM will sync
		// the Data immediately.
		// if segment was only updated, we will push the different of
		// the updated segment.		
		return segSyncInfo;
	}	

	/**
	 * delete the template and sync data
	 * 
	 * @param eventId
	 *            eventId
	 * @param externalId
	 *            externalId
	 * @param containerIds
	 *            containerId list
	 */
	public int delete(String externalId, Integer containerId,  List<SegSyncInfos> delMap) {
		
		int count = deleteTemplate(externalId, containerId, delMap);
//		if (count > 0) {
//			pushOrCallSlb(delMap, false);
//		} else {
//			log.info("Skip push the segment diff or"
//					+ " call slb when delete result is zero.");
//		}
		return count;
	}

	/**
	 * deleteTemplate
	 * 
	 * @param externalId
	 *            externalId
	 * @param eventId
	 *            eventId
	 * @param containerIds
	 *            delete containerId list
	 */
	private int deleteTemplate(String externalId, Integer containerId, List<SegSyncInfos> syncMap) {
		long pNo = PartitionUtil.getInstance().caculateHashAtThisToday(LocalDate.now());
		log.debug("delete(" + externalId + ", " + ", " + (containerId.toString()) + ")");
		DeleteBiometricsProcedure procedure = new DeleteBiometricsProcedure(dataSource);
		procedure.setExternalId(externalId);
		procedure.setContainerId(containerId);
		procedure.setpNo(pNo);
		Integer count = null;
		try {
			count = procedure.executeDeletion(syncMap);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		log.debug("Called. numRecordsDeleted = " + count);
		return count.intValue();
	}

	/**
	 * insert Template with externalId, eventId and SyncRequest list
	 * 
	 * @param eventId
	 *            eventId
	 * @param externalId
	 *            externalId
	 * @param syncRequests
	 *            SyncRequest list
	 */
	private Map<Long, List<SegSyncInfos>> insertTemplate(String externalId, List<AimSyncRequest> syncRequests) {			
		// sort syncRequests list order by container id
		sortByContainerId(syncRequests);

		final Map<Long, List<SegSyncInfos>> groupBySeg = Maps.newHashMap();

		// Confirm is new segment generated		
		// loop each SyncRequest and do insert operation
		for (final AimSyncRequest request : syncRequests) {
			RecordChecker.checkRecord(request.getRecord());
			// container id required
			final Integer containerId = request.getContainerId();
			// insert the each template with each specified request
			byte[] data = request.getRecord().getTemplateData();
			long pNo = PartitionUtil.getInstance().caculateHashAtThisToday(LocalDate.now());
			SegSyncInfos syncInfos = insertEachTemplate(externalId, containerId, data, pNo);
			// if new segment was created, ASYNC call SLB directly
			if (!syncInfos.isUpdateSegment()) {
				// new segment was created
				if (log.isDebugEnabled()) {
					log.debug("New Segment({}) is created..",
							syncInfos.getSegmentId());
				}
				// once new segment was created,
				// call SLB and skip do push operation				
				List<SegSyncInfos> newList = new ArrayList<>();
				newList.add(syncInfos);
				groupBySeg.put(syncInfos.getSegmentId(), newList);
				
			} else {
				Long segId = syncInfos.getSegmentId();
				if (!groupBySeg.containsKey(segId)) {
					groupBySeg.put(segId, new ArrayList<SegSyncInfos>());
				}
				groupBySeg.get(segId).add(syncInfos);
			}	
		}
		return groupBySeg;
		
	}
	
	private SegSyncInfos insertTemplate(String externalId, AimSyncRequest syncRequests) {	
			RecordChecker.checkRecord(syncRequests.getRecord());			
			final Integer containerId = syncRequests.getContainerId();			
			byte[] data = syncRequests.getRecord().getTemplateData();
			long pNo = PartitionUtil.getInstance().caculateHashAtThisToday(LocalDate.now());
			SegSyncInfos syncInfos = insertEachTemplate(externalId, containerId, data, pNo);		
		return syncInfos;
	}


	/**
	 * insert one Template
	 * 
	 * @param externalId
	 *            externalId
	 * @param eventId
	 *            eventId
	 * @param containerId
	 *            containerId
	 * @param record
	 *            Record include template binary
	 */
	private SegSyncInfos insertEachTemplate(String externalId, int containerId, byte[] tempatleData , long pno) {			
		log.debug("insert(" + externalId + ", " + ") -> containerId " + containerId);
		for (OpLockHelper op = new OpLockHelper("insert", dataSource); op .hasNext(); op.next()) {
				
			try {
				AddBiometricsProcedure procedure = new AddBiometricsProcedure(
						dataSource);
				procedure.setExternalId(externalId);				
				procedure.setContainerId(containerId);
				procedure.setBiometricsData(tempatleData);
				procedure.setpNo(pno);
				return procedure.execute();
			} catch (DataAccessException e) {
				op.checkException(new SQLException(e), AimError.SYNC_DB);
			}
		}
		throw new UnreachableCodeException();
	}

	/**
	 * pushOrCallSlb
	 * 
	 * @param isSegCreated
	 */
	public void pushOrCallSlb(final Map<Long, List<SegSyncInfos>> syncMap, boolean isUpdateSegment) {
		// new segment created
		if (!isUpdateSegment) {
			log.info("New Segment was created, need to call slb, "
					+ "SLB will wakeUp the DM MU, DM MU will fetch the data..");

			// Insert RUC count before call SLB
			rucDao.increaseRUC();
			//new segment requeust to dm
			

			// Commit before call SLB
			//commitDao.commit();

			// ASYNC CALL SLB
			asyncCallSlb();
		} else {
			// no segment was created
			log.info("No Segment was created, call SegmentUpdateManager "
					+ "and ready to push the segment diff..");
			
			//insert  sync  requeust to dm

			// Commit before push the different
			//commitDao.commit();

			// Create muMap due to one MU may related to MULTIP
			// segment, we expect that only one push operation is enough
			// key -> muId
			Map<Long, Set<PBSegmentSyncInfo>> muMap = Maps.newHashMap();
			// key -> dmId
			//Map<Long, Set<PBSegmentSyncInfo>> dmMap = Maps.newHashMap();
			// Push group by segment id one by one
			for (final Long segId : syncMap.keySet()) {
				final List<SegSyncInfos> syncInfos = syncMap.get(segId);

				Set<Long> muIds = Sets.newTreeSet(); // MU id set				

				PBSegmentSyncInfo muSyncInfo = segUpdate.getSegmentUpdates(
						ComponentType.MATCH_UNIT, segId, syncInfos, muIds);

				if (muSyncInfo != null) {
					for (Long muId : muIds) {
						if (!muMap.containsKey(muId)) {
							muMap.put(muId, new HashSet<PBSegmentSyncInfo>());
						}
						muMap.get(muId).add(muSyncInfo);
					}
				}
		
			}

			if (log.isDebugEnabled()) {
				log.debug("Ready to async push data to DM and MU using mutip-thread..");
			}

			try {
				final SyncThreadExecutor executor = SyncThreadExecutor
						.getInstance();
				asyncPostMU(muMap, executor);				
			} catch (Exception ex) {
				final String error = "Exception occurred when sync data to MU DM..";
				log.error(error, ex);
				throw new AimRuntimeException(error, ex);
			} finally {
				// finally
			}
		}
	}

	/**
	 * push segment different information to MU
	 * 
	 * @param muMap
	 * @param executor
	 */
	private void asyncPostMU(Map<Long, Set<PBSegmentSyncInfo>> muMap,
			final SyncThreadExecutor executor) {
		if (CollectionsUtil.isEmpty(muMap)) {
			log.warn("Nothing to push any segment diffs to MU..");
			return;
		}

		log.info("Push async data to MU, task number: {}.", muMap.size());

		final int retryCount = configDao
				.getMMPropertyInt(MMConfigProperty.MU_POST_COUNT);					
				
		for (final Long muId : muMap.keySet()) {
			final PBSegmentSyncRequest.Builder bodyBuilder = PBSegmentSyncRequest.newBuilder();	
			Set<PBSegmentSyncInfo> segUpdates = muMap.get(muId);
			segUpdates.forEach( one -> {
				bodyBuilder.addSegmentUpdates(one);
			});	
			
			final MatchUnitEntity mu = unitDao.findMU(muId);
			if (mu == null || mu.getState() != UnitState.WORKING) {
				log.warn(
						"Could not find MU with mu id: {}, or MU is not in working state.",
						muId);
				continue;
			}

			// Asynchronous add push MU task
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						String contactUrl = mu.getContactUrl();
						if (contactUrl.endsWith("/")) {
							contactUrl += MU_SEGMENT_UPDATE_URL;
						} else {
							contactUrl += "/" + MU_SEGMENT_UPDATE_URL;
						}						
						HttpPoster.post(contactUrl, bodyBuilder.build()
								.toByteArray(), retryCount);
					} catch (Exception e) {
						log.error(
								"IOException occurred while sync data to mu..",
								e);
					}
				}
			});
		}
	}


	/**
	 * sort the SyncRequest list order by ContainerId asc
	 * 
	 * @param syncRequests
	 */
	private void sortByContainerId(List<AimSyncRequest> syncRequests) {
		Collections.sort(syncRequests, new Comparator<AimSyncRequest>() {
			@Override
			public int compare(AimSyncRequest o1, AimSyncRequest o2) {
				return o1.getContainerId().compareTo(o2.getContainerId());
			}
		});
	}

	/**
	 * Sync call SLB using JmsSender utility
	 */
	private void asyncCallSlb() {
		JmsSender.getInstance().sendToSLB(NotifierEnum.Registration,
				"New Segment was created.");
	}
}
