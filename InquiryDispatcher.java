package jp.co.nec.aim.mm.identify.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

import jp.co.nec.aim.message.proto.AIMMessages.PBInquiryJobInfoInput;
import jp.co.nec.aim.message.proto.AIMMessages.PBInquiryJobInfoInternal;
import jp.co.nec.aim.message.proto.AIMMessages.PBMapInquiryJobRequest;
import jp.co.nec.aim.message.proto.AIMMessages.PBMuInquiryJobOption;
import jp.co.nec.aim.message.proto.AIMMessages.PBMuJobMapInfo;
import jp.co.nec.aim.message.proto.AIMMessages.PBTargetSegmentVersion;
import jp.co.nec.aim.mm.constants.AimError;
import jp.co.nec.aim.mm.constants.MMConfigProperty;
import jp.co.nec.aim.mm.dao.CommitDao;
import jp.co.nec.aim.mm.dao.DateDao;
import jp.co.nec.aim.mm.dao.InquiryJobDao;
import jp.co.nec.aim.mm.dao.MapReducersDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.dao.UnitDao;
import jp.co.nec.aim.mm.entities.ContainerJobEntity;
import jp.co.nec.aim.mm.entities.JobQueueEntity;
import jp.co.nec.aim.mm.entities.MapReducerEntity;
import jp.co.nec.aim.mm.entities.MatchUnitEntity;
import jp.co.nec.aim.mm.entities.UnitState;
import jp.co.nec.aim.mm.exception.AimRuntimeException;
import jp.co.nec.aim.mm.identify.planner.MuJobExecutePlan;
import jp.co.nec.aim.mm.logger.PerformanceLogger;
import jp.co.nec.aim.mm.notifier.DistributorNotifier;
import jp.co.nec.aim.mm.procedure.InquiryDistributorProcedure;
import jp.co.nec.aim.mm.sessionbeans.InquiryJobHandler;
import jp.co.nec.aim.mm.sessionbeans.pojo.ExceptionSender;
import jp.co.nec.aim.mm.util.CollectionsUtil;
import jp.co.nec.aim.mm.util.StopWatch;
import jp.co.nec.aim.mm.util.XmlUtil;

/**
 * Inquiry job Dispatcher <br>
 * 1. Receive event from the planner <br>
 * 2. Find the next MR position <br>
 * 3. Look for the Inquiry information <br>
 * 4. Post to MR with PBMapInquiryJobRequest <br>
 * 
 * @author liuyq
 * 
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
public class InquiryDispatcher {

	/** log instance **/
	private static Logger log = LoggerFactory.getLogger(InquiryDispatcher.class);
	/** mapreducer/MapInquiryJob **/
	private static final String DISPATCH_MR_URL = "mapreducer/MapInquiryJob";	
	private static final String COMMA = ", ";	
	@PersistenceContext(unitName = "aim-db")
	private EntityManager manager;

	@Resource(mappedName = "java:jboss/MySqlDS")
	private DataSource dataSource;
	private MapReducersDao mapReducersDao;	
	private InquiryJobDao inquiryJobDao;
	private DateDao dateDao;
	private UnitDao unitDao;
	private SystemConfigDao configDao;
	private CommitDao commitDao;	
	private ExceptionSender exceptionSender;
	
	@EJB
	private InquiryJobHandler jobHandler; 

	@PostConstruct
	public void init() {
		this.dateDao = new DateDao(dataSource);
		this.unitDao = new UnitDao(manager);		
		this.mapReducersDao = new MapReducersDao(dataSource, manager);		
		this.inquiryJobDao = new InquiryJobDao(manager, dataSource);
		this.configDao = new SystemConfigDao(manager);
		this.commitDao = new CommitDao(dataSource);
		this.exceptionSender = new ExceptionSender();
	}

	/**
	 * InquiryDistributor constructor
	 */
	public InquiryDispatcher() {
	}

	/**
	 * dispatch the inquiry job to MR
	 * 
	 * @param plan
	 *            ExecutePlanEntity instance
	 */
	public void dispatch(final Map<Long, List<MuJobExecutePlan>>  plans) {
		if (CollectionsUtil.isEmpty(plans)) {
			throw new IllegalArgumentException(
					"Plans is empty while do inquiry dispatch operation.");
		}		
		List<MuJobExecutePlan> allPlanList = plans.values().stream().flatMap(muPlanList -> muPlanList.stream()).collect(Collectors.toList());			
		findAndNotifyMR(allPlanList);
	}
	
	public void dispatch(final List<MuJobExecutePlan>  batchPlans) {
		if (CollectionsUtil.isEmpty(batchPlans)) {
			throw new IllegalArgumentException(
					"Plans is empty while do inquiry dispatch operation.");
		}				
		findAndNotifyMR( batchPlans);
	}


	/**
	 * check plan
	 * 
	 * @param MuJobExecutePlan
	 *            plan
	 */
	private void checkPlan(MuJobExecutePlan plan) {
		final Long planId = plan.getPlanId();

		if (planId == null || planId <= 0) {
			throw new IllegalArgumentException(
					"Plan id is not correct while do inquiry dispatch operation.");
		}

		final Long jobId = plan.getJobId();
		if (jobId == null || jobId <= 0) {
			throw new IllegalArgumentException(
					"Job id is not correct while do inquiry dispatch operation.");
		}
		final Integer functionId = plan.getFunctionId();
		if (functionId == null || functionId <= 0) {
			throw new IllegalArgumentException(
					"Function id is not correct while do inquiry dispatch operation.");
		}

		final Integer containerId = plan.getContainerId();
		if (containerId == null || containerId <= 0) {
			throw new IllegalArgumentException(
					"Container id is not correct while do inquiry dispatch operation.");
		}
	}

	/**
	 * find And Notify MR
	 * 
	 * @param planId
	 *            plan id from the inquiry planner
	 * @return next MR position could be found or not
	 */
	private boolean findAndNotifyMR(List<MuJobExecutePlan> allPlanList) {
		MapReducerEntity nextMR = null;		
		int exeCount = 0;
		StopWatch sw = new StopWatch();
		sw.start();		
		
		// before loop find MR, get the number working MR as
		// max retry count.
		int retryCount = unitDao.getWorkingMRCount();
		if (retryCount <= 0) {
			log.warn("Working MR is not exist when dispatch inquiry job..");
		}
		try {
			while (nextMR == null) {
				log.info("Ready to fetch the next MR, Executed {} times.",
						++exeCount);

				// if exeCount is over max retry
				if (exeCount > retryCount) {
					log.error("Could not fetch the next MR due to "
							+ "there are no Working MR...");
					nextMR = null;
					break;
				}

				// call MRFetcher Procedure and find next MR
				// if Multiple thread reached,
				// we will lock table [LAST_ASSIGNED_MR] to
				// avoid fetch the same MR position
				nextMR = mapReducersDao.getNextMapReducer();

				// nextMR is null means can not found next MR position
				// we will fail the container job, so break this loop
				if (nextMR == null) {
					log.error("Could not find next MR position,"
							+ " Ready to fail the Conatiner job..");
					break;
				}

				// nextMR contact URL is blank
				// continue to find next MR position..
				// maybe next MR is alive
				String contactUrl = nextMR.getContactUrl();
				if (StringUtils.isBlank(contactUrl)) {
					log.error("nextMR contact URL is blank, "
							+ "continue to find next MR position..");
					nextMR = null;
					continue;
				} else {
					if (contactUrl.endsWith("/")) {
						contactUrl += DISPATCH_MR_URL;
					} else {
						contactUrl += "/" + DISPATCH_MR_URL;
					}
				}
				
				Long mrPlanId = inquiryJobDao.insertMrPlan(nextMR.getMrId());
				
				// notify the MR with specified URL and body
				// 1. lock the container job row with id (transaction 1)
				// 2. job complete wait for 1 (transaction 2)
				// 3. update the container job set the AssignedTs and MR id
				// (transaction 1)
				// 4. commit (transaction 1)
				// 5. job complete do completion operation (transaction 2)
				PBMapInquiryJobRequest mrRequest = buildPBMapInquiryJobRequest(mrPlanId,allPlanList);
				log.info(mrRequest.toString());
				if (log.isDebugEnabled()) {
					log.debug(mrRequest.toString());
				}
//				long cJobId = inquiryJobDao.lockContainerJob(info
//						.getContainerJobId());
//				log.info("Container job id {} was locked for update.", cJobId);
				boolean isSuccess = notifyMR(contactUrl, mrRequest);
				if (isSuccess) {
					
					log.info(
							"Notify MR({}) with URL({}) mrPlanId({}) successfully.", nextMR.getMrId(), contactUrl, mrPlanId);
					// update MR id and ASSIGNED_TS(current epoch time)
					updateAfterSuccss(allPlanList, nextMR);						
					inquiryJobDao.deleteMrPlan(Integer.valueOf(String.valueOf(nextMR.getMrId())), mrPlanId);
					commitDao.commit();
					break;
				} else {
					log.warn("Notify MR({}) with URL({}) failed, "
							+ "find next MR position..", nextMR.getMrId(),
							contactUrl);
					// offLine the current MR that was not alive
					mapReducersDao.updateMRState(nextMR.getMrId(),
							UnitState.TIMED_OUT);

					// rollBack container jobs with MR id
					rollbackInqJobs(nextMR.getMrId());					
					// reFind the next MR position					
					inquiryJobDao.deleteMrPlan(Integer.valueOf(String.valueOf(nextMR.getMrId())), mrPlanId);
					commitDao.commit();
					nextMR = null;
				}
			}

			// NextMR is null due to the system without any map reducer
			// we will fail the inquiry container job and
			if (nextMR == null) {
				faildMrInquiryJobs(allPlanList);
				return false;
			}
		} catch (Exception ex) {
			log.error("Exception occurred when findAndNotifyMR,"
					+ " rollback the job..", ex);
			faildInquiryJobs(allPlanList, nextMR, ex);	
			return false;
		} finally {
			sw.stop();
			PerformanceLogger.log(getClass().getSimpleName(), "findAndNotifyMR", sw.elapsedTime());
		}
		return true;
	}
	
	private void updateAfterSuccss(List<MuJobExecutePlan> allPlanList,  MapReducerEntity nextMR) {
		allPlanList.forEach(one -> {
			InqDispatchInfo info = getInquiryJobInfo(one);
			inquiryJobDao.updateContainerJob((int) nextMR.getMrId(),
					info.getContainerJobId());			
			// commit and unlock the container job row
			// let the inquiry job complete the result			
			// Notify the MR successfully, break Notify..			
		});	
		
	}



	/**
	 * Get necessary inquiry job information
	 * 
	 * @param planId
	 *            plan id
	 * @return InqDistributorInfo instance
	 */
	private InqDispatchInfo getInquiryJobInfo(MuJobExecutePlan plan) {
		final InquiryDistributorProcedure procedure = new InquiryDistributorProcedure(
				dataSource);
		procedure.setPlanId(plan.getPlanId());
		procedure.setFunctionId(plan.getFunctionId());
		procedure.setJobId(plan.getJobId());
		return procedure.execute();
	}

	/**
	 * notifyMR
	 * 
	 * @param url
	 * @param request
	 */
	private boolean notifyMR(final String url, final PBMapInquiryJobRequest request) {			
		final int retryCount = configDao
				.getMMPropertyInt(MMConfigProperty.MR_POST_COUNT);
		final DistributorNotifier notifier = new DistributorNotifier(url,
				request.toByteArray(), retryCount);
		return notifier.notifyMR();
	}

	/**
	 * Roll back the inquiry job with specified MR id
	 * 
	 * @param mrId
	 *            MR id
	 */

	
	public void rollbackInqJobs(long mrId) {
		List<ContainerJobEntity> deadJobs = inquiryJobDao.listDeadJobs(mrId);
		if (CollectionsUtil.isEmpty(deadJobs)) {
			if (log.isDebugEnabled()) {
				log.debug("mr id: {} could not found any container jobs.", mrId);
			}
			return;
		}		
		log.warn("{}", makeDeadJobInfo(mrId, deadJobs));
		// loop each dead inquiry jobs
		for (final ContainerJobEntity job : deadJobs) {
			long containerJobId = job.getContainerJobId();
			try {
				final AimError aimError = AimError.DISPATCHER_MR_RESPONSE;
				jobHandler.failInquiryJob(containerJobId,
						aimError.getErrorCode(),
						String.format(aimError.getMessage(), mrId),
						dateDao.getReasonTime());
			} catch (Exception e) {
				final AimError aimError = AimError.INQ_JOB_RETRY;				
				exceptionSender.sendAimException(aimError.getErrorCode(),
						aimError.getMessage(), containerJobId, containerJobId, mrId, e);
			}
		}			
		log.info("rollbackInqJobs success for mr]{}", mrId);
	}	
	
	private String makeDeadJobInfo(long mrId,
			List<ContainerJobEntity> deadJobList) {
		StringBuilder sb = new StringBuilder();
		String firstWarnInfo = "Failed to notify containerJob to MR(ID="
				+ String.valueOf(mrId)
				+ "). Process job as failed or retrying : ";
		sb.append(firstWarnInfo);		
		for (ContainerJobEntity job : deadJobList) {
			sb.append("[");	
			sb.append("FUSION_JOB_ID=");
			sb.append(job.getFusionJobId());
			sb.append(COMMA);
			sb.append("CONTAINER_JOB_ID=");
			sb.append(job.getContainerJobId());
			sb.append(COMMA);
			sb.append("PLAN_ID=");
			sb.append(job.getPlanId());
			sb.append("]");
			sb.append(COMMA);
		}			
		sb.delete(sb.length() -2 , sb.length() -1);	
		return sb.toString();
	}
	
	public String createErrorResponse(long jobId)  {
		JobQueueEntity topJob = inquiryJobDao.getTopLevelJob(jobId);
		
		AimError aimErr = AimError.INQ_INTERNAL;		
		String errMsg =aimErr.getMessage() + ". inquriy dispach error.";
		aimErr.setMessage(errMsg);
		String xmlRes = null;
		try {
			xmlRes = XmlUtil.dom4jCreateResponseXml(topJob.getRequestId(), "2", aimErr.getUidCode());
		} catch (IOException e1) {
			xmlRes = e1.getMessage();
		}		
		return xmlRes;		
	}
	
	private PBMapInquiryJobRequest buildPBMapInquiryJobRequest(long mrPlanId, List<MuJobExecutePlan> oneTopPlans) {
		PBMapInquiryJobRequest.Builder pbMrReq = PBMapInquiryJobRequest.newBuilder();
		pbMrReq.setPlanId(mrPlanId);
		boolean mustAddMujobMap = true;
		for (int i = 0; i < oneTopPlans.size(); i++) {
			MuJobExecutePlan one = oneTopPlans.get(i);
			checkPlan(one);
			PBInquiryJobInfoInternal.Builder pbiqyJobInt = PBInquiryJobInfoInternal.newBuilder();
			InqDispatchInfo dispatchInfo = getInquiryJobInfo(one);
			pbiqyJobInt.setTopLevelJobId(one.getJobId().longValue());
			pbiqyJobInt.setRequestIndex(dispatchInfo.getRequestIndex());
			pbiqyJobInt.setContainerId(one.getContainerId());
			pbiqyJobInt.setMessageSequence(dispatchInfo.getMessageSequence());
			pbiqyJobInt.setJobTimeout(dispatchInfo.getJobTimeout());
			PBInquiryJobInfoInput.Builder pbinqJobInput = PBInquiryJobInfoInput.newBuilder();
			pbinqJobInput.setRequest(dispatchInfo.getInquiryRequstXml());
			pbinqJobInput.setBisonTemplate(ByteString.copyFrom(dispatchInfo.getInquiryData()));
			pbiqyJobInt.setIn(pbinqJobInput.build());
			pbMrReq.addJobInfo(pbiqyJobInt.build());
			if (mustAddMujobMap) {
				List<PBMuJobMapInfo> mujobMap = buildPBMuJobMapInfo(one.getPlan());
				pbMrReq.addAllMuJobMap(mujobMap);
				mustAddMujobMap = Boolean.FALSE;
			}

			long cJobId = inquiryJobDao.lockContainerJob(dispatchInfo.getContainerJobId());
			log.debug("Container job id {} was locked for update.", cJobId);
		}

		PBMuInquiryJobOption.Builder muOption = PBMuInquiryJobOption.newBuilder();
		muOption.setSearchMode(configDao.getMMProperty(MMConfigProperty.INQUIRY_SEARCH_MODE));
		muOption.setCrossMatch(
				Boolean.valueOf(configDao.getMMProperty(MMConfigProperty.INQUIRY_CROSS_MATCH)).booleanValue());
		pbMrReq.setOption(muOption.build());

		return pbMrReq.build();

	}
	private List<PBMuJobMapInfo> buildPBMuJobMapInfo(String plan) {
		List<PBMuJobMapInfo> pBMuJobMapInfoList = new ArrayList<>();
		final  String FIRST_SPLIT = "/";
		final  String SECOND_SPLIT = ",";
		final  String THIRD_SPLIT = ":";
		final String MU_URL = "matchunit/InquiryJob";
		final  String muSegStr = plan;
		if (StringUtils.isBlank(muSegStr)) {
			throw new AimRuntimeException("plan bytes is null or empty..");
		}

		final StringTokenizer muSegs = new StringTokenizer(muSegStr, FIRST_SPLIT);				
		while (muSegs.hasMoreTokens()) {
			final String muSeg = muSegs.nextToken();
			if (StringUtils.isBlank(muSeg)) {
				log.warn("ip segment item is null or empty..");
				continue;
			}

			String[] array = muSeg.split(SECOND_SPLIT);
			if (array == null || array.length <= 1) {
				log.warn("mu id must be specified.");
				continue;
			}

			PBMuJobMapInfo.Builder mujob = PBMuJobMapInfo.newBuilder();
			Long muId = Long.valueOf(array[0]);
			mujob.setMuId(muId);

			MatchUnitEntity mu = unitDao.findMU(muId);
			if (mu != null && !Strings.isNullOrEmpty(mu.getContactUrl())) {
				String contactUrl = mu.getContactUrl();
				if (contactUrl.endsWith("/")) {
					contactUrl += MU_URL;
				} else {
					contactUrl += "/" + MU_URL;
				}
				mujob.setUrl(contactUrl);
			} else {
				throw new AimRuntimeException(String.format(
						"Can not get MU(%s) contact URL.", muId));
			}

			for (int i = 1; i < array.length; i++) {
				String item = array[i];
				if (Strings.isNullOrEmpty(item)) {
					throw new AimRuntimeException(
							"Seg:Version is null or empty.");
				}

				String[] SegVers = item.split(THIRD_SPLIT);
				if (SegVers == null || SegVers.length != 2) {
					throw new AimRuntimeException(
							"Seg:Version length is not 2.");
				}

				try {
					mujob.addTargetSegments(PBTargetSegmentVersion.newBuilder()
							.setId(Long.valueOf(SegVers[0]))
							.setVersion(Long.valueOf(SegVers[1])));
				} catch (Exception e) {
					throw new AimRuntimeException(
							"Exception occurred when create "
									+ "PBTargetSegmentVersion instance", e);
				}
			}
			pBMuJobMapInfoList.add(mujob.build());
		}
		return pBMuJobMapInfoList ;
	}
	
	private void faildMrInquiryJobs(List<MuJobExecutePlan> allPlanList) {
		allPlanList.forEach(one -> {
			final AimError aimError = AimError.DISPATCHER_MR_NOT_FOUND;
			InqDispatchInfo info = getInquiryJobInfo(one);
			final long containerJobId = info.getContainerJobId();
			final String failTime =String.valueOf(System.currentTimeMillis());
			log.warn(aimError.getMessage());				
			String errorRes  = createErrorResponse(one.getJobId()); //create error res with no mr
			jobHandler.failInquiryJob(containerJobId, aimError.getErrorCode(), aimError.getMessage(), errorRes, failTime);
			log.info("faild inquiry containerJob, containerJobId:{}", info.getContainerJobId());			
		});	
	}
	
	
	private void faildInquiryJobs(List<MuJobExecutePlan> allPlanList, MapReducerEntity nextMR, Exception ex) {	
		final AimError aimError = AimError.DISPATCHER_EXCEPTION;
		String errMsg = String.format("%s", aimError.getMessage() + "." + ex.getMessage());			
		aimError.setMessage(errMsg);
		allPlanList.forEach(one -> {		
			final String failTime = String.valueOf(System.currentTimeMillis());		
			InqDispatchInfo faildInfo = getInquiryJobInfo(one);
				if (faildInfo == null) {
					log.info(
							"Could not fetch the container job id with "
									+ "plan id: {}, job id: {} function id: {},"
									+ " container id: {}, fail "
									+ "or rollback with job id.",
							new Object[] { one.getPlanId(), one.getJobId(),
									one.getFunctionId(), one.getContainerId() });

					List<ContainerJobEntity> containerJobs = inquiryJobDao
							.getAllContainerJob(one.getJobId());
					for (ContainerJobEntity containerJob : containerJobs) {
						String errorRes  = createErrorResponse(containerJob.getContainerJobId());
						jobHandler.failInquiryJob(containerJob.getContainerJobId(), aimError.getErrorCode(), errorRes, failTime);
						// send the inquiry error event to error queue
						exceptionSender.sendAimException(aimError.getErrorCode(),
								aimError.getMessage(), containerJob.getContainerJobId(), one.getJobId(),
								(nextMR == null) ? -1 : nextMR.getMrId(), ex);
					}				
			}
			
		});
	}
}
