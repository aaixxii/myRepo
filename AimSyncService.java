package jp.co.nec.aim.mm.acceptor.service;

import static jp.co.nec.aim.mm.constants.Constants.maxSegmetRecordCount;
import static jp.co.nec.aim.mm.constants.MMConfigProperty.INTERVAL_CLIENT_SYNC_RESPONSE_TIMEOUT;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import jp.co.nec.aim.dm.message.proto.AimDmMessages.PBDmSyncRequest;
import jp.co.nec.aim.dm.message.proto.AimDmMessages.PBTargetSegmentVersion;
import jp.co.nec.aim.dm.message.proto.AimDmMessages.PBTemplateInfo;
import jp.co.nec.aim.dm.message.proto.AimDmMessages.SegmentSyncCommandType;
import jp.co.nec.aim.message.proto.AIMMessages.PBMuExtractJobResultItem;
import jp.co.nec.aim.mm.acceptor.AimSyncRequest;
import jp.co.nec.aim.mm.acceptor.Record;
import jp.co.nec.aim.mm.acceptor.Registration;
import jp.co.nec.aim.mm.callbakSender.AmqExecutorManager;
import jp.co.nec.aim.mm.callbakSender.AmqSocketSender;
import jp.co.nec.aim.mm.constants.AimError;
import jp.co.nec.aim.mm.constants.UidRequestType;
import jp.co.nec.aim.mm.dao.CommitDao;
import jp.co.nec.aim.mm.dao.FEJobDao;
import jp.co.nec.aim.mm.dao.FunctionDao;
import jp.co.nec.aim.mm.dao.PersonBiometricDao;
import jp.co.nec.aim.mm.dao.SegmentDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.dm.client.DmJobPoster;
import jp.co.nec.aim.mm.dm.client.mgmt.UidDmJobRunManager;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.entities.FunctionTypeEntity;
import jp.co.nec.aim.mm.entities.PersonBiometricEntity;
import jp.co.nec.aim.mm.hazelcast.HazelcastService;
import jp.co.nec.aim.mm.jms.JmsSender;
import jp.co.nec.aim.mm.jms.NotifierEnum;
import jp.co.nec.aim.mm.procedure.FeJobProcedures;
import jp.co.nec.aim.mm.segment.sync.SegSyncInfos;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimManager;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimServiceState;
import jp.co.nec.aim.mm.sessionbeans.pojo.ExtractJobHandler;
import jp.co.nec.aim.mm.sessionbeans.pojo.UidAimAmqResponse;
import jp.co.nec.aim.mm.util.ObjectUtil;
import jp.co.nec.aim.mm.util.XmlUtil;
import jp.co.nec.aim.mm.validator.AcceptorValidator;

/**
 * The main work flow of Sync <br>
 * Include following public method:
 * <p>
 * syncData
 * <p>
 * 
 * @author xiazp
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRED)
public class AimSyncService {
	/** log instance **/
	private static Logger log = LoggerFactory.getLogger(AimSyncService.class);
	
	@PersistenceContext(unitName = "aim-db")
	private EntityManager manager;

	@Resource(mappedName = "java:jboss/MySqlDS")
	private DataSource dataSource;

	private Registration reg;

	private FEJobDao feJobDao;
	
	private PersonBiometricDao personBiometricDao;

	private SystemConfigDao sysConfigDao;

	private FunctionDao functionDao;
	
	private SegmentDao segmentDao;

	private FeJobProcedures feJobProcedures;
	
	private ExtractJobHandler extractJobHandler;	
	
	private CommitDao commitDao;

	@EJB
	private AimExtractService aimExService;

	/**
	 * default constructor
	 */
	public AimSyncService() {
	}

	@PostConstruct
	private void init() {
		this.reg = new Registration(dataSource, manager);
		this.feJobDao = new FEJobDao(manager);
		this.personBiometricDao = new PersonBiometricDao(manager);
		this.functionDao = new FunctionDao(manager);
		this.sysConfigDao = new SystemConfigDao(manager);
		this.segmentDao = new SegmentDao(manager);		
		this.extractJobHandler = new ExtractJobHandler(manager, dataSource);
		feJobProcedures = new FeJobProcedures(dataSource);
		this.commitDao = new CommitDao(dataSource);
	}

	/**
	 * The main work flow of syncData
	 * 
	 * @param request
	 *            PBSyncJobRequest instance
	 * @return the instance of PBSyncJobResponse
	 * @throws IOException 
	 */
	public UidAimAmqResponse syncData(String syncRequest) throws IOException {
		UidAimAmqResponse resResult = syncData(syncRequest, null);		
		return resResult;
	}
	
	public void syncDataByAmq(String syncRequest) throws IOException {
		UidAimAmqResponse resResult = syncData(syncRequest, null);
		byte [] uidResData = ObjectUtil.serializeAmqResults(resResult);
		Integer port = AmqExecutorManager.getInstance().getCallbackIpPort("1");		
		AmqSocketSender sendTask = new AmqSocketSender(port, uidResData);
		AmqExecutorManager.getInstance().commitTask(sendTask);
        return;
	}

	/**
	 * The main work flow of syncData
	 * 
	 * @param syncRequest
	 *            PBSyncJobRequest instance
	 * @param delCount
	 *            delete count
	 * @return PBSyncJobResponse instance
	 * @throws IOException 
	 * @throws JAXBException
	 * @throws Exception
	 */
	public UidAimAmqResponse syncData(final String syncRequest, final AtomicInteger delCount) throws IOException {
		// xml response return:0:notused,1:Successed, 2:Failed
		UidAimAmqResponse response = null;
		if (log.isDebugEnabled()) {
			log.debug(syncRequest);
		}
		String requestId = XmlUtil.getRequestId(syncRequest);
		String cmd = XmlUtil.getXmlCmd(syncRequest);
		String refId = null;
		String refUrl = null;
		if (cmd.equals("Delete")) {
			refId = XmlUtil.getRefId(syncRequest, UidRequestType.Delete.name());
			refUrl = XmlUtil.getUrl(syncRequest, UidRequestType.Delete.name());
			
		} else if (cmd.equals("Insert")) {
			 refId = XmlUtil.getRefId(syncRequest, UidRequestType.Insert.name());
			 refUrl = XmlUtil.getUrl(syncRequest, UidRequestType.Insert.name());
		}		
		if (cmd.equals("Delete")) {
			response = AcceptorValidator.checkSyncDeleteRequest(syncRequest, requestId, cmd, refId);			
			if (response != null) {
				return response;
			}	
			return doSyncDelete(syncRequest, requestId, refId, delCount);
		}
		if (cmd.equals("Insert")) {
			response = AcceptorValidator.checkSyncInsertRequest(syncRequest, requestId, cmd, refId, refUrl);			
			if (response != null) {
				return response;
			}
			if (!StringUtils.isBlank(refId)) {
				PersonBiometricEntity pbEntity = personBiometricDao.isHaveBioMetricDataInMyTable(refId);
				if (pbEntity != null) {
					log.info("The template is already in Bison(NSM) do nothing"); 
					response = new UidAimAmqResponse();
					response.setRequestId(requestId);
					response.setRequestType(UidRequestType.Insert.name());
					response.setXmlResult("The template is already in Bison(NSM) do nothing");
					return response;
				}	
				
				boolean hasTemplateInFeQueue = feJobDao.hasTempalte(refId);
				if (hasTemplateInFeQueue) {
					UidAimAmqResponse refIdResult =  doSyncByRefId(syncRequest, refId, requestId);
					if (refIdResult != null) {
						return refIdResult;	
					}									
				}
			}
			
			if (!StringUtils.isBlank(refId) && !StringUtils.isBlank(refUrl)) {
				return doSyncByRefUrl(syncRequest, refId, refUrl);
			} 
		}
		if (response == null) {			
			AimError aimErr = AimError.INTERNAL_ERROR;
			response = buildFaildSyncReespose(requestId, aimErr.getUidCode());			
		}
		return response;
	}

	private UidAimAmqResponse doSyncByRefId(final String syncRequest, String refId, String requestId) throws IOException {
		log.info("syncData insert by refernceId begin..");	
		UidAimAmqResponse mqRespose  = null;
		FeJobQueueEntity feEntity = null;
		try {
			List<FeJobQueueEntity> resultList = feJobDao.getExResult(refId);			
			feEntity = resultList.get(0);			
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return mqRespose;
		}

		final List<AimSyncRequest> aimRequestList = new ArrayList<>();
		Record record = new Record(feEntity.getTempalteData(), true);
		AimSyncRequest aimReq = new AimSyncRequest(Integer.valueOf(1));
		aimReq.setRecord(record);
		aimRequestList.add(aimReq);
		try {
			//Map<Long, List<SegSyncInfos>> segSyncMap = reg.insert(refId,aimRequestList);			
			SegSyncInfos segSyncInfo = reg.insert(refId, aimReq);
			if (!segSyncInfo.isUpdateSegment()) {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW);
			} else {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
			}			
			segSyncInfo.setExternalId(refId);
			segSyncInfo.setTemplateData(feEntity.getTempalteData());			
			PBDmSyncRequest dmRequest = buildDmJobRequest(segSyncInfo);
			
			String baseUrl = UidDmJobRunManager.getOneActiveDm();
			if (baseUrl == null) {
				log.warn("No active dm!");
				AimError aimErr = AimError.DM_TIME_OUT;
				mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl +"/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;			
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (!dmRequest.getCmd().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
				if (postResult.booleanValue()) {
					commitDao.commit();
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", refId, refId);
					final Map<Long, List<SegSyncInfos>> segSyncMap = Maps.newHashMap();
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					segSyncMap.put(segSyncInfo.getSegmentId(), segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());				
					mqRespose = buildSuccessSyncReespose(requestId, feEntity.getDiagnostcs());				
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, eenrollmentId={}", refId, refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;					
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
					return mqRespose;				
				}
			} else {
				//new segment must post 2 times
				segSyncInfo.setIsUpdateSegment(false);
				segSyncInfo.setSegVersion(0l);
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
				PBDmSyncRequest dmRequest2 = buildDmJobRequest(segSyncInfo);
				Boolean postResult2 = DmJobPoster.post(url, dmRequest2);
				if (postResult.booleanValue() && postResult2.booleanValue()) {
					segmentDao.updateSegmentVersin(0l, 0l, segSyncInfo.getSegmentId().longValue());
					commitDao.commit();
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", refId, refId);
					final Map<Long, List<SegSyncInfos>> segSyncMap = Maps.newHashMap();
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					segSyncMap.put(segSyncInfo.getSegmentId(), segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());				
					mqRespose = buildSuccessSyncReespose(requestId, feEntity.getDiagnostcs());
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, eenrollmentId={}", refId, refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;					
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
					return mqRespose;
				}
			}		
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			AimError aimErr = AimError.INTERNAL_ERROR;
			mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
		}	
		return mqRespose;
	}

	private UidAimAmqResponse doSyncByRefUrl(final String syncRequest, String refId, String refUrl) throws IOException {		
		log.info("syncData insert by url begin..");
		UidAimAmqResponse mqRespose  = null;
		// final FeJobQueueEntity feJob = new FeJobQueueEntity();
		String requestId = XmlUtil.getRequestId(syncRequest);
		FunctionTypeEntity fte = functionDao.getExtractFunction();
		if (fte == null) {
			AimError dbErr = AimError.SYNC_FUNCTIONTYPE;			
			mqRespose = buildFaildSyncReespose(requestId, dbErr.getUidCode());
			return mqRespose;
		}

		Long newFeJobId = null;
		try {
			newFeJobId = feJobProcedures.createNewFeJob(refId, requestId, UidRequestType.Insert.name(), syncRequest);
		} catch (Exception e1) {
			feJobDao.deleteExtractJob(newFeJobId);
			Throwable cause = e1.getCause();
			Throwable lastCause = null;
			String errMsg = null;
			while (cause != null) {
				cause = cause.getCause();
				if (cause != null) {
					lastCause = cause;
				}
			}
			if (lastCause != null) {
				errMsg = lastCause.getMessage();
			}
			AimError dbErr = AimError.SYNC_DB;
			errMsg = StringUtils.isNoneEmpty(errMsg) ? errMsg : String.format(dbErr.getMessage() + " ." + e1.getMessage());
			dbErr.setMessage(errMsg);
			mqRespose = buildFaildSyncReespose(requestId, dbErr.getUidCode());
			return mqRespose;
		}

		JmsSender.getInstance().sendToFEJobPlanner(NotifierEnum.ExtractService,
				String.format("create extract job id: %s", newFeJobId));
		Integer syncJobWaitTime = sysConfigDao.getMMPropertyInt(INTERVAL_CLIENT_SYNC_RESPONSE_TIMEOUT);
		if (Objects.isNull(syncJobWaitTime) || syncJobWaitTime < 0) {
			syncJobWaitTime = 100000;
		}
		String key = String.valueOf(newFeJobId.longValue());
		Long extractJoblocker = Long.valueOf(key);
		HazelcastService.getInstance().saveToExtractLockQueue(key, extractJoblocker);
		Optional<PBMuExtractJobResultItem> onejobResult = null;
		synchronized (extractJoblocker) {
			long startGetExtResultTime = System.currentTimeMillis();
			try {
				extractJoblocker.wait(syncJobWaitTime);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
			try {
				onejobResult = Optional.ofNullable(HazelcastService.getInstance().getExtractJobResult(key));
			} catch (NullPointerException e) {
				log.warn("can't get extractResponse, it may be timeout.");
				AimError timeoutErr = AimError.JOB_TIMEOUT;	
				mqRespose = buildFaildSyncReespose(requestId, timeoutErr.getUidCode());	
				return mqRespose;
			}
			if (onejobResult.isPresent()) {
				log.info("Get insert by url jobId({}) result success", newFeJobId);
				long endGetResultTime = System.currentTimeMillis();
				log.info("*****MM get insert by url job results used time = {}****",
						endGetResultTime - startGetExtResultTime);
			} else {
				log.warn("Got empty PBMuExtractJobResultItem, key={}", key);
				long currentTime = System.currentTimeMillis();
				if (currentTime - startGetExtResultTime >= syncJobWaitTime) {
					log.warn("Timeout is happend! the waiting time = ({}), jobId({})",
							currentTime - startGetExtResultTime, newFeJobId.longValue());
					AimError timeoutErr = AimError.JOB_TIMEOUT;
					String errMsg = String.format(timeoutErr.getMessage() + " . sync insert by url job timeout.");
					timeoutErr.setMessage(errMsg);
					mqRespose = buildFaildSyncReespose(requestId, timeoutErr.getUidCode());
					FeJobQueueEntity eje = feJobDao.getFeJobInfoById(newFeJobId.longValue());
					AimServiceState ass = new AimServiceState();
					ass.setErrMsg(timeoutErr.getMessage());
					ass.setErrorcode(timeoutErr.getErrorCode());
					ass.setFailureTime(String.valueOf(System.currentTimeMillis()));					
					extractJobHandler.failExtractJob(null, eje, eje.getMuId().longValue(), ass,  true, 0);					
					return mqRespose;
				}
			}
		}
		HazelcastService.getInstance().finishExtractJob(key);
		//final List<AimSyncRequest> aimRequestByExtList = new ArrayList<>();
		Record recordByExt = new Record(onejobResult.get().getBisonTemplate().toByteArray(), true);

		AimSyncRequest aimReqExt = new AimSyncRequest(Integer.valueOf(1), recordByExt);	
		mqRespose  = new UidAimAmqResponse();
		try {
			// Map<Long, List<SegSyncInfos>> segSyncMap = reg.insert(refId,aimRequestList);			
			SegSyncInfos segSyncInfo = reg.insert(refId, aimReqExt);
			if (!segSyncInfo.isUpdateSegment()) {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW);
			} else {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
			}
			
			segSyncInfo.setExternalId(refId);
			segSyncInfo.setTemplateData(onejobResult.get().getBisonTemplate().toByteArray());			
			log.info("success insert into PERSON_BIOMETRICS table(not yet commit) reqeustId={}, eenrollmentId={}", refId, refId);
			PBDmSyncRequest dmRequest = buildDmJobRequest(segSyncInfo);
			String baseUrl = UidDmJobRunManager.getOneActiveDm();
			if (baseUrl == null) {
				log.warn("No active dm!");
				AimError aimErr = AimError.DM_TIME_OUT;
				mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl +"/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (!dmRequest.getCmd().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
				if (postResult.booleanValue()) {
					commitDao.commit();
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", refId, refId);
					final Map<Long, List<SegSyncInfos>> segSyncMap = Maps.newHashMap();
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					segSyncMap.put(segSyncInfo.getSegmentId(), segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());	
					mqRespose.setRequestId(requestId);
					mqRespose.setXmlResult(onejobResult.get().getResult());
					mqRespose.setDiagnostics(onejobResult.get().getDiagnostics().toByteArray());
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, eenrollmentId={}", refId, refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;					
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());				
				}				
			} else {
				//new segment must post 2 times
				segSyncInfo.setIsUpdateSegment(false);
				segSyncInfo.setSegVersion(0l);
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
				PBDmSyncRequest dmRequest2 = buildDmJobRequest(segSyncInfo);
				Boolean postResult2 = DmJobPoster.post(url, dmRequest2);
				if (postResult.booleanValue() && postResult2.booleanValue()) {
					segmentDao.updateSegmentVersin(0l, 0l, segSyncInfo.getSegmentId().longValue());
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", refId, refId);
					commitDao.commit();
					final Map<Long, List<SegSyncInfos>> segSyncMap = Maps.newHashMap();
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					segSyncMap.put(segSyncInfo.getSegmentId(), segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());	
					mqRespose.setRequestId(requestId);
					mqRespose.setXmlResult(onejobResult.get().getResult());
					mqRespose.setDiagnostics(onejobResult.get().getDiagnostics().toByteArray());
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, eenrollmentId={}", refId, refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;					
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
				}
			}
		
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			AimError aimErr = AimError.INTERNAL_ERROR;
			mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
		}	
		byte[] diagnostics = onejobResult.get().getDiagnostics().toByteArray();
		String xmlRespose = onejobResult.get().getResult();
		UidAimAmqResponse extRespose = new UidAimAmqResponse(requestId, UidRequestType.Insert.name(), xmlRespose, diagnostics);
		return extRespose;
	}

	private UidAimAmqResponse doSyncDelete(String deleteReq,String requestId, String externalId, final AtomicInteger delCount) throws IOException {
		log.info("syncData delete begin..");	
		List<SegSyncInfos> delSyncList = new ArrayList<>();
		int count = reg.delete(externalId, Integer.valueOf(1), delSyncList);
		if (count < 1 || delSyncList.size() < 1) {
			log.warn("delete count is zero, skip sync to dm service!");
			UidAimAmqResponse uidRes = new UidAimAmqResponse();
			uidRes.setRequestId(requestId);
			uidRes.setRequestType(UidRequestType.Delete.name());
			String xml = "delete count is zero";
			uidRes.setXmlResult(xml);
			return uidRes;
		}
		if (delCount != null) {
			delCount.set(count);
		}
		
		log.info("delete count={}", count);
		for (int i = 0; i < delSyncList.size(); i++) {
			SegSyncInfos one = delSyncList.get(i);
			PBDmSyncRequest dmRequest = buildDmJobRequest(one);
			String baseUrl = UidDmJobRunManager.getOneActiveDm();
			if (baseUrl == null) {
				log.warn("No active dm!");
				AimError aimErr = AimError.DM_TIME_OUT;
				UidAimAmqResponse response = new UidAimAmqResponse();
				String xmlRes = dom4jCreateFaildXml(requestId, aimErr.getUidCode());	
				response.setRequestId(requestId);
				response.setRequestType(UidRequestType.Delete.name());
				response.setXmlResult(xmlRes);
				return response;
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (postResult.booleanValue()) {
				commitDao.commit();
				final Map<Long, List<SegSyncInfos>> segSyncMap = Maps.newHashMap();
				List<SegSyncInfos> segSyncList = new ArrayList<>();
				segSyncList.add(one);
				segSyncMap.put(one.getSegmentId(), segSyncList);
				reg.pushOrCallSlb(segSyncMap, true);
			} else {
				commitDao.rollback();
			}
		}		
		
		String delXmlRes = dom4jCreateResponseXml(requestId, 2);
		UidAimAmqResponse extRespose = new UidAimAmqResponse(requestId, UidRequestType.Delete.name(), delXmlRes, null);		
		return extRespose;
	}
	
	private String dom4jCreateResponseXml(String requestId , int success) throws IOException {
		 Document document = DocumentHelper.createDocument();
		 Element root = document.addElement( "Response")
				 .addAttribute("requestId", requestId)
				 .addAttribute("timeStamp", String.valueOf(System.currentTimeMillis()));
		 root.addElement("Return").addAttribute("value", String.valueOf(success));
		 OutputFormat format = OutputFormat.createPrettyPrint();
		 StringWriter sw = new StringWriter();
         XMLWriter writer  = new XMLWriter(sw, format);        
         writer.write(document);
         String resuslt = sw.toString();		 
		return resuslt;		
	}
	
	private String dom4jCreateFaildXml(String requestId , String failureReason) throws IOException {
		 Document document = DocumentHelper.createDocument();
		 Element root = document.addElement( "Response")
				 .addAttribute("requestId", requestId)
				 .addAttribute("timeStamp", String.valueOf(System.currentTimeMillis()));
		 root.addElement("Return").addAttribute("value", String.valueOf("2")).addAttribute("failureReason", failureReason);
		 OutputFormat format = OutputFormat.createPrettyPrint();
		 StringWriter sw = new StringWriter();
        XMLWriter writer  = new XMLWriter(sw, format);        
        writer.write(document);
        String resuslt = sw.toString();		 
		return resuslt;		
	}
	
	private UidAimAmqResponse buildSuccessSyncReespose(String requestId, byte[] diagnostics) throws IOException  {
		UidAimAmqResponse response = new UidAimAmqResponse();
		String xmlRes = dom4jCreateResponseXml(requestId, 1);		
		response.setRequestId(requestId);
		response.setRequestType(UidRequestType.Insert.name());
		response.setXmlResult(xmlRes);
		response.setDiagnostics(diagnostics);
		return response;
	}	

	private UidAimAmqResponse buildFaildSyncReespose(String requestId, String failureReason) throws IOException  {
		UidAimAmqResponse response = new UidAimAmqResponse();
		String xmlRes = dom4jCreateFaildXml(requestId, failureReason);	
		response.setRequestId(requestId);
		response.setRequestType(UidRequestType.Insert.name());
		response.setXmlResult(xmlRes);
		return response;
	}

	private  PBDmSyncRequest buildDmJobRequest(SegSyncInfos segSyncInfo) {
		PBDmSyncRequest.Builder pbDmSycReq = PBDmSyncRequest.newBuilder();
		pbDmSycReq.setCmd(segSyncInfo.getCommand());
		long bioStart = segSyncInfo.getTemplateId().longValue();
		pbDmSycReq.setBioIdStart(bioStart);
		if (segSyncInfo.getCommand().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
			long maxEnd = bioStart + Long.valueOf(AimManager.getValueFromUidMMSettingMap(maxSegmetRecordCount)).longValue();
			pbDmSycReq.setBioIdEnd(maxEnd);
		} else {
			pbDmSycReq.setBioIdEnd(segSyncInfo.getTemplateId());
		}
		PBTargetSegmentVersion.Builder targetSegment = PBTargetSegmentVersion.newBuilder();
		targetSegment.setId(segSyncInfo.getSegmentId().longValue());
		targetSegment.setVersion(segSyncInfo.getSegVersion());
		pbDmSycReq.setTargetSegment(targetSegment.build());
		if (segSyncInfo.getCommand().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT)) {
			PBTemplateInfo.Builder pbTemplateInfo = PBTemplateInfo.newBuilder();
			pbTemplateInfo.setReferenceId(segSyncInfo.getExternalId());
			pbTemplateInfo.setData(ByteString.copyFrom(segSyncInfo.getTemplateData()));
			pbDmSycReq.setTemplateData(pbTemplateInfo.build());
		}
		return pbDmSycReq.build();
	}
}
