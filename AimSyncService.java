package jp.co.nec.aim.mm.acceptor.service;

import static jp.co.nec.aim.mm.constants.Constants.maxSegmetRecordCount;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import jp.co.nec.aim.mm.dao.MatchManagerDao;
import jp.co.nec.aim.mm.dao.PersonBiometricDao;
import jp.co.nec.aim.mm.dao.SegmentChangeLogDao;
import jp.co.nec.aim.mm.dao.SegmentDao;
import jp.co.nec.aim.mm.dm.client.DmJobPoster;
import jp.co.nec.aim.mm.dm.client.mgmt.UidDmJobRunManager;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.entities.FunctionTypeEntity;
import jp.co.nec.aim.mm.entities.PersonBiometricEntity;
import jp.co.nec.aim.mm.exception.DataBaseException;
import jp.co.nec.aim.mm.jms.JmsSender;
import jp.co.nec.aim.mm.jms.NotifierEnum;
import jp.co.nec.aim.mm.procedure.FeJobProcedures;
import jp.co.nec.aim.mm.segment.sync.SegSyncInfos;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimManager;
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
	

	private FunctionDao functionDao;
	
	private SegmentDao segmentDao;
	
	private SegmentChangeLogDao segmentChangeLogDao;
	
	private MatchManagerDao mmDao;

	private FeJobProcedures feJobProcedures;
	
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
		this.feJobDao = new FEJobDao(manager, dataSource);
		this.personBiometricDao = new PersonBiometricDao(manager);
		this.functionDao = new FunctionDao(manager);		
		this.segmentDao = new SegmentDao(manager);	
		this.segmentChangeLogDao = new SegmentChangeLogDao(dataSource);
		this.mmDao = new MatchManagerDao(manager);		
		feJobProcedures = new FeJobProcedures(dataSource);
		this.commitDao = new CommitDao(dataSource);
	}
	
	public void syncDataByAmq(String syncRequest) throws IOException {
		syncData(syncRequest, null);		
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
	public void syncData(final String syncRequest, final AtomicInteger delCount) throws IOException {
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
			AcceptorValidator.checkSyncDeleteRequest(syncRequest, requestId, cmd, refId);
			doSyncDelete(syncRequest, requestId, refId, delCount);
		}
		if (cmd.equals("Insert")) {
			AcceptorValidator.checkSyncInsertRequest(syncRequest, requestId, cmd, refId, refUrl);
			if (!StringUtils.isBlank(refId)) {
				PersonBiometricEntity pbEntity = personBiometricDao.isHaveBioMetricDataInMyTable(refId);
				if (pbEntity != null) {
					log.info("The template is already in Bison(NSM) do nothing"); 
					response = new UidAimAmqResponse();
					response.setRequestId(requestId);
					response.setRequestType(UidRequestType.Insert.name());
					response.setXmlResult("The template is already in Bison(NSM) do nothing");
					sendToAmq(response);					
					return;
				}	
				
				boolean hasTemplateInFeQueue = feJobDao.hasTempalte(refId);
				if (hasTemplateInFeQueue) {
					UidAimAmqResponse refIdResult =  doSyncByRefId(syncRequest, refId, requestId);
					if (refIdResult != null) {
					    sendToAmq(refIdResult);						
						return ;	
					}									
				}
			}
			
			if (!StringUtils.isBlank(refId) && !StringUtils.isBlank(refUrl)) {
				doSyncByRefUrl(syncRequest, refId, refUrl);
			} 
		}
	}
	
	public UidAimAmqResponse callSyncByRefId(final String syncRequest, String requestId, String refId) {
		UidAimAmqResponse response = null;
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
			try {
				response = doSyncByRefId(syncRequest, refId, requestId);
			} catch (IOException e) {
				commitDao.rollback();
				log.error(e.getMessage(), e);
				AimError aimErr = AimError.INTERNAL_ERROR;
				response = buildFaildSyncReespose(requestId, aimErr.getUidCode());
			}								
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
			AimError aimErr = AimError.SYNC_DB;
			mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
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
				AimError aimErr = AimError.INTERNAL_ERROR;
				mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
				return mqRespose;
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl +"/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;			
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (!dmRequest.getCmd().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
				if (postResult.booleanValue()) {
					commitDao.commit();
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", requestId, refId);					
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);					
					final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);				
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());				
					mqRespose = buildSuccessSyncReespose(requestId, feEntity.getDiagnostcs());				
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, eenrollmentId={}", requestId, refId);
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
				if (postResult2.booleanValue()) {
					segmentDao.updateSegmentVersin(0l, 0l, segSyncInfo.getSegmentId().longValue());
					segmentChangeLogDao.insertToSegChangeLog(segSyncInfo);					
					commitDao.commit();	
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, eenrollmentId={}", requestId, refId);					
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);					
					final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());				
					mqRespose = buildSuccessSyncReespose(requestId, feEntity.getDiagnostcs());
				} else {
					log.info("DmService retrun false,rollback transaction. reqeustId={}, eenrollmentId={}", requestId, refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;					
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
				}
			} 
		} catch (Exception e) {
			commitDao.rollback();
			log.error(e.getMessage(), e);
			AimError aimErr = AimError.INTERNAL_ERROR;
			mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
		}	
		return mqRespose;
	}

	public void doSyncByRefUrl(final String syncRequest, String refId, String refUrl) throws IOException {		
		log.info("syncData insert by url begin..");	
		// final FeJobQueueEntity feJob = new FeJobQueueEntity();
		String requestId = XmlUtil.getRequestId(syncRequest);
		FunctionTypeEntity fte = functionDao.getExtractFunction();
		if (fte == null) {
			AimError dbErr = AimError.SYNC_FUNCTIONTYPE;			
			throw new DataBaseException(dbErr.getErrorCode(), dbErr.getMessage(), String.valueOf(System.currentTimeMillis()), dbErr.getUidCode());
		}
		
		String myUrl = mmDao.getMyIpAndPort();
		Long newFeJobId = null;
		try {
			newFeJobId = feJobProcedures.createNewFeJob(refId, requestId, UidRequestType.Insert.name(), myUrl, syncRequest);
			commitDao.commit();
		} catch (Exception e1) {
			feJobDao.deleteExtractJob(newFeJobId);			
			log.error(e1.getMessage(), e1);
			AimError dbErr = AimError.SYNC_DB;
			String errMsg = dbErr.getMessage() + "," + e1.getMessage();
			errMsg = String.format(errMsg);			
			dbErr.setMessage(errMsg);
			UidAimAmqResponse uidRes = XmlUtil.buildFaildXmlReespose(requestId, dbErr.getUidCode());	
			sendToAmq(uidRes);
			commitDao.rollback();
			return;
		}

		JmsSender.getInstance().sendToFEJobPlanner(NotifierEnum.ExtractService,
				String.format("create extract job id: %s", newFeJobId));
		log.info("Notify to FEJobPlanner, jobId ={}", newFeJobId);
	}

	public void doSyncDelete(String deleteReq,String requestId, String externalId, final AtomicInteger delCount) throws IOException {
		log.info("syncData delete begin..");	
		List<SegSyncInfos> delSyncList = new ArrayList<>();
		
		int count = reg.delete(externalId, Integer.valueOf(1), delSyncList);
		if (count < 1 || delSyncList.size() < 1) {
			commitDao.rollback();
			log.warn("delete count is zero, skip sync to dm service!");
			UidAimAmqResponse uidRes = new UidAimAmqResponse();
			uidRes.setRequestId(requestId);
			uidRes.setRequestType(UidRequestType.Delete.name());
			String xml = "delete count is zero";
			uidRes.setXmlResult(xml);
			sendToAmq(uidRes);			
			return ;
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
				sendToAmq(response);				
				return;
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (postResult.booleanValue()) {
				commitDao.commit();				
				List<SegSyncInfos> segSyncList = new ArrayList<>();
				segSyncList.add(one);				
				final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);
				reg.pushOrCallSlb(segSyncMap, true);
			} else {
				commitDao.rollback();
				log.info("Post to dm is false, rollback transaction!");
			}
		}		
		
		String delXmlRes = dom4jCreateResponseXml(requestId, 2);
		UidAimAmqResponse extRespose = new UidAimAmqResponse(requestId, UidRequestType.Delete.name(), delXmlRes, null);	
		sendToAmq(extRespose);		
		return;
	}

    private void sendToAmq(UidAimAmqResponse mqRespose) throws IOException {
        Integer port = AmqExecutorManager.getInstance().getCallbackIpPort("1");
        byte[] uidResData = ObjectUtil.serializeAmqResults(mqRespose);
        AmqSocketSender sendTask = new AmqSocketSender(port, uidResData);
        AmqExecutorManager.getInstance().commitTask(sendTask);
    }	
	
	public UidAimAmqResponse syncDelete(String deleteReq,String requestId, String externalId, final AtomicInteger delCount) throws IOException {
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
				commitDao.rollback();
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
				List<SegSyncInfos> segSyncList = new ArrayList<>();
				segSyncList.add(one);				
				final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);
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

	private UidAimAmqResponse buildFaildSyncReespose(String requestId, String failureReason)   {
		UidAimAmqResponse response = new UidAimAmqResponse();
		String xmlRes = null;
		try {
			xmlRes = dom4jCreateFaildXml(requestId, failureReason);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			xmlRes = "Create response xml faild";
		}	
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
	
	private  Map<Long, List<SegSyncInfos>> listToMap(List<SegSyncInfos> syncList) {
		final Map<Long, List<SegSyncInfos>> groupBySeg = Maps.newHashMap();
		for (SegSyncInfos one : syncList) {
			Long segId = one.getSegmentId();
			if (!groupBySeg.containsKey(segId)) {
				groupBySeg.put(segId, new ArrayList<SegSyncInfos>());
			}
			groupBySeg.get(segId).add(one);
		}
		return groupBySeg;
	}
}
