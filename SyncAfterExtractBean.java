package jp.co.nec.aim.mm.sessionbeans;

import static jp.co.nec.aim.mm.constants.Constants.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;

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
import jp.co.nec.aim.mm.dao.SegmentChangeLogDao;
import jp.co.nec.aim.mm.dao.SegmentDao;
import jp.co.nec.aim.mm.dao.SystemInitDao;
import jp.co.nec.aim.mm.dm.client.DmJobPoster;
import jp.co.nec.aim.mm.dm.client.mgmt.UidDmJobRunManager;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.segment.sync.SegSyncInfos;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimManager;
import jp.co.nec.aim.mm.sessionbeans.pojo.UidAimAmqResponse;
import jp.co.nec.aim.mm.util.ObjectUtil;

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
@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
public class SyncAfterExtractBean {
	/** log instance **/
	private static Logger log = LoggerFactory.getLogger(SyncAfterExtractBean.class);

	@PersistenceContext(unitName = "aim-db")
	private EntityManager manager;

	@Resource(mappedName = "java:jboss/MySqlDS")
	private DataSource dataSource;

	private Registration reg;

	private FEJobDao feJobDao;

	private SegmentDao segmentDao;

	private SegmentChangeLogDao segmentChangeLogDao;

	private SystemInitDao systemInitDao;

	private CommitDao commitDao;

	/**
	 * default constructor
	 */
	public SyncAfterExtractBean() {
	}

	@PostConstruct
	private void init() {
		this.reg = new Registration(dataSource, manager);
		this.feJobDao = new FEJobDao(manager, dataSource);
		this.segmentDao = new SegmentDao(manager);
		this.segmentChangeLogDao = new SegmentChangeLogDao(dataSource);
		this.systemInitDao = new SystemInitDao(manager);
		this.commitDao = new CommitDao(dataSource);
	}

	public void getExtResAndDoSync(long feJobId) throws IOException {
		FeJobQueueEntity feJobInfo = feJobDao.getFeJobInfoById(feJobId);
		String requestId = feJobInfo.getRequestId();
		String refId = feJobInfo.getReferenceId();
		log.info("Get insert by url jobId({}) result success", feJobId);
		UidAimAmqResponse mqRespose = new UidAimAmqResponse();
		Record recordByExt = new Record(feJobInfo.getTempalteData(), true);
		AimSyncRequest aimReqExt = new AimSyncRequest(Integer.valueOf(1), recordByExt);
		try {
			SegSyncInfos segSyncInfo = reg.insert(refId, aimReqExt);
			if (!segSyncInfo.isUpdateSegment()) {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW);
			} else {
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
			}
			segSyncInfo.setExternalId(refId);
			segSyncInfo.setTemplateData(feJobInfo.getTempalteData());
			log.info("success insert into PERSON_BIOMETRICS table(not yet commit) reqeustId={}, enrollmentId={}",
					requestId, refId);
			PBDmSyncRequest dmRequest = buildDmJobRequest(segSyncInfo);
			String baseUrl = UidDmJobRunManager.getOneActiveDm();
			if (baseUrl == null) {
				log.warn("No active dm!");
				commitDao.rollback();
				AimError aimErr = AimError.INQ_INTERNAL;
				mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
				sendResult(mqRespose);
				return;
			}
			baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
			String url = baseUrl + jp.co.nec.aim.mm.constants.Constants.dmSync;
			Boolean postResult = DmJobPoster.post(url, dmRequest);
			if (!dmRequest.getCmd().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
				if (postResult.booleanValue()) {
					commitDao.commit();
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, enrollmentId={}", requestId,
							refId);
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());
					mqRespose.setRequestId(requestId);
					mqRespose.setXmlResult(feJobInfo.getResult());
					mqRespose.setDiagnostics(feJobInfo.getDiagnostcs());
				} else {
					log.info("DmService retrun false rollback transaction. reqeustId={}, enrollmentId={}", requestId,
							refId);
					commitDao.rollback();
					AimError aimErr = AimError.SYNC_DM_SERVICE_POST_ERROR;
					mqRespose = buildFaildSyncReespose(requestId, aimErr.getUidCode());
				}
			} else {
				// new segment must post 2 times
				segSyncInfo.setIsUpdateSegment(false);
				segSyncInfo.setSegVersion(0l);
				segSyncInfo.setCommand(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
				PBDmSyncRequest dmRequest2 = buildDmJobRequest(segSyncInfo);
				Boolean postResult2 = DmJobPoster.post(url, dmRequest2);
				if (postResult2.booleanValue()) {
					segmentDao.updateSegmentVersin(0l, 0l, segSyncInfo.getSegmentId().longValue());
					segmentChangeLogDao.insertToSegChangeLog(segSyncInfo);
					log.info("success insert into PERSON_BIOMETRICS table reqeustId={}, enrollmentId={}", requestId,
							refId);
					commitDao.commit();
					List<SegSyncInfos> segSyncList = new ArrayList<>();
					segSyncList.add(segSyncInfo);
					final Map<Long, List<SegSyncInfos>> segSyncMap = listToMap(segSyncList);
					reg.pushOrCallSlb(segSyncMap, segSyncInfo.isUpdateSegment());
					mqRespose.setRequestId(requestId);
					mqRespose.setXmlResult(feJobInfo.getResult());
					mqRespose.setDiagnostics(feJobInfo.getDiagnostcs());
				} else {
					log.info("DmService retrun false, rollback transaction. reqeustId={}, enrollmentId={}", requestId,
							refId);
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
		sendResult(mqRespose);
		return;

	}

	public void getExtRes(long feJobId) {
		FeJobQueueEntity feJobInfo = feJobDao.getFeJobInfoById(feJobId);
		String requestId = feJobInfo.getRequestId();
		log.info("Get insert by url jobId({}) result success", feJobId);
		log.info("For job with requestId ='{}' and retryFlag = true, template is already in Bison."
				+ " Therefore, only the extraction result is returned.", requestId);
		UidAimAmqResponse mqRespose = new UidAimAmqResponse();
		byte[] diagnostics = feJobInfo.getDiagnostcs();
		String xmlRespose = feJobInfo.getResult();
		mqRespose = new UidAimAmqResponse(requestId, UidRequestType.Insert.name(), xmlRespose, diagnostics);
		log.info("Success do finished sync by url, fejobId={}", feJobId);
		sendResult(mqRespose);	
	}
	
	private void sendResult(UidAimAmqResponse mqRespose) {
		String reqFrom = systemInitDao.getRequestFromSetting();
    	if (reqFrom.toUpperCase().equals("AMQ")) { 
    		log.info("Send sync by url result to amq..");      		
    		try {
                sendToAmq(mqRespose);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
    	} else {    		
    	}		
	}	

	private void sendToAmq(UidAimAmqResponse mqRespose) throws IOException {
		Integer port = AmqExecutorManager.getInstance().getCallbackIpPort("1");
		byte[] uidResData = ObjectUtil.serializeAmqResults(mqRespose);
		AmqSocketSender sendTask = new AmqSocketSender(port, uidResData);
		AmqExecutorManager.getInstance().commitTask(sendTask);
	}

	private String dom4jCreateFaildXml(String requestId, String failureReason) throws IOException {
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement("Response").addAttribute("requestId", requestId).addAttribute("timeStamp",
				String.valueOf(System.currentTimeMillis()));
		root.addElement("Return").addAttribute("value", String.valueOf("2")).addAttribute("failureReason",
				failureReason);
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setNewLineAfterDeclaration(false);
		StringWriter sw = new StringWriter();
		XMLWriter writer = new XMLWriter(sw, format);
		writer.write(document);
		String resuslt = sw.toString();
		return resuslt;
	}

	private UidAimAmqResponse buildFaildSyncReespose(String requestId, String failureReason) throws IOException {
		UidAimAmqResponse response = new UidAimAmqResponse();

		String xmlRes = null;
		xmlRes = dom4jCreateFaildXml(requestId, failureReason);
		response.setRequestId(requestId);
		response.setRequestType(UidRequestType.Insert.name());
		response.setXmlResult(xmlRes);
		return response;
	}

	private PBDmSyncRequest buildDmJobRequest(SegSyncInfos segSyncInfo) {
		PBDmSyncRequest.Builder pbDmSycReq = PBDmSyncRequest.newBuilder();
		pbDmSycReq.setCmd(segSyncInfo.getCommand());
		long bioStart = segSyncInfo.getTemplateId().longValue();
		pbDmSycReq.setBioIdStart(bioStart);
		if (segSyncInfo.getCommand().equals(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_NEW)) {
			long maxEnd = bioStart
					+ Long.valueOf(AimManager.getValueFromUidMMSettingMap(maxSegmetRecordCount)).longValue() - 1;
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

	private Map<Long, List<SegSyncInfos>> listToMap(List<SegSyncInfos> syncList) {
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
