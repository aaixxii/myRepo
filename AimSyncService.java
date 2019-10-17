package jp.co.nec.aim.mm.acceptor.service;

import static jp.co.nec.aim.mm.constants.MMConfigProperty.INTERVALS_CLIENT_RESPONSE_TIMEOUT;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import jp.co.nec.aim.message.proto.AIMMessages.PBMuExtractJobResultItem;
import jp.co.nec.aim.message.proto.BusinessMessage.PBBusinessMessage;
import jp.co.nec.aim.message.proto.BusinessMessage.PBResponse;
import jp.co.nec.aim.message.proto.BusinessMessage.PBTemplateInfo;
import jp.co.nec.aim.message.proto.SyncService.SyncRequest;
import jp.co.nec.aim.message.proto.SyncService.SyncResponse;
import jp.co.nec.aim.mm.acceptor.AimSyncRequest;
import jp.co.nec.aim.mm.acceptor.Record;
import jp.co.nec.aim.mm.acceptor.Registration;
import jp.co.nec.aim.mm.constants.JobState;
import jp.co.nec.aim.mm.constants.MMConfigProperty;
import jp.co.nec.aim.mm.dao.DateDao;
import jp.co.nec.aim.mm.dao.FEJobDao;
import jp.co.nec.aim.mm.dao.FunctionDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.entities.BatchJobInfoEntity;
import jp.co.nec.aim.mm.entities.FeJobPayloadEntity;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.entities.FunctionTypeEntity;
import jp.co.nec.aim.mm.exception.AimRuntimeException;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimManager;
import jp.co.nec.aim.mm.validator.AcceptorValidator;
import jp.co.nec.aim.mm.validator.TemplateValidator;

/**
 * The main work flow of Sync <br>
 * 
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
	@Resource(mappedName = "java:jboss/OracleDS")
	private DataSource dataSource;
	@EJB
	private TemplateValidator templateValidator;
	private Registration reg;	
	private AcceptorValidator validator;	
	private FEJobDao feJobDao;	
	private SystemConfigDao sysConfigDao;
	private DateDao dateDao;
	private FunctionDao functionDao;
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
		this.validator = new AcceptorValidator(manager, dataSource);		
		this.feJobDao = new FEJobDao(manager);		
		this.functionDao = new FunctionDao(manager);
		this.sysConfigDao = new SystemConfigDao(manager);
		this.dateDao = new DateDao(dataSource);
	}

	/**
	 * The main work flow of syncData
	 * 
	 * @param request
	 *            PBSyncJobRequest instance
	 * @return the instance of PBSyncJobResponse
	 */
	public SyncResponse syncData(final SyncRequest request) {
		return syncData(request, null);
	}

	/**
	 * The main work flow of syncData
	 * 
	 * @param syncRequest
	 *            PBSyncJobRequest instance
	 * @param delCount
	 *            delete count
	 * @return PBSyncJobResponse instance
	 */
	public SyncResponse syncData(final SyncRequest request,
			final AtomicInteger delCount) {
		// first check the parameter PBSyncJobRequest
		// if any error occurred, throw IllegalArgumentException
		// servLet will response 400 bad request
		// at the meanwhile convert to new Request
		PBBusinessMessage pbm = validator.checkSyncJobRequest(request);		
		final String externalId = pbm.getRequest().getEnrollmentId();
		String syncType  = pbm.getRequest().getRequestType().name();
		log.info("received sync request, externalId = {}",externalId);
		switch (syncType) {
		case "INSERT_REFID_DEFAULT":
			if (log.isDebugEnabled()) {
				log.info("syncData insert by refernceId begin..");
			}		
			//PBMuExtractJobResultItem muResult = null;
			PBBusinessMessage pbMes = null;
			try {
				String refernceId = pbm.getRequest().getEnrollmentId();			
				 FeJobQueueEntity feEntity = feJobDao.getExResult(refernceId).get(0);
				byte [] result = feEntity.getResult();
				//muResult = PBMuExtractJobResultItem.parseFrom(result);
				pbMes = PBBusinessMessage.parseFrom(result);
			} catch (InvalidProtocolBufferException e1) {
				throw new AimRuntimeException(e1.getMessage(),e1.getCause());
			}	
			if (pbMes.hasDataBlock() && pbMes.getDataBlock().hasTemplateInfo()) {
				
				throw new AimRuntimeException("result form fe_job_queue no template!");
			}
			PBTemplateInfo templateData = pbMes.getDataBlock().getTemplateInfo();			
			final List<AimSyncRequest> aimRequestList = new ArrayList<>();
			Record record = new Record(templateData.toByteArray());
			AimSyncRequest aimReq = new AimSyncRequest(Integer.valueOf(1), record);
			aimRequestList.add(aimReq);
			reg.insert(0,externalId, aimRequestList);
			
			SyncResponse.Builder insByRefIdRes = SyncResponse.newBuilder();
			insByRefIdRes.setBatchJobId(request.getBatchJobId());
			insByRefIdRes.setType(request.getType());
			PBBusinessMessage.Builder newPbMes = PBBusinessMessage.newBuilder();
			newPbMes.setRequest(pbm.getRequest());
			PBResponse.Builder refIdPbRespose = PBResponse.newBuilder();
			refIdPbRespose.setStatus("0");
			newPbMes.setResponse(refIdPbRespose);			
			insByRefIdRes.addBusinessMessage(newPbMes.build().toByteString());
			return insByRefIdRes.build();			
		case "INSERT_REFURL_DEFAULT":
			if (log.isDebugEnabled()) {
				log.debug("syncData insert by url begin..");
			}
			final FeJobQueueEntity feJob = new FeJobQueueEntity();
			FunctionTypeEntity fte = functionDao.getExtractFunction();
			if (fte == null) {
				throw new AimRuntimeException(
						"Extract function not registered in the database.");
			}
			feJob.setFunctionId((int) fte.getId());
			feJob.setReferenceId(externalId);
			feJob.setStatus(JobState.QUEUED);
			feJob.setFailureCount(0l);	
			Integer priority = sysConfigDao.getMMPropertyInt(MMConfigProperty.EXTRACT_DEFAULTS_PRIORITY);
			feJob.setPriority(priority);
			feJob.setSubmissionTs(dateDao.getCurrentTimeMS());				
			manager.persist(feJob);
			FeJobPayloadEntity epe = new FeJobPayloadEntity();
			epe.setPayload(pbm.toByteArray());
			epe.setJobId(feJob.getId());
			manager.persist(epe);
			//manager.flush();
			BatchJobInfoEntity batchInfo = new BatchJobInfoEntity();
			batchInfo.setBatchJobId(request.getBatchJobId());
			batchInfo.setBatchType(request.getType().name());
			batchInfo.setReqeustId(pbm.getRequest().getRequestId());
			batchInfo.setInternalJobId(feJob.getId());		
			manager.persist(batchInfo);
			Integer extractJobWaitTime = sysConfigDao.getMMPropertyInt(INTERVALS_CLIENT_RESPONSE_TIMEOUT);			
			if (Objects.isNull(extractJobWaitTime) || extractJobWaitTime < 0 ) {
				extractJobWaitTime = 10000;
			}
					
			Object extractJoblocker = new Object();				
			PBMuExtractJobResultItem onejobResult = null;
			synchronized (extractJoblocker) {
				long startGetExtResultTime = System.currentTimeMillis();
				log.info("Go to waiting  job results!");
				try {
					extractJoblocker.wait(extractJobWaitTime);
				} catch (InterruptedException e) {
					log.error(e.getMessage(), e);
					Thread.currentThread().interrupt();
				}			
				onejobResult = AimManager.getExtractJobResult(String.valueOf(feJob.getId()));
				if (onejobResult != null) {
					log.info("Get insert by url job({}) result success", feJob.getId());
					long endGetResultTime = System.currentTimeMillis();
					log.info("*****MM get insert by url job results used time = {}****",
							endGetResultTime - startGetExtResultTime);				
				} else {
					long currentTime = System.currentTimeMillis();			
					if (currentTime -  startGetExtResultTime >= extractJobWaitTime) {
						log.warn(
								"Timeout is happend! the waiting time = ({}), jobId({})",
								currentTime - startGetExtResultTime, feJob.getId());
					} 
				}			
				AimManager.finishExtractJob(feJob.getId());			
			}
			PBBusinessMessage pbExtMes = null;
			try {
				pbExtMes = PBBusinessMessage.parseFrom(onejobResult.getResult());
			} catch (InvalidProtocolBufferException e) {				
				e.printStackTrace();
			}
			PBTemplateInfo exTemplateData = pbExtMes.getDataBlock().getTemplateInfo();			
			final List<AimSyncRequest> aimRequestByExtList = new ArrayList<>();
			Record recordByExt = new Record(exTemplateData.toByteArray());
			AimSyncRequest aimReqExt = new AimSyncRequest(Integer.valueOf(1), recordByExt);
			aimRequestByExtList.add(aimReqExt);
			reg.insert(null,externalId, aimRequestByExtList);	
			SyncResponse.Builder insByUrlRes = SyncResponse.newBuilder();
			insByUrlRes.setBatchJobId(request.getBatchJobId());
			insByUrlRes.setType(request.getType());
			insByUrlRes.addBusinessMessage(pbExtMes.toByteString());
			return insByUrlRes.build();	
		case "DELETE_REFID":
			if (log.isDebugEnabled()) {
				log.debug("syncData delete begin..");
			}			
			int count = reg.delete(null, externalId, Lists.newArrayList(new Integer[] {1}));
			if (delCount != null) {
				delCount.set(count);
			}	
			SyncResponse.Builder delRes = SyncResponse.newBuilder();
			delRes.setBatchJobId(request.getBatchJobId());
			delRes.setType(request.getType());				
			PBBusinessMessage oldPbMes = null;
			PBBusinessMessage.Builder dlePbMes = PBBusinessMessage.newBuilder();
			try {
				oldPbMes = PBBusinessMessage.parseFrom(request.getBusinessMessage(0));
				dlePbMes.setRequest(oldPbMes.getRequest());
				PBResponse.Builder delPbRespose = PBResponse.newBuilder();
				delPbRespose.setStatus("success");
				dlePbMes.setResponse(delPbRespose);
			} catch (InvalidProtocolBufferException e) {
				throw new AimRuntimeException(e.getMessage(),e.getCause());
			}			
			delRes.addBusinessMessage(dlePbMes.build().toByteString());
			return delRes.build();			
			
		default:
			throw new IllegalArgumentException("E_REQUESET_TYPE:" + syncType
					+ " is not support.");
		}
	}	
}
