package jp.co.nec.aim.mm.sessionbeans;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import javax.xml.ws.http.HTTPException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.protobuf.InvalidProtocolBufferException;

import jp.co.nec.aim.message.proto.AIMEnumTypes.ServiceStateType;
import jp.co.nec.aim.message.proto.AIMMessages.PBMuExtractJobResult;
import jp.co.nec.aim.message.proto.AIMMessages.PBMuExtractJobResultItem;
import jp.co.nec.aim.message.proto.AIMMessages.PBServiceState;
import jp.co.nec.aim.mm.acceptor.service.AimInquiryRemote;
import jp.co.nec.aim.mm.acceptor.service.AimSyncRemote;
import jp.co.nec.aim.mm.amq.service.AmqServiceManager;
import jp.co.nec.aim.mm.constants.AimError;
import jp.co.nec.aim.mm.constants.MMConfigProperty;
import jp.co.nec.aim.mm.constants.UidRequestType;
import jp.co.nec.aim.mm.dao.DateDao;
import jp.co.nec.aim.mm.dao.FEJobDao;
import jp.co.nec.aim.mm.dao.MuLoadDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.dao.SystemInitDao;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.exception.DataBaseException;
import jp.co.nec.aim.mm.jms.JmsSender;
import jp.co.nec.aim.mm.jms.NotifierEnum;
import jp.co.nec.aim.mm.logger.FeJobDoneLogger;
import jp.co.nec.aim.mm.logger.ProtobufDumpLogger;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimServiceState;
import jp.co.nec.aim.mm.sessionbeans.pojo.ExceptionSender;
import jp.co.nec.aim.mm.sessionbeans.pojo.ExtractJobHandler;
import jp.co.nec.aim.mm.sessionbeans.pojo.UidAimAmqResponse;
import jp.co.nec.aim.mm.util.TimeHelper;
import jp.co.nec.aim.mm.util.XmlUtil;

/**
 * EJB to record search job results into DB.
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRED)
public class ExtractJobCompleteBean {
    private static final Logger log = LoggerFactory.getLogger(ExtractJobCompleteBean.class);
    
    @PersistenceContext(unitName = "aim-db")
    private EntityManager manager;
    
    @Resource(mappedName = "java:jboss/MySqlDS")
    private DataSource dataSource;
    
    private JdbcTemplate jdbcTemplate;
    
    private MuLoadDao muLoadDao;
    
    private FEJobDao feJobDao;  
    
    private DateDao dateDao;  
    
    private SystemConfigDao sysConfigDao;
    
    private SystemInitDao systemInitDao;
    
    private ExtractJobHandler extractJobHandler;
    
    private FeJobDoneLogger feJobDoneLogger;
    
    private ExceptionSender exceptionSender;
    
    @EJB(name="ejb:/matchmanager/mm-ejb/AimInquiryRemoveService!jp.co.nec.aim.mm.acceptor.service.AimInquiryRemote")    
    AimInquiryRemote inqRemote;
    
    @EJB(name="ejb:/matchmanager/mm-ejb/AimSyncRemoteService!jp.co.nec.aim.mm.acceptor.service.AimSyncRemote") 
    AimSyncRemote syncRemote;
    
   
    
    public ExtractJobCompleteBean() {
    }
    
    @PostConstruct
    public void init() {
        extractJobHandler = new ExtractJobHandler(manager, dataSource);
        feJobDoneLogger = new FeJobDoneLogger(dataSource);
        exceptionSender = new ExceptionSender();
        muLoadDao = new MuLoadDao(dataSource);
        feJobDao = new FEJobDao(manager, dataSource);         
        dateDao = new DateDao(dataSource);
        sysConfigDao = new SystemConfigDao(manager);
        systemInitDao = new SystemInitDao(manager);
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    /**
     * Update for extract jobs
     * 
     * @param extactJobResult
     * @throws InvalidProtocolBufferException
     */
    public void completeExtractJobs(PBMuExtractJobResult extactJobResult) {
        long muId = extactJobResult.getMuId();
        long lotJobId = extactJobResult.getLotJobId();
        int updatedRecordCnt = 0;
        log.info("Ready to complete Lot Extract Jobs: {}, MU id: {}.", lotJobId, muId);
        try {
            Integer maxExtractJobFailures = sysConfigDao.getMMPropertyInt(MMConfigProperty.MAX_EXTRACT_JOB_FAILURES);            
            for (PBMuExtractJobResultItem ejrItem : extactJobResult.getResultList()) {
                long exJobId = ejrItem.getJobId();
                log.info("mu PBMuExtractJobResultItem jobId= {}", exJobId);
                String extResult = ejrItem.getResult();               
                //must null check              
                PBServiceState pbState = ejrItem.getJobState();
                FeJobQueueEntity ejq = feJobDao.findFeJob(exJobId);                              
                ProtobufDumpLogger.traceAimJobResult("PBMuExtractJobResultItem", ejrItem.getJobId(), muId,extResult);  
                if (pbState.getState().equals(ServiceStateType.SERVICE_STATE_SUCCESS)) {
                    log.info("MU " + muId + " finishing extract job " + ejrItem.getJobId()); 
                    updatedRecordCnt = finishExtractJob(ejrItem,  muId, false, ejq.getRequestType());
                } else if (pbState.getState().equals(ServiceStateType.SERVICE_STATE_ERROR)) {
                    log.warn("MU " + muId + " failed extract job " + ejrItem.getJobId());
                    if (!pbState.hasReason()) {
                        throw new IllegalArgumentException(
                            "The error message must be specified when service state is error..");
                    }
                    exceptionSender.sendAimException(pbState.getReason().getCode(), pbState.getReason().getDescription(), 0,  ejrItem.getJobId(), muId, null);
                    updatedRecordCnt = finishExtractJob(ejrItem, muId, true, ejq.getRequestType());
                } else if (pbState.getState().equals(ServiceStateType.SERVICE_STATE_ROLLBACK)) {
					FeJobQueueEntity eje = manager.find(FeJobQueueEntity.class, new Long(ejrItem.getJobId()));
					manager.refresh(eje);
					if (eje == null || eje.getMuId() == null
							|| eje.getMuId() != muId) {
						log.warn("MU " + muId
								+ " attempting to report error reason '"
								+ pbState.getReason().getCode() + " : "
								+ pbState.getReason().getDescription()
								+ "' of nonexistant or unassigned job "
								+ ejrItem.getJobId() + ", ignoring...");
					} else {
						AimServiceState simState = new AimServiceState();
						AimServiceState aimServiceState = new AimServiceState();
						aimServiceState.setErrMsg(pbState.getReason().getDescription());
						aimServiceState.setErrorcode(pbState.getReason().getCode());
						aimServiceState.setFailureTime( pbState.getReason().getTime());
						updatedRecordCnt = extractJobHandler.failExtractJob(ejrItem, eje, muId, simState, false, maxExtractJobFailures);										
					}
                }
                
                /* send Jms to ExtractJobPlanner */
                JmsSender.getInstance().sendToFEJobPlanner(NotifierEnum.FEJobCompleteBean, "Recieve Extract Job " + ejrItem.getJobId());          
            }
        } catch (SQLException sqlEx) {
            AimError dbErr = AimError.EXTRACT_DB;
            String errMsg = String.format(dbErr.getMessage(), sqlEx.getMessage());
            dbErr.setMessage(errMsg);
            
            throw new DataBaseException(dbErr.getErrorCode(), dbErr.getMessage(), String.valueOf(System.currentTimeMillis()), dbErr.getUidCode());
        } catch (InvalidProtocolBufferException pex) {
            AimError pbErr = AimError.PROTOBUF_ERROR;
            String errMsg = String.format(pbErr.getMessage(), "at do extract job complete process");
            errMsg = errMsg + String.format(pex.getMessage());
            pbErr.setMessage(errMsg);
            throw new DataBaseException(pbErr.getErrorCode(), pbErr.getMessage(), String.valueOf(System.currentTimeMillis()), pbErr.getUidCode());
            
        } finally {
            log.info("Ready to delete Lot Extract Jobs: {}" + " and decrease ExtractLoad with MU id: {}.", lotJobId, muId);
            feJobDao.deleteLotJob(extactJobResult.getLotJobId());
            if (updatedRecordCnt > 0) {
                muLoadDao.decreaseExtractLoad(muId);
                // increaseExtractJobCompleteCount(updatedRecordCnt);
            }
        }
    }    
   
    
    /**
     * Finish extract job normally.
     * 
     * @param jobId
     * @param muId
     * @param itemResult
     * @param failed
     * @throws SQLException
     * @throws InvalidProtocolBufferException
     * @throws NamingException 
     * @throws IOException
     * @throws HTTPException
     */
    private int finishExtractJob(PBMuExtractJobResultItem ejrItem, long muId, boolean failed,  String reqType)       
        throws SQLException, InvalidProtocolBufferException {
        TimeHelper th = new TimeHelper("finishExtractJob");
        th.t();  
        long jobId = ejrItem.getJobId(); 
       FeJobQueueEntity feJobInfo = feJobDao.getFeJobInfoById(jobId);       
        String jobResult = ejrItem.getResult(); //must null check       
        byte[] template  = ejrItem.getBisonTemplate().toByteArray();//must null check       
        byte[] diagnostics = ejrItem.getDiagnostics().toByteArray(); //must null check         
        int numCompleted = extractJobHandler.completeExtractJob(jobId, muId, jobResult, template, diagnostics, failed);      
        log.info("MM get MuExtractJobResultItem. jobId:{} for requestType", jobId, reqType);
        Context jndiContext = null;	
        if (reqType.equals(UidRequestType.Identify.name())) {
        	inqRemote.getExtResAndDoInquriy(ejrItem);
//			try {
//				jndiContext = getContext(feJobInfo.getCallbackUrl());
//				inqRemote = (AimInquiryRemote) jndiContext
//						.lookup("ejb:/matchmanager/mm-ejb/AimInquiryRemoveService!jp.co.nec.aim.mm.acceptor.service.AimInquiryRemote");
//				inqRemote.getExtResAndDoInquriy(ejrItem);
//			} catch (NamingException e) {
//				AimError aimError = AimError.INTERNAL_ERROR;				
//				String requestId = feJobInfo.getRequestId();							
//				UidAimAmqResponse response = XmlUtil.buildFaildXmlReespose(requestId, aimError.getUidCode());
//				AmqServiceManager.getInstance().addToAmqQueue(response);	
//			} 
        } else  if (reqType.equals(UidRequestType.Insert.name())) { 
        	 syncRemote.getExtResAndDoSync(ejrItem);
//     			try {  
//     			  jndiContext = getContext(feJobInfo.getCallbackUrl());
//     			  syncRemote = (AimSyncRemote) jndiContext
//     						.lookup("ejb:/matchmanager/mm-ejb/AimSyncRemoteService!jp.co.nec.aim.mm.acceptor.service.AimSyncRemote");
//     			 syncRemote.getExtResAndDoSync(ejrItem);
//     			} catch (NamingException e) {	
//     				log.error(e.getMessage(), e);
//     				AimError aimError = AimError.INTERNAL_ERROR;				
//    				String requestId = feJobInfo.getRequestId();							
//    				UidAimAmqResponse response = XmlUtil.buildFaildXmlReespose(requestId, aimError.getUidCode());
//    				AmqServiceManager.getInstance().addToAmqQueue(response);		
//     			} 
        }        
        else if (reqType.equals(UidRequestType.Quality.name())) {         	
        	String reqFrom = systemInitDao.getRequestFromSetting();
        	if (reqFrom.toUpperCase().equals("AMQ")) { 
        		log.info("In ExtractJobCompleteBean, Extract result:{}", jobResult);
        	    sendXmlResToAmq(jobResult);
        	}
                
        }
        feJobDoneLogger.info(jobId);
        if (failed) {
            log.warn("{} is faild, error info:{}", jobId, ejrItem.getJobState().getReason().getDescription());
            feJobDao.deleteExtractJob(jobId);
            numCompleted = 0;
        }
        
        th.t();
        if (log.isDebugEnabled()) {
            log.debug(th.message());
        }
        return numCompleted;
    }
    
    public void increaseExtractJobCompleteCount(int finishedFeJobs) {
        Long updateTime = dateDao.getCurrentTimeMS();
        String updateSql = "" + "UPDATE extract_complete_count " + "SET complete_count = COMPLETE_COUNT + ?, "
            + " complete_ts = ?";
        jdbcTemplate.update(updateSql, new Object[] {
            new Integer(finishedFeJobs), updateTime
        });
    }
    
    private void sendXmlResToAmq(String extRes) {
    	AmqServiceManager.getInstance().addToAmqXmlQueue(UidRequestType.Quality.name(), extRes);    	
    } 
    
    @SuppressWarnings("unused")
	private Context getContext(String remoteUrl) throws NamingException {
		Properties jndiProperties = new Properties();
		jndiProperties.put(Context.INITIAL_CONTEXT_FACTORY, "org.wildfly.naming.client.WildFlyInitialContextFactory");
		jndiProperties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
		jndiProperties.put(Context.PROVIDER_URL, "http-remoting://" + remoteUrl);
		jndiProperties.put("jboss.naming.client.ejb.context", true);
		//jndiProperties.put("remote.connection.default.username", true);
 		//jndiProperties.put("oremote.connection.default.password", true);
		jndiProperties.put(Context.SECURITY_PRINCIPAL, "client");
		jndiProperties.put(Context.SECURITY_CREDENTIALS, "q");

		Context jndiContext = new InitialContext(jndiProperties);
		return jndiContext;
	}    
  
}
