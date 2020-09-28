package jp.co.nec.aim.mm.sessionbeans;

import static jp.co.nec.aim.mm.constants.Constants.dmGetTemplateByRefId;
import static jp.co.nec.aim.mm.constants.Constants.dmSync;
import static jp.co.nec.aim.mm.constants.Constants.hazelcastConfigPath;
import static jp.co.nec.aim.mm.constants.Constants.maxSegmetRecordCount;
import static jp.co.nec.aim.mm.constants.Constants.oneTemplateSize;

import java.io.File;
import java.util.List;

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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import jp.co.nec.aim.mm.constants.AimError;
import jp.co.nec.aim.mm.constants.MMEventType;
import jp.co.nec.aim.mm.dao.DateDao;
import jp.co.nec.aim.mm.dao.DmServiceDao;
import jp.co.nec.aim.mm.dao.InquiryJobDao;
import jp.co.nec.aim.mm.dao.MMEventsDao;
import jp.co.nec.aim.mm.dao.MatchManagerDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.dao.SystemInitDao;
import jp.co.nec.aim.mm.dm.client.heartbeat.SocketHeatBeatSender;
import jp.co.nec.aim.mm.dm.client.mgmt.UidDmJobRunManager;
import jp.co.nec.aim.mm.entities.ContainerJobEntity;
import jp.co.nec.aim.mm.entities.DmServiceEntity;
import jp.co.nec.aim.mm.entities.MatchManagerEntity;
import jp.co.nec.aim.mm.entities.UnitState;
import jp.co.nec.aim.mm.exception.AimRuntimeException;
import jp.co.nec.aim.mm.exception.UidDmClientException;
import jp.co.nec.aim.mm.hazelcast.HazelcastService;
import jp.co.nec.aim.mm.logger.PerformanceLogger;
import jp.co.nec.aim.mm.procedure.InitExecutingJobCountProcedure;
import jp.co.nec.aim.mm.scheduler.QuartzManager;
import jp.co.nec.aim.mm.segment.sync.SyncThreadExecutor;
import jp.co.nec.aim.mm.sessionbeans.pojo.AimManager;
import jp.co.nec.aim.mm.sessionbeans.pojo.ExceptionSender;
import jp.co.nec.aim.mm.sessionbeans.pojo.InquiryJobHandler;
import jp.co.nec.aim.mm.util.StopWatch;

/**
 * @author mozj
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRED)
public class SystemInitializationBean {
	private static Logger log = LoggerFactory
			.getLogger(SystemInitializationBean.class);		
	@EJB
	private PollBean pollBean;	
	@EJB
	private PartitionServiceBean partitionServiceBean;
	

	@PersistenceContext(unitName = "aim-db")
	private EntityManager entityManager;
	@Resource(mappedName = "java:jboss/MySqlDS")
	private DataSource dataSource;

	private MatchManagerDao mmDao;
	private MMEventsDao mmeDao;
	private SystemConfigDao sysConfigDao;	
	private InquiryJobDao jobDao;
	private DateDao dateDao;
	private DmServiceDao dmServiceDao;	
	private SystemInitDao systemInitDao;	
	private ExceptionSender exceptionSender;
	private InquiryJobHandler inquiryJobHandler;
	private JdbcTemplate jdbcTemplate;

	public SystemInitializationBean() {

	}

	@PostConstruct
	public void init() {
		mmDao = new MatchManagerDao(entityManager);
		mmeDao = new MMEventsDao(entityManager);
		sysConfigDao = new SystemConfigDao(entityManager);		
		jobDao = new InquiryJobDao(entityManager, dataSource);
		dateDao = new DateDao(dataSource);
		dmServiceDao = new DmServiceDao(entityManager);
		systemInitDao = new SystemInitDao(entityManager);		
		exceptionSender = new ExceptionSender();
		inquiryJobHandler = new InquiryJobHandler(entityManager, dataSource);				
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public void initializeAIM() {
		log.info("initializeAIM() called in SystemInitializationBean");
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		try {
			MatchManagerEntity mm = setStartupTime();
			writeAllMissingProperties();
			calculatingExcutingJobCount();
			clearMapReduces(mm.getMmId());
			initCompletedJobCount();			
			startScheduler();
			partitionServiceBean.getSegChangeLogRotationData();
			partitionServiceBean.startPartition();	
			getDmServiceSetting();
			getUidMMSetting();
			HazelcastService.getInstance().init();
			
		} finally {
			stopWatch.stop();
			PerformanceLogger.log(getClass().getSimpleName(), "initializeAIM",
					stopWatch.elapsedTime());
		}
	}

	private void writeAllMissingProperties() {
		sysConfigDao.writeAllMissingProperties(dataSource);
		entityManager.flush();
	}

	private void calculatingExcutingJobCount() {
		if (log.isDebugEnabled()) {
			log.debug("Calculating Excuting Job Count.");
		}
		try {
			InitExecutingJobCountProcedure initExcJobCount = new InitExecutingJobCountProcedure(
					dataSource);
			initExcJobCount.execute();
		} catch (DataAccessException ex) {
			String message = "DataAccessException when Init Executing Job Count";
			log.error(message, ex);
			throw new AimRuntimeException(message, ex);
		}
	}

	private void clearMapReduces(long mmId) {
		List<MatchManagerEntity> mmList = mmDao.listWorkingMMs();
		for (MatchManagerEntity mm : mmList) {
			if (mm.getMmId() != mmId) {
				return;
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("clearMapReduces by MM (" + mmId + ").");
		}

		List<ContainerJobEntity> conjobs = jobDao.getWorkingContainerJobs();
		for (ContainerJobEntity job : conjobs) {
			String errReason = "Dead jobs : " + job.getContainerJobId()
					+ " due to when MM (" + mmId
					+ ") restart clear Map Reduces.";
			if (log.isDebugEnabled()) {
				log.debug(errReason);
			}
			// call failContainerJob function
			try {				
				inquiryJobHandler.failInquiryJob(job.getContainerJobId(),
						AimError.INQ_JOB_RETRY_OVER.getErrorCode(), errReason,
						dateDao.getReasonTime());	
				
			} catch (AimRuntimeException  e) {
				String message = "Exception: Container job "
						+ job.getContainerJobId() + " when MM (" + mmId
						+ ") restart clear Map Reduces.";
				log.error(message, e);

				exceptionSender.sendAimException(
						AimError.INQ_INTERNAL.getErrorCode(), message,
						job.getContainerJobId(), 0, job.getMrId(), e);
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Clear Map Reduces.");
		}
	}

	/**
	 * Starts Scheduler Service
	 */
	private void startScheduler() {
		if (log.isDebugEnabled()) {
			log.debug("Start All Schedulers.");
		}

		try {
			QuartzManager qzManager = QuartzManager.getInstance(entityManager);
			qzManager.startScheduler();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw new AimRuntimeException(e);
		}
	}

	/**
	 * insert MM startUpTime into MM_EVENTS table.
	 */
	private MatchManagerEntity setStartupTime() {
		MatchManagerEntity mm = mmDao.createOrLookupVersion();
		mmeDao.setLast(mm.getMmId(), MMEventType.STARTUP);
//		AimInfo info = AimInfo.MM_START_UP;
//		String detail = String.format(info.getMessage(), mm.getUniqueId());
//		eventLog.logEvent(info.getInfoCode(), info.getEventType(),
//				mm.getMmId(), detail, EventLogLevel.INFO);
		return mm;
	}
	
	private void getUidMMSetting() {
		Integer templateSize = systemInitDao.getOneTemplateSize();	
		AimManager.saveToUidMMSettingMap(oneTemplateSize, String.valueOf(templateSize));
		String sql = "select MAX_RECORD_COUNT from CONTAINERS where CONTAINER_ID=1";
		Integer maxSegmentCount = jdbcTemplate.queryForObject(sql,Integer.class);
		AimManager.saveToUidMMSettingMap(maxSegmetRecordCount, String.valueOf(maxSegmentCount));		
		String homePath = System.getProperty("jboss.home.dir");
		 homePath =  homePath.endsWith(File.separator) ? File.separator :  homePath + File.separator;
		String hazelcastXmlPath = homePath + File.separator + "modules" + File.separator + "aim" + File.separator + "mm" + File.separator + "configuration" + File.separator + "main" + File.separator + "hazelcast.xml";
		AimManager.saveToUidMMSettingMap(hazelcastConfigPath, hazelcastXmlPath);
		System.out.println("hazelcastXmlPath=" + hazelcastXmlPath);
		
		
	}
	
	private void getDmServiceSetting() {
		List<DmServiceEntity> dmsList = dmServiceDao.findAllDmService();
		if (dmsList.size() < 1) {
			throw new UidDmClientException("DM Service CONTACT_URL is not setting in AIMDB."); 
		}
		UidDmJobRunManager.setSocketHeatbeatExector(dmsList.size());
		 dmsList.forEach(one -> {
			 String key = one.getDmId().toString();
			 String sokectPort = one.getHeatbeatPort();
			 Integer interval = one.getHeartBeatInterval();
			 String thisUrl = one.getContactUrl();			 
			 UidDmJobRunManager.addActiveDmServiceTome(thisUrl);			
			 String[] tmpArr = thisUrl.split(":");			
			 String ip = tmpArr[1].substring(2, tmpArr[1].length());			
			 String temp = tmpArr[2];
			 String port = temp.substring(0, temp.length() -3);			
			 int idx = temp.indexOf("/");
			 String dmWebContent = temp.substring(idx + 1, temp.length());	
			 String ipAndPort = ip + ":" + port;			
			 SocketHeatBeatSender sender = new SocketHeatBeatSender(dmWebContent, ipAndPort, sokectPort, interval.longValue());
			 UidDmJobRunManager.sumitSocketHeatbeatJobToMe(sender);
			 String dmSyncUrl = one.getContactUrl() + dmSync;			 
		     String dmGetTemplateByRefIdUrl = one.getContactUrl() + dmGetTemplateByRefId;
		     UidDmJobRunManager.putToDmsUrlMap(key + "_" + "dmSync" , dmSyncUrl);		    
		     UidDmJobRunManager.putToDmsUrlMap(key + "_"  + "dmGetTemplateByRefId" , dmGetTemplateByRefIdUrl);
		 });		 
	}
	
	public String getUidReqProtocol() {
		String reqFrom = systemInitDao.getRequestFromSetting();
		return reqFrom;    	
	}
	
	public void finalizeAIM() {
		MatchManagerEntity mm = mmDao.createOrLookupVersion();
		mm.setState(UnitState.EXITED);
		entityManager.merge(mm);
		entityManager.flush();

		try {
			QuartzManager qzManager = QuartzManager.getInstance(entityManager);
			qzManager.shutdownScheduler();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		try {
			final SyncThreadExecutor executor = SyncThreadExecutor.getInstance();
			executor.stopInternal();
			SocketHeatBeatSender.stop();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		HazelcastService.getInstance().shutdown();
	}

	public void initCompletedJobCount() {
		String inqurySql = "UPDATE INQUIRY_TRAFFIC set JOB_COMPLETE_COUNT = 0";
		String extractSql = "UPDATE EXTRACT_COMPLETE_COUNT set COMPLETE_COUNT =0,COMPLETE_TS = 0";
		jdbcTemplate.update(inqurySql);
		jdbcTemplate.update(extractSql);
		jdbcTemplate.execute("commit");
	}

}