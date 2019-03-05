package jp.co.nec.lsm.tmi.service.sessionbean;

import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.Date;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.nec.lsm.tm.common.constants.Constants;
import jp.co.nec.lsm.tm.common.constants.JNDIConstants;
import jp.co.nec.lsm.tm.common.log.InfoLogger;
import jp.co.nec.lsm.tm.common.transition.RoleStateTransition;
import jp.co.nec.lsm.tm.common.util.SafeCloseUtil;
import jp.co.nec.lsm.tmi.common.util.IdentifyBatchJobGetterTimer;
import jp.co.nec.lsm.tmi.common.util.IdentifyEventBus;
import jp.co.nec.lsm.tmi.db.dao.IdentifyDateDaoLocal;
import jp.co.nec.lsm.tmi.db.dao.IdentifySystemConfigDaoLocal;
import jp.co.nec.lsm.tmi.db.dao.IdentifyTransactionManagerDaoLocal;
import jp.co.nec.lsm.tmi.timer.IdentifyExpirationTimer;

/**
 * @author jimy <br>
 *         IdentifySystemInitializationBean Initialization TMI
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRED)
public class IdentifySystemInitializationBean implements
		IdentifySystemInitializationRemote {
	private static final Logger log = LoggerFactory
			.getLogger(IdentifySystemInitializationBean.class);

	@EJB
	private IdentifySystemConfigDaoLocal identifySystemConfigDao;
	@EJB
	private IdentifyTransactionManagerDaoLocal identifyManagerDao;
	@EJB
	private IdentifySegmentLoadBalanceManagerLocal loadBalance;
	@EJB
	private IdentifyRecoveryServiceBeanLocal recoveryService;
	@EJB
	private IdentifyDateDaoLocal dateDao;

	/**
	 * constructor
	 */
	public IdentifySystemInitializationBean() {
	}

	@Override
	public void initialize() {
		printLogMessage("start public function initialize()..");

		// expired date validation
		if (isSystemExpiredOrRunTimer()) {
			if (log.isWarnEnabled()) {
				log.warn("identify system is already expired..");
			}
			return;
		}

		// write TM start time into DB
		setStartupTime();
		// read properties from file write into DB
		writeAllMissingProperties();
		// remove identify_queue_event message
		removeJmsMessage(JNDIConstants.IDENTIFY_QUEUE);
		// remove identify_prepare_template_event message
		removeJmsMessage(JNDIConstants.IDENTIFY_PREPARE_TMEPLATE_QUEUE);
		// judge the start mode
		if (setAndJudgeStartMode()) {
			// if active mode
			// set slb enabled = false
			loadBalance.disableSLB();
			// recover the uncompleted jobs
			recoveryService.recovery();
		}
		// start job poll bean timer
		IdentifyEventBus.notifyStartJobPollTimerService();
		// start usc poll bean timer
		IdentifyEventBus.notifyStartUSCPollTimerService();
		// start delivery check poll bean timer
		IdentifyEventBus.notifyDeliveryCheckPollTimerService();
		// start get batch job poll bean timer
		IdentifyEventBus.notifyGetBatchJobPollTimerService();
		// start heartbeat poll bean timer
		int pollDuraton = identifySystemConfigDao.getHeartbeatPollingDuration();
		IdentifyEventBus.notifyHeartbeatPollTimerService(pollDuraton);

		// out put the TME System Initialization
		if (log.isInfoEnabled()) {
			log.info(InfoLogger.infoOutput("IdentifySystemInitializationBean",
					"initialize", "DETAIL",
					"TMI System Initialization successfully.."));
		}

		printLogMessage("end public function initialize()..");
	}

	/**
	 * startExpirationTimer
	 * 
	 * @return is expired
	 * @throws ParseException
	 */
	private boolean isSystemExpiredOrRunTimer() {
		Date dbDate = dateDao.getDatabaseDate();
		IdentifyExpirationTimer expirationTimer = new IdentifyExpirationTimer(
				dbDate);

		// already expired.. skip initialize
		if (expirationTimer.isExpired()) {
			IdentifyBatchJobGetterTimer jobGetter = IdentifyBatchJobGetterTimer
					.getInstance();
			jobGetter.setStop(true);
			jobGetter.setTransitionState(RoleStateTransition.PASSIVE);
			return true;
		}

		expirationTimer.start();
		return false;
	}

	/**
	 * setAndJudgeStartMode
	 */
	private boolean setAndJudgeStartMode() {
		IdentifyBatchJobGetterTimer jobGetter = IdentifyBatchJobGetterTimer
				.getInstance();
		String mode = System.getProperty(Constants.PROPERTY_KEY_NAME);
		if (mode != null && (Constants.PASSIVE.equalsIgnoreCase(mode))) {
			jobGetter.setTransitionState(RoleStateTransition.PASSIVE);
			return false;
		} else {
			jobGetter.setTransitionState(RoleStateTransition.ACTIVE);
			return true;
		}
	}

	/**
	 * 
	 * @param queueName
	 */
	@SuppressWarnings("unused")
    private void removeJmsMessageOld(String queueName) {
		try {
			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
			StringBuilder sb = new StringBuilder();
			sb.append("org.hornetq:module=JMS,name=\"");
			sb.append(queueName);
			sb.append("\",type=Queue");
			ObjectName target = new ObjectName(sb.toString());

			String[] sig = new String[] { "java.lang.String" };
			mBeanServer.invoke(target, "removeMessages", new String[] { "" },
					sig);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
    
    private void removeJmsMessage(String queueName) {
        InitialContext c = null;
        Session session = null;
        Connection connection = null;
        try {
            c = new InitialContext();
            ConnectionFactory jmsConnectionFactory = (ConnectionFactory) c.lookup(JNDIConstants.JmsFactory);
            connection = jmsConnectionFactory.createConnection();
            Queue queue = (Queue) c.lookup(queueName);
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer mc = session.createConsumer(queue);
            Message obj = null;
            while ((obj = mc.receive(100L)) != null) {
                obj.getJMSMessageID();
            }
        } catch (NamingException | JMSException e) {
            e.printStackTrace();
        } finally {
            SafeCloseUtil.close(session);
            SafeCloseUtil.close(connection);
            try {
                c.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
	

	private void writeAllMissingProperties() {
		identifySystemConfigDao.writeAllMissingProperties();
	}

	/**
	 * insert MM startUpTime into MATCH_MANAGER_TIMES table.
	 */
	private void setStartupTime() {
		identifyManagerDao.setStartupTime();
	}

	/**
	 * print Debug Log Message
	 * 
	 * @param logMessage
	 * @return
	 */
	private void printLogMessage(String logMessage) {
		if (log.isDebugEnabled()) {
			log.debug(logMessage);
		}
	}
}
