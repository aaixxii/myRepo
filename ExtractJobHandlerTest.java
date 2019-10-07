package jp.co.nec.aim.mm.sessionbeans.pojo;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.sql.DataSource;
import javax.transaction.Transactional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.object.StoredProcedure;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import jp.co.nec.aim.message.proto.AIMMessages.PBMuExtractJobResultItem;
import jp.co.nec.aim.message.proto.BusinessMessage.PBBusinessMessage;
import jp.co.nec.aim.message.proto.CommonEnumTypes.TemplateFormatType;
import jp.co.nec.aim.message.proto.ExtractPayloads.PBExtractIrisOutput;
import jp.co.nec.aim.message.proto.ExtractPayloads.PBExtractOutputPayload;
import jp.co.nec.aim.message.proto.ExtractPayloads.PBExtractTenprintOutput;
import jp.co.nec.aim.mm.dao.DateDao;
import jp.co.nec.aim.mm.dao.SystemConfigDao;
import jp.co.nec.aim.mm.entities.FeJobQueueEntity;
import jp.co.nec.aim.mm.entities.SystemConfigEntity;
import jp.co.nec.aim.mm.exception.AimRuntimeException;
import jp.co.nec.aim.mm.exception.HttpPostException;
import jp.co.nec.aim.mm.logger.FeJobDoneLogger;
import jp.co.nec.aim.mm.util.Deflater;
import jp.co.nec.aim.mm.util.HttpPoster;
import jp.co.nec.aim.mm.util.HttpResponseInfo;
import jp.co.nec.aim.mm.util.ProtobufCreater;
import mockit.Mock;
import mockit.MockUp;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration()
@Transactional
public class ExtractJobHandlerTest {
	@Resource
	private DataSource dataSource;
	@PersistenceContext(unitName = "aim-db")
	private EntityManager entityManager;
	@Resource
	private JdbcTemplate jdbcTemplate;
	private ProtobufCreater  protobufCreater;

	private ExtractJobHandler extractJobHandler;

	private DateDao dateDao;
	private SystemConfigDao systemconfigdao;
	private long curTime;
	private static String postUrlType;
	private static long logJobId;
	private static String postUrl;
	private int port = 6543;

	@Before
	public void setUp() throws Exception {
		clearDB();
		dateDao = new DateDao(dataSource);
		systemconfigdao = new SystemConfigDao(entityManager);
		systemconfigdao.writeAllMissingProperties(dataSource);
		protobufCreater = new ProtobufCreater ();
		curTime = dateDao.getCurrentTimeMS();

		intsertUnit();
		insertFeJob();
		jdbcTemplate.execute("commit");

		Query q = entityManager.createNamedQuery("NQ::getConfigEntity");
		q.setParameter("name", "BEHAVIOR.MAX_EXTRACT_JOB_FAILURES");
		SystemConfigEntity e = (SystemConfigEntity) q.getResultList().get(0);
		e.setValue("2");
		entityManager.merge(e);

		logJobId = 0;
		postUrl = "";
		postUrlType = "";

		new MockUp<FeJobDoneLogger>() {
			@Mock
			public void info(long jobId) {
				logJobId = jobId;
				return;
			}
		};

		new MockUp<HttpPoster>() {
			@Mock
			public final HttpResponseInfo post(final String url, byte[] bytes,
					Integer retryCount) throws HttpPostException {
				postUrl = url;
				postUrlType = "byte[]";
				return null;
			}

			@Mock
			public final HttpResponseInfo post(final String url, String body,
					Integer retryCount) throws HttpPostException {
				postUrl = url;
				postUrlType = "String";
				return null;
			}
		};

		extractJobHandler = new ExtractJobHandler(entityManager, dataSource);
				
	}

	@After
	public void tearDown() throws Exception {
		clearDB();		
	}

	private void clearDB() {
		jdbcTemplate.execute("delete from MU_EXTRACT_LOAD");
		jdbcTemplate.execute("delete from FE_LOT_JOBS");
		jdbcTemplate.execute("delete from FE_JOB_QUEUE");
		jdbcTemplate.execute("delete from MATCH_UNITS");
		jdbcTemplate.execute("delete from SEGMENT_DEFRAGMENTATION");
		jdbcTemplate.execute("delete from SYSTEM_CONFIG");
		jdbcTemplate.execute("commit");
	}

	private void intsertUnit() {
		int id = 1;
		jdbcTemplate.execute("insert into MATCH_UNITS(MU_ID, UNIQUE_ID,"
				+ " STATE) values(" + id + ", '" + id + "', 'WORKING')");
		jdbcTemplate.execute("insert into MU_CONTACTS(MU_ID, CONTACT_TS)"
				+ " values(" + id + ", " + curTime + ")");
		jdbcTemplate.execute("insert into MU_EXTRACT_LOAD(MU_ID, PRESSURE,"
				+ " UPDATE_TS) values(" + id + ", 1, " + curTime + ")");

	}

	private void insertFeJob() {
		String fljSQL = "insert into FE_LOT_JOBS(LOT_JOB_ID, MU_ID,"
				+ " ASSIGNED_TS, TIMEOUTS) values(?, ?, ?, ?)";

		String fejSQL = "insert into FE_JOB_QUEUE(JOB_ID, LOT_JOB_ID, PRIORITY,"
				+ " FUNCTION_ID, JOB_STATE, SUBMISSION_TS, ASSIGNED_TS, MU_ID,"
				+ " CALLBACK_STYLE, CALLBACK_URL, FAILURE_COUNT) values(?, ?,"
				+ "  5,17, ?, 1421206612748, ?, ?, 1, 'http://127.0.0.1:"
				+ port + "', ?)";

		int fljId = 1;
		jdbcTemplate.update(fljSQL, new Object[] { fljId, fljId, curTime,
				30 * 1000 });
		int fejId = 1;
		jdbcTemplate.update(fejSQL, new Object[] { (fljId - 1) * 3 + fejId,
				fljId, 1, curTime, fljId, 0 });

	}

	

	@Test
	public void test_Complete_SUCCESS() throws SQLException, IOException {		
		PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		int count = extractJobHandler
				.completeExtractJob(1, 1, jobResult, false);
		assertEquals(1, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertNull(listFeJ.get(0).get("LOT_JOB_ID"));
		assertEquals("2", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertTrue(curTime < ((BigDecimal) listFeJ.get(0).get("RESULT_TS"))
				.longValue());
		assertNotNull(listFeJ.get(0).get("RESULT"));
		assertEquals("1", listFeJ.get(0).get("MU_ID").toString());
		assertEquals("0", listFeJ.get(0).get("FAILED_FLAG").toString());
		assertEquals("0", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(0, listReason.size());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(2, listResult.size());
		String[] key = new String[] { TemplateFormatType.TEMPLATE_RDBTM.name(),
				TemplateFormatType.TEMPLATE_RDBT.name() };
		byte[][] data = new byte[][] { { 1, 2, 3, 4 }, { 5, 6, 7, 8 } };
		for (int i = 0; i < listResult.size(); i++) {
			assertEquals("1", listResult.get(i).get("JOB_ID").toString());
			assertArrayEquals(data[i],
					(byte[]) listResult.get(i).get("RESULT_DATA"));
			assertEquals(key[i], listResult.get(i).get("TEMPLATE_KEY")
					.toString());
			assertEquals("" + (i + 1), listResult.get(i).get("TEMPLATE_INDEX")
					.toString());
		}

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}

	@Test
	public void test_Complete_ERROR() throws SQLException, IOException {
	 PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		int count = extractJobHandler.completeExtractJob(1, 1, jobResult, true);
		assertEquals(1, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertNull(listFeJ.get(0).get("LOT_JOB_ID"));
		assertEquals("2", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertTrue(curTime < ((BigDecimal) listFeJ.get(0).get("RESULT_TS"))
				.longValue());
		assertNotNull(listFeJ.get(0).get("RESULT"));
		assertEquals("1", listFeJ.get(0).get("MU_ID").toString());
		assertEquals("1", listFeJ.get(0).get("FAILED_FLAG").toString());
		assertEquals("1", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(1, listReason.size());
		assertEquals("1", listReason.get(0).get("JOB_ID").toString());
		assertEquals("1", listReason.get(0).get("MU_ID").toString());
		assertEquals("TestCode", listReason.get(0).get("CODE").toString());
		assertEquals("TestMessage", listReason.get(0).get("REASON").toString());
		assertEquals("TestTime", listReason.get(0).get("FAILURE_TIME")
				.toString());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}

	@Test
	public void test_Fail_Retry() {
		AimServiceState  aimServiceState  = new AimServiceState ();
		aimServiceState.setErrMsg("TestCode");
		aimServiceState.setErrorcode("109:faild");
		aimServiceState.setFailureTime("TestTime");
		FeJobQueueEntity eje = entityManager.find(FeJobQueueEntity.class,
				new Long(1));

		int count = extractJobHandler.failExtractJob(eje, 1, aimServiceState, false, 2);
		assertEquals(1, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertNull(listFeJ.get(0).get("LOT_JOB_ID"));
		assertEquals("0", listFeJ.get(0).get("JOB_STATE").toString());
		assertNull(listFeJ.get(0).get("ASSIGNED_TS"));
		assertNull(listFeJ.get(0).get("RESULT_TS"));
		assertNull(listFeJ.get(0).get("RESULT"));
		assertNull(listFeJ.get(0).get("MU_ID"));
		assertNull(listFeJ.get(0).get("FAILED_FLAG"));
		assertEquals("1", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(1, listReason.size());
		assertEquals("1", listReason.get(0).get("JOB_ID").toString());
		assertEquals("1", listReason.get(0).get("MU_ID").toString());
		assertEquals("TestCode", listReason.get(0).get("CODE").toString());
		assertEquals("TestMessage", listReason.get(0).get("REASON").toString());
		assertEquals("TestTime", listReason.get(0).get("FAILURE_TIME")
				.toString());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}

	@Test
	public void test_Fail_Done() {
		jdbcTemplate.execute("update FE_JOB_QUEUE set FAILURE_COUNT = 1");
		jdbcTemplate.execute("commit");

		AimServiceState  aimServiceState  = new AimServiceState ();
		aimServiceState.setErrMsg("TestCode");
		aimServiceState.setErrorcode("109:faild");
		aimServiceState.setFailureTime("TestTime");

		FeJobQueueEntity eje = entityManager.find(FeJobQueueEntity.class,
				new Long(1));

		int count = extractJobHandler.failExtractJob(eje, 1, aimServiceState, false, 2);
		assertEquals(1, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertNull(listFeJ.get(0).get("LOT_JOB_ID"));
		assertEquals("2", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertTrue(curTime < ((BigDecimal) listFeJ.get(0).get("RESULT_TS"))
				.longValue());
		assertNotNull(listFeJ.get(0).get("RESULT"));
		assertEquals("1", listFeJ.get(0).get("MU_ID").toString());
		assertEquals("1", listFeJ.get(0).get("FAILED_FLAG").toString());
		assertEquals("2", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(1, listReason.size());
		assertEquals("1", listReason.get(0).get("JOB_ID").toString());
		assertEquals("1", listReason.get(0).get("MU_ID").toString());
		assertEquals("TestCode", listReason.get(0).get("CODE").toString());
		assertEquals("TestMessage", listReason.get(0).get("REASON").toString());
		assertEquals("TestTime", listReason.get(0).get("FAILURE_TIME")
				.toString());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(1, logJobId);
		assertEquals("http://127.0.0.1:6543/1", postUrl);
	}

	@Test
	public void test_Fail_Undo() {
		jdbcTemplate.execute("update FE_JOB_QUEUE set MU_ID = null");
		jdbcTemplate.execute("commit");

		AimServiceState  aimServiceState  = new AimServiceState ();
		aimServiceState.setErrMsg("TestCode");
		aimServiceState.setErrorcode("109:faild");
		aimServiceState.setFailureTime("TestTime");

		FeJobQueueEntity eje = entityManager.find(FeJobQueueEntity.class,
				new Long(1));

		int count = extractJobHandler.failExtractJob(eje, 1, aimServiceState, false, 2);
		assertEquals(0, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertEquals("1", listFeJ.get(0).get("LOT_JOB_ID").toString());
		assertEquals("1", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertNull(listFeJ.get(0).get("RESULT_TS"));
		assertNull(listFeJ.get(0).get("RESULT"));
		assertNull(listFeJ.get(0).get("MU_ID"));
		assertNull(listFeJ.get(0).get("FAILED_FLAG"));
		assertEquals("0", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(0, listReason.size());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}

	@Test
	public void test_Complete_ERROR_Undo() throws SQLException, IOException {
		jdbcTemplate.execute("update FE_JOB_QUEUE set JOB_STATE = 0");
		jdbcTemplate.execute("commit");

		 PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		int count = extractJobHandler.completeExtractJob(1, 1, jobResult, true);
		assertEquals(0, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertEquals("1", listFeJ.get(0).get("LOT_JOB_ID").toString());
		assertEquals("0", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertNull(listFeJ.get(0).get("RESULT_TS"));
		assertNull(listFeJ.get(0).get("RESULT"));
		assertEquals("1", listFeJ.get(0).get("MU_ID").toString());
		assertNull(listFeJ.get(0).get("FAILED_FLAG"));
		assertEquals("0", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(0, listReason.size());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}

	@Test
	public void test_Complete_SUCCESS_Undo() throws SQLException, IOException {
		jdbcTemplate.execute("update FE_JOB_QUEUE set MU_ID = null");
		jdbcTemplate.execute("commit");

		 PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		int count = extractJobHandler
				.completeExtractJob(1, 1, jobResult, false);
		assertEquals(0, count);

		List<Map<String, Object>> listFeJ = jdbcTemplate
				.queryForList("select JOB_ID, LOT_JOB_ID, JOB_STATE, ASSIGNED_TS,"
						+ " RESULT_TS, RESULT, MU_ID, FAILED_FLAG, FAILURE_COUNT"
						+ " from FE_JOB_QUEUE order by JOB_ID");
		assertEquals(1, listFeJ.size());
		assertEquals("1", listFeJ.get(0).get("JOB_ID").toString());
		assertEquals("1", listFeJ.get(0).get("LOT_JOB_ID").toString());
		assertEquals("1", listFeJ.get(0).get("JOB_STATE").toString());
		assertEquals("" + curTime, listFeJ.get(0).get("ASSIGNED_TS").toString());
		assertNull(listFeJ.get(0).get("RESULT_TS"));
		assertNull(listFeJ.get(0).get("RESULT"));
		assertNull(listFeJ.get(0).get("MU_ID"));
		assertNull(listFeJ.get(0).get("FAILED_FLAG"));
		assertEquals("0", listFeJ.get(0).get("FAILURE_COUNT").toString());

		List<Map<String, Object>> listReason = jdbcTemplate
				.queryForList("select JOB_ID, MU_ID, CODE, REASON, FAILURE_TIME"
						+ " from FE_JOB_FAILURE_REASONS order by FAILURE_ID");
		assertEquals(0, listReason.size());

		List<Map<String, Object>> listResult = jdbcTemplate
				.queryForList("select JOB_ID, RESULT_DATA, TEMPLATE_KEY,"
						+ " TEMPLATE_INDEX from FE_RESULTS order by TEMPLATE_INDEX");
		assertEquals(0, listResult.size());

		assertEquals(0, logJobId);
		assertEquals("", postUrl);
	}
	

	@Test
	public void test_Fail_Retry_Exception() {
		new MockUp<StoredProcedure>() {
			@Mock
			public Map<String, Object> execute(Map<String, ?> inParams)
					throws DataAccessException {
				throw new DataRetrievalFailureException(
						"DataRetrievalFailureException for Test");
			}
		};

		AimServiceState  aimServiceState  = new AimServiceState ();
		aimServiceState.setErrMsg("TestCode");
		aimServiceState.setErrorcode("109:faild");
		aimServiceState.setFailureTime("TestTime");

		FeJobQueueEntity eje = entityManager.find(FeJobQueueEntity.class,
				new Long(1));
		try {
			extractJobHandler.failExtractJob(eje, 1, aimServiceState, false, 2);
		} catch (AimRuntimeException e) {
			assertEquals("DataAccessException when Retry Extract Job",
					e.getMessage());
			return;
		}
		fail();
	}

	@Test
	public void test_Fail_Done_Exception() {
		new MockUp<StoredProcedure>() {
			@Mock
			public Map<String, Object> execute(Map<String, ?> inParams)
					throws DataAccessException {
				throw new DataRetrievalFailureException(
						"DataRetrievalFailureException for Test");
			}
		};

		jdbcTemplate.execute("update FE_JOB_QUEUE set FAILURE_COUNT = 1");
		jdbcTemplate.execute("commit");
		
		AimServiceState  aimServiceState  = new AimServiceState ();
		aimServiceState.setErrMsg("TestCode");
		aimServiceState.setErrorcode("109:faild");
		aimServiceState.setFailureTime("TestTime");

		FeJobQueueEntity eje = entityManager.find(FeJobQueueEntity.class,
				new Long(1));
		try {
			extractJobHandler.failExtractJob(eje, 1, aimServiceState, true, 2);
		} catch (AimRuntimeException e) {
			assertEquals("DataAccessException when Failing Extract Job",
					e.getMessage());
			return;
		}
		fail();
	}

	@Test
	public void test_Complete_SUCCESS_Exception() throws SQLException,
			IOException {
		new MockUp<StoredProcedure>() {
			@Mock
			public Map<String, Object> execute(Map<String, ?> inParams)
					throws DataAccessException {
				throw new DataRetrievalFailureException(
						"DataRetrievalFailureException for Test");
			}
		};

		PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		try {
			extractJobHandler.completeExtractJob(1, 1, jobResult, false);
		} catch (AimRuntimeException e) {
			assertEquals("DataAccessException when Finish Extract Job",
					e.getMessage());
			return;
		}
		fail();
	}

	@Test
	public void test_Complete_ERROR_Exception() throws SQLException,
			IOException {
		new MockUp<StoredProcedure>() {
			@Mock
			public Map<String, Object> execute(Map<String, ?> inParams)
					throws DataAccessException {
				throw new DataRetrievalFailureException(
						"DataRetrievalFailureException for Test");
			}
		};

		PBMuExtractJobResultItem jobResult = createPBMuExtractJobResultItem();

		try {
			extractJobHandler.completeExtractJob(1, 1, jobResult, true);
		} catch (AimRuntimeException e) {
			assertEquals("DataAccessException when Failing Extract Job",
					e.getMessage());
			return;
		}
		fail();
	}





	public byte[] createCompuressedExtractPayload() throws IOException {
		PBExtractOutputPayload.Builder payload = PBExtractOutputPayload
				.newBuilder();
		payload.setTenprintOutput(PBExtractTenprintOutput.newBuilder())
				.setIrisOutput(PBExtractIrisOutput.newBuilder());
		byte[] bytePayload = payload.build().toByteArray();
		byte[] compressBytes = Deflater.compress(bytePayload);
		return compressBytes;
	}
	
	private PBMuExtractJobResultItem createPBMuExtractJobResultItem() {
	PBBusinessMessage pbMes = protobufCreater.createPBBusinessMessage();
	PBMuExtractJobResultItem.Builder item = PBMuExtractJobResultItem.newBuilder();
	item.setJobId(100);		
	item.setResult(pbMes.toByteString());
	return item.build();
}

}
