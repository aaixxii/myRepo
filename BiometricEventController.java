package com.nec.biomatcher.web.controller.bioevent;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.nec.biomatcher.comp.bioevent.BiometricEventService;
import com.nec.biomatcher.comp.bioevent.dataAccess.BiometricEventCriteria;
import com.nec.biomatcher.comp.common.query.criteria.BetweenCriteria;
import com.nec.biomatcher.comp.common.query.criteria.CriteriaDto;
import com.nec.biomatcher.comp.common.query.criteria.EqualCriteria;
import com.nec.biomatcher.comp.common.query.criteria.ILikeCriteria;
import com.nec.biomatcher.comp.config.BioMatcherConfigService;
import com.nec.biomatcher.comp.entities.dataAccess.BiometricEventInfo;
import com.nec.biomatcher.comp.template.packing.util.MeghaTemplateConfig;
import com.nec.biomatcher.comp.template.packing.util.MeghaTemplateUtil;
import com.nec.biomatcher.comp.template.storage.TemplateDataService;
import com.nec.biomatcher.core.framework.common.DateUtil;
import com.nec.biomatcher.core.framework.common.pagination.OrderedColumn;
import com.nec.biomatcher.core.framework.common.pagination.PageRequest;
import com.nec.biomatcher.core.framework.common.pagination.PageResult;
import com.nec.biomatcher.core.framework.springSupport.SpringServiceManager;
import com.nec.biomatcher.spec.transfer.event.BiometricEventStatus;
import com.nec.biomatcher.web.controller.common.BaseController;

@Controller
@RequestMapping(value = "/secured/admin/bioevent")
public class BiometricEventController extends BaseController {
	private static final Logger logger = Logger.getLogger(BiometricEventController.class);

	private BiometricEventService biometricEventService = SpringServiceManager.getBean("biometricEventService");
	private TemplateDataService templateDataService = SpringServiceManager.getBean("templateDataService");
	private BioMatcherConfigService bioMatcherConfigService = SpringServiceManager.getBean("bioMatcherConfigService");

	private final String EMPTY_STRING = "";

	private enum BiometricEventColumnOrder {
		biometricId, externalId, eventId, binId, assignedSegmentId, status, phase, dataVersion, updateDateTime;
	}

	@RequestMapping(value = "/getBiometricEventsIndex", method = RequestMethod.GET)
	public ModelAndView getBiometricEventsIndex(HttpServletRequest request) {
		logger.debug("In BiometricEventController.getBiometricEventsIndex");
		return new ModelAndView("biometricevent.list");
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/getBiometricEvents", method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<String> getBiometricEvents(HttpServletRequest request) {
		logger.debug("In BiometricEventController.getBiometricEvents");
		PageRequest pageRequest = constructPageRequest(request);
		// String filterExtJobId =
		// request.getParameter("columns[8][search][value]");
		JSONArray jsonArray = new JSONArray();
		JSONObject jsonResponse = new JSONObject();
		try {
			BiometricEventCriteria biometricEventCriteria = new BiometricEventCriteria();
			addRestrictionsBySearchOptions(request, BiometricEventInfo.class, BiometricEventCriteria.class,
					biometricEventCriteria, BiometricEventColumnOrder.values().length - 1, true);

			PageResult<BiometricEventInfo> pageResult = biometricEventService
					.getBiometricEventInfoList(biometricEventCriteria, pageRequest);

			List<BiometricEventInfo> biometricEventInfoList = pageResult.getResultList();
			if (CollectionUtils.isNotEmpty(biometricEventInfoList)) {
				JSONObject jsonObject = null;
				for (BiometricEventInfo biometricEventInfo : biometricEventInfoList) {
					jsonObject = new JSONObject();

					// biometricId, externalId, eventId, binId,
					// assignedSegmentId, status, phase, dataVersion,
					// updateDateTime;
					jsonObject.put(BiometricEventColumnOrder.biometricId.name(), biometricEventInfo.getBiometricId());
					jsonObject.put(BiometricEventColumnOrder.externalId.name(), biometricEventInfo.getExternalId());
					jsonObject.put(BiometricEventColumnOrder.eventId.name(), biometricEventInfo.getEventId());
					jsonObject.put(BiometricEventColumnOrder.binId.name(), biometricEventInfo.getBinId());
					jsonObject.put(BiometricEventColumnOrder.assignedSegmentId.name(),
							biometricEventInfo.getAssignedSegmentId());
					jsonObject.put(BiometricEventColumnOrder.status.name(), biometricEventInfo.getStatus() == null
							? EMPTY_STRING : biometricEventInfo.getStatus().getValue());
					jsonObject.put(BiometricEventColumnOrder.phase.name(), biometricEventInfo.getPhase() == null
							? EMPTY_STRING : biometricEventInfo.getPhase().getValue());
					jsonObject.put(BiometricEventColumnOrder.dataVersion.name(), biometricEventInfo.getDataVersion());
					jsonObject.put(BiometricEventColumnOrder.updateDateTime.name(), DateUtil.parseDate(
							biometricEventInfo.getUpdateDateTime(), DateUtil.FORMAT_MM_DD_YYYY_HH24_MM_SS_SSS));
					jsonArray.add(jsonObject);
				}

			}
			jsonResponse.put("data", jsonArray);
			addPaginationFieldsToResponse(request, jsonResponse, pageResult);
			return new ResponseEntity<String>(jsonResponse.toString(), HttpStatus.OK);
		} catch (Exception ex) {
			logger.error("Error in getBiometricEvents: " + ex.getMessage(), ex);
			return new ResponseEntity<String>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/getTemplateDetails", method = RequestMethod.GET)
	@ResponseBody
	public String getTemplateDetails(HttpServletRequest request) {
		String biometricId = request.getParameter("biometricId");
		logger.debug("In BiometricEventController.getTemplateDetails: biometricId: " + biometricId);
		if (StringUtils.isNotBlank(biometricId) && StringUtils.isNumeric(biometricId)) {
			try {
				BiometricEventInfo biometricEventInfo = biometricEventService
						.getBiometricEventInfo(Long.parseLong(biometricId));
				if (biometricEventInfo != null && BiometricEventStatus.ACTIVE.equals(biometricEventInfo.getStatus())) {
					byte[] templateData = templateDataService.getTemplateData(biometricEventInfo.getTemplateDataKey());

					MeghaTemplateConfig meghaTemplateConfig = bioMatcherConfigService.getMeghaTemplateConfig();

					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					try (PrintStream printStream = new PrintStream(baos)) {
						MeghaTemplateUtil.printTemplateDetails(templateData, meghaTemplateConfig, printStream);
					}

					JSONObject jsonResponse = new JSONObject();
					jsonResponse.put("data", baos.toString());
					return jsonResponse.toString();
				}
			} catch (Exception e) {
				logger.error("Error in getTemplateDetails for biometricId: " + biometricId + " : " + e.getMessage(), e);

				StringWriter stackTraceWriter = new StringWriter();
				try (PrintWriter pw = new PrintWriter(stackTraceWriter)) {
					e.printStackTrace(pw);
				}

				JSONObject jsonResponse = new JSONObject();
				jsonResponse.put("data", "Error in getTemplateDetails for biometricId: " + biometricId + " : "
						+ e.getMessage() + "\r\n" + stackTraceWriter);
				return jsonResponse.toString();
			}
		}
		JSONObject jsonResponse = new JSONObject();
		jsonResponse.put("data", "");
		return jsonResponse.toString();
	}

	public PageRequest constructPageRequest(HttpServletRequest request) {
		int currentPage = Integer.parseInt(request.getParameter("draw"));
		int maxRecordsPerPage = Integer.parseInt(request.getParameter("length"));
		int startIndex = Integer.parseInt(request.getParameter("start"));

		PageRequest pageRequest = new PageRequest();
		pageRequest.setFirstRowIndex(startIndex);
		pageRequest.setMaxRecords(maxRecordsPerPage);
		pageRequest.setCalculateRecordCount(true);

		OrderedColumn[] orderedColumns = getSortOptions(request);

		pageRequest.setOrderedColumns(orderedColumns);

		return pageRequest;
	}

	private OrderedColumn[] getSortOptions(HttpServletRequest request) {
		String orderedColumnIndex = request.getParameter("order[0][column]");
		String sortingOrder = request.getParameter("order[0][dir]");
		String sortingColName = request.getParameter("columns[" + orderedColumnIndex + "][data]");

		OrderedColumn[] orderedColumns = new OrderedColumn[1];
		int sortOrder = StringUtils.isBlank(sortingOrder) ? OrderedColumn.ASCENDING
				: (sortingOrder.equalsIgnoreCase("asc") ? OrderedColumn.ASCENDING : OrderedColumn.DESCNDING);
		OrderedColumn orderedColumn = new OrderedColumn(sortingColName, sortOrder);
		orderedColumns[0] = orderedColumn;

		return orderedColumns;

	}

	public void addPaginationFieldsToResponse(HttpServletRequest request, JSONObject jsonObject,
			PageResult<?> pageResult) {
		int currentPage = Integer.parseInt(request.getParameter("draw"));

		jsonObject.put("draw", currentPage);
		jsonObject.put("recordsTotal", pageResult.getTotalRecords());
		jsonObject.put("recordsFiltered", pageResult.getTotalRecords());

	}

	/**
	 * Description: This method parsed java.util.Date type to java.lang.String
	 * by specified string format
	 *
	 * @param date
	 *            the date
	 * @return the string
	 */
	protected static String parseDateTime(java.util.Date date, Locale locale) {
		String returnDateStr = "";
		try {
			if (date != null) {
				returnDateStr = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale)
						.format(date);
			}
		} catch (Exception e) {
			returnDateStr = "";
		}
		return returnDateStr;
	}

	protected <T> void addRestrictionsBySearchOptions(HttpServletRequest request, Class entityClass,
			Class<T> criteriaClass, T criteriaObj, int noOfSearchColumns, boolean isExactSearch)
			throws IllegalArgumentException, IllegalAccessException, ParseException {

		@SuppressWarnings("unchecked")
		Map<Field, String> searchColMap = getSearchFieldValues(request, entityClass, noOfSearchColumns);

		if (MapUtils.isNotEmpty(searchColMap)) {
			for (Map.Entry<Field, String> fieldEntry : searchColMap.entrySet()) {
				Field searchField = fieldEntry.getKey();
				String fieldName = searchField.getName();
				String searchVal = fieldEntry.getValue().toUpperCase();

				boolean added = addRestrictions(criteriaClass, criteriaObj, searchField, fieldName, searchVal,
						isExactSearch);
				if (!added) {
					addCustomCriteria(criteriaObj, searchField, fieldName, searchVal);
				}
			}
		}

	}

	protected <T> Map<Field, String> getSearchFieldValues(HttpServletRequest request, Class<T> entityClass,
			int noOfSearchColumns) {
		Map<Field, String> searchColMap = new HashMap<Field, String>();

		for (int i = 0; i < noOfSearchColumns; i++) {
			String columnName = request.getParameter("columns[" + i + "][data]");
			String searchValue = request.getParameter("columns[" + i + "][search][value]");
			if (StringUtils.isNotBlank(columnName) && StringUtils.isNotBlank(searchValue)) {
				Field field = ReflectionUtils.findField(entityClass, columnName);
				field.setAccessible(Boolean.TRUE);
				searchColMap.put(field, searchValue);
			}
		}

		return searchColMap;

	}

	private <T> boolean addRestrictions(Class<T> criteriaClass, T criteriaObj, Field searchField, String fieldName,
			String value, boolean isExactSearch)
			throws IllegalArgumentException, IllegalAccessException, ParseException {
		boolean added = false;
		if (searchField != null) {
			Class<?> columnType = searchField.getType();
			if (columnType.getName().equals("com.nec.biomatcher.spec.transfer.event.BiometricEventStatus")) {
				added = true;				
				EqualCriteria equalCriteria = new EqualCriteria(BiometricEventStatus.enumOf(value));
				updatePropertyValue(criteriaClass, criteriaObj, fieldName, equalCriteria);				
			} else 	if (columnType.equals(String.class)) {
				added = true;
				if (isExactSearch && value.indexOf("-") < 0) {
					EqualCriteria equalCriteria = new EqualCriteria(value);
					updatePropertyValue(criteriaClass, criteriaObj, fieldName, equalCriteria);
				} else {
					ILikeCriteria iLikeCriteria = new ILikeCriteria('%' + value + '%');
					updatePropertyValue(criteriaClass, criteriaObj, fieldName, iLikeCriteria);
				}

			} else if (columnType.equals(Long.class)) {
				added = true;
				value = StringUtils.isNumeric(value) ? value : "-1";
				EqualCriteria equalCriteria = new EqualCriteria(Long.valueOf(value));
				updatePropertyValue(criteriaClass, criteriaObj, fieldName, equalCriteria);

			} else if (columnType.equals(Integer.class)) {
				added = true;
				value = StringUtils.isNumeric(value) ? value : "-1";
				EqualCriteria equalCriteria = new EqualCriteria(Integer.valueOf(value));
				updatePropertyValue(criteriaClass, criteriaObj, fieldName, equalCriteria);

			} else if (columnType.equals(Date.class) && value != null && value.contains("-")) {
				added = true;
				String[] dateRange = value.split("-");
				Date startDate = DateUtil.strToDate(dateRange[0].trim(), DateUtil.FORMAT_MM_DD_YYYY_HH_MM_SS);
				Date endDate = DateUtil.strToDate(dateRange[1].trim(), DateUtil.FORMAT_MM_DD_YYYY_HH_MM_SS);
				BetweenCriteria betweenCriteria = new BetweenCriteria();
				betweenCriteria.setFromValue(startDate);
				betweenCriteria.setToValue(endDate);

				updatePropertyValue(criteriaClass, criteriaObj, fieldName, betweenCriteria);
			}
		}
		return added;
	}

	public <T> void addCustomCriteria(T criteriaObj, Field searchField, String fieldName, String value) {

	}

	protected <T> void updatePropertyValue(Class<T> criteriaClass, T criteriaObj, String fieldName,
			CriteriaDto criteriaDto) throws IllegalArgumentException, IllegalAccessException {

		Field field = ReflectionUtils.findField(criteriaClass, fieldName);
		field.setAccessible(Boolean.TRUE);
		field.set(criteriaObj, criteriaDto);

	}
}
