package jp.co.nec.aim.mm.extract.dispatch;

import java.io.IOException;

import jp.co.nec.aim.mm.logger.PerformanceLogger;
import jp.co.nec.aim.mm.util.StopWatch;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http poster to post extract request to mu
 * 
 * @author xiazp
 *
 */
public class ExtractResultsPoster {
	private static final Logger logger = LoggerFactory
			.getLogger(ExtractResultsPoster.class);
	private static final int SO_TIMEOUT = 50 * 1000;
	private static final int CON_TIMEOUT = 50 * 1000;
	private static final int REQUEST_TIMEOUT = 30 * 1000;
	private static final  String HTTP = "http://";

	// private static final int RETRY_COUNT = 3;
	public boolean postRequest(String Url, int muPostRetryCount, byte[] request) {
		logger.info("Starting to send extract request to:" + Url);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();		
		if (request == null || request.length == 0 || Url == null
				|| Url.length() <= 0 || Url.isEmpty()) {
			throw new IllegalArgumentException();
		}
		CloseableHttpClient httpclient = createCloseableHttpClient();
		HttpPost httpPost = new HttpPost(Url);
		ByteArrayEntity byteArrayEntity = new ByteArrayEntity(request);
		httpPost.setEntity(byteArrayEntity);
		boolean mustRetry = true;
		int postedTimes = 0;
		CloseableHttpResponse response = null;
		int resposeCode = -999;
		while (mustRetry && postedTimes < muPostRetryCount) {
			try {
				response = httpclient.execute(httpPost);				
			} catch (IOException e) {
				String error = "An IOException occurred while post muJob request to mu(URL:"
						+ Url + ").";
				logger.error("An IOException occurred while post muJob request to mu(URL:"
						+ Url + ").");
				logger.error(error, e);
			}
			if (response != null) {
				resposeCode = response.getStatusLine().getStatusCode();
				logger.info("responseCode = {}",resposeCode);
			}
			resposeCode = response.getStatusLine().getStatusCode();
			if (resposeCode == 200) {
				mustRetry = false;
				logger.info("Success to send extract request to:" + Url);
				stopWatch.stop();
				PerformanceLogger.log(getClass().getSimpleName(), "postFailedExtractJobResult",stopWatch.elapsedTime());				
			} else {
				postedTimes++;
			}
		}
		if (postedTimes < muPostRetryCount) {
			return true;
		} else {
			logger.info("Failed to send extract request to:" + Url);
			return false;
		}
	}

	/**
	 * PostRequest to the mu
	 * 
	 * @param Url
	 * @param message
	 * @return
	 */
	public boolean postRequest(String Url, int muPostRetryCount, String message) {
		logger.info("Starting to send extract request to:" + Url);
		if (message == null || message.length() == 0 || Url == null
				|| Url.length() <= 0 || Url.isEmpty() || !Url.startsWith(HTTP)) {
			logger.error("Request paramers are incorrect!");
			throw new IllegalArgumentException();
		}
		CloseableHttpClient httpclient = createCloseableHttpClient();
		HttpPost httpPost = new HttpPost(Url);
		StringEntity entity;
		boolean mustRetry = true;
		int postedTimes = 0;
		CloseableHttpResponse response = null;
		int resposeCode = -999;
		while (mustRetry && postedTimes < muPostRetryCount) {
			try {
				entity = new StringEntity(message);
				httpPost.setEntity(entity);
				response = httpclient.execute(httpPost);
				if (response != null) {
					resposeCode = response.getStatusLine().getStatusCode();
					logger.info("resposeCode = {}",resposeCode);
				}				
				if (resposeCode == 200) {
					mustRetry = false;
					logger.info("Success to send extract request to:" + Url);
				} else {
					postedTimes++;
				}				
			} catch (IOException e) {				
				logger.error(e.getMessage(), e);
			} finally {
				try {
					httpclient.close();
					response.close();
					 httpPost = null;
				} catch (IOException e) {					
					logger.error(e.getMessage(),e);
				}
			}
		}		
		if (postedTimes < muPostRetryCount) {
			return true;
		} else {
			logger.info("Failed to send extract request to:" + Url);
			return false;
		}
	}

	/**
	 * 
	 * @return
	 */
	private CloseableHttpClient createCloseableHttpClient() {		
		RequestConfig defaultRequestConfig = RequestConfig.custom()
				.setSocketTimeout(SO_TIMEOUT).setConnectTimeout(CON_TIMEOUT)
				.setConnectionRequestTimeout(REQUEST_TIMEOUT)
				.setStaleConnectionCheckEnabled(true).setRedirectsEnabled(true)
				.build();
		CloseableHttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(defaultRequestConfig).build(); 
		return httpclient;
	}
}
