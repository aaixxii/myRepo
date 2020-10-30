package jp.co.nec.aim.mm.dm.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squareup.okhttp.Call;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import jp.co.nec.aim.dm.message.proto.AimDmMessages.PBDmSyncRequest;
import jp.co.nec.aim.dm.message.proto.AimDmMessages.PBDmSyncResponce;
import jp.co.nec.aim.mm.dm.client.mgmt.UidDmJobRunManager;
import jp.co.nec.aim.mm.util.StopWatch;

public class DmJobPoster {
    private static Logger logger = LoggerFactory.getLogger(DmJobPoster.class);
    
    private static final MediaType MEDIA_TYPE_PLAINTEXT = MediaType.parse("text/plain; charset=utf-8");
    
    public static Boolean post(String url, PBDmSyncRequest dmSegReq) {
        Callable<Boolean> newPostTask = () -> {
            String cmmd = dmSegReq.getCmd().name().toLowerCase();
            OkHttpClient client = new OkHttpClient();
            client.setConnectTimeout(10, TimeUnit.SECONDS);
            client.setReadTimeout(10, TimeUnit.SECONDS);
            client.setWriteTimeout(30, TimeUnit.SECONDS);
            final StopWatch t = new StopWatch();
            t.start();
            Request request = new Request.Builder().url(url)
                .post(RequestBody.create(MEDIA_TYPE_PLAINTEXT, dmSegReq.toByteArray())).build();
            try {
                Response response = client.newCall(request).execute();
                t.stop();
                logger.info("Post PBDmSyncRequest(segmentId={}) to {} used time={} and status={}", dmSegReq.getTargetSegment().getId(),
                    url, t.elapsedTime(), response.code());
                PBDmSyncResponce dmRes = PBDmSyncResponce.parseFrom(response.body().bytes());
                Boolean jobResult = Boolean.valueOf(dmRes.getSuccess());
                logger.info("Post PBDmSyncRequest(cmd= {} segmentId={}) to {} used time={}, and status={} job success is {}",
                    cmmd, dmSegReq.getTargetSegment().getId(), url, t.elapsedTime(), response.code(), jobResult);
                return jobResult;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return Boolean.valueOf(false);
            }
        };
        try {
            return UidDmJobRunManager.submit(newPostTask);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            return Boolean.valueOf(false);
        }
    }
    
    
    
    public static Integer doOnePost(String url, PBDmSyncRequest dmSegReq) {
        // PostSuccess return 0
        // PostSuccess return 1
        Callable<Integer> newPostTask = () -> {
            String cmmd = dmSegReq.getCmd().name().toLowerCase();
            OkHttpClient client = new OkHttpClient();
            client.setConnectTimeout(10, TimeUnit.SECONDS);
            client.setReadTimeout(10, TimeUnit.SECONDS);
            client.setWriteTimeout(30, TimeUnit.SECONDS);
            final StopWatch t = new StopWatch();
            t.start();
            Request request = new Request.Builder().url(url)
                .post(RequestBody.create(MEDIA_TYPE_PLAINTEXT, dmSegReq.toByteArray())).build();
            try {
                Response response = client.newCall(request).execute();
                t.stop();
                logger.info("Post PBDmSyncRequest(segmentId={}) to {} used time={} and status={}", dmSegReq.getTargetSegment().getId(),
                    url, t.elapsedTime(), response.code());
                PBDmSyncResponce dmRes = PBDmSyncResponce.parseFrom(response.body().bytes());
                Boolean jobResult = Boolean.valueOf(dmRes.getSuccess());
                logger.info("Post PBDmSyncRequest(cmd= {} segmentId={}) to {} used time={}, and status={} job success is {}",
                    cmmd, dmSegReq.getTargetSegment().getId(), url, t.elapsedTime(), response.code(), jobResult);
                return Integer.valueOf(0);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return Integer.valueOf(1);
            }
        };
        try {
            return UidDmJobRunManager.submitPost(newPostTask);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            return Integer.valueOf(1);
        }
    }
    
    public static byte[] getTemplate(String url, String refId) throws InterruptedException, ExecutionException {
        
        String getUrl = url + "/?refId=" + refId;
        Callable<byte[]> newGetTask = () -> {
            final StopWatch t = new StopWatch();
            t.start();
            OkHttpClient client = new OkHttpClient();
            client.setConnectTimeout(10, TimeUnit.SECONDS);
            client.setReadTimeout(10, TimeUnit.SECONDS);
            client.setWriteTimeout(30, TimeUnit.SECONDS);
            Request request = new Request.Builder().get().url(getUrl).build();
            Call call = client.newCall(request);
            Response response = call.execute();
            t.stop();
            if (response.code() == 200) {
                logger.info("Send getTemplate request(refId={}) to {} used time={} and status={}", refId,
                    getUrl, t.elapsedTime(), response.code());
                return response.body().bytes();
            } else {
                logger.warn("Faild to get template data from dm cluster, sendUrl={} refId={} and status={}", getUrl, refId, response.code());
                return null;
            }
        };
        return UidDmJobRunManager.submitGetRequest(newGetTask);
    }
    
    public static byte[] getTemplate(String url, Long segId, Long bioId) throws InterruptedException, ExecutionException {
        String getUrl = url + "/?segId=" + segId + "&bioId=" + bioId;
        Callable<byte[]> newGetTask = () -> {
            final StopWatch t = new StopWatch();
            t.start();
            OkHttpClient client = new OkHttpClient();
            client.setConnectTimeout(10, TimeUnit.SECONDS);
            client.setReadTimeout(10, TimeUnit.SECONDS);
            client.setWriteTimeout(30, TimeUnit.SECONDS);
            Request request = new Request.Builder().get().url(getUrl).build();
            Call call = client.newCall(request);
            Response response = call.execute();
            t.stop();
            if (response.code() == 200) {
                logger.info("Send getTemplate request(segId={}, bioId={}) to {} used time={} and status={}", segId, bioId,
                    getUrl, t.elapsedTime(), response.code());
                return response.body().bytes();
            } else {
                logger.warn(
                    "Faild to get template data from dm cluster, sendUrl={} segId={}  bioId = {} and status={}", getUrl, segId, bioId,
                    response.code());
                return null;
            }
        };
        return UidDmJobRunManager.submitGetRequest(newGetTask);
    }
    
    public static byte[] getSegment(String getUrl, Long segId) throws InterruptedException, ExecutionException {
        Callable<byte[]> newGetTask = () -> {
            final StopWatch t = new StopWatch();
            t.start();
            OkHttpClient client = new OkHttpClient();
            client.setConnectTimeout(10, TimeUnit.SECONDS);
            client.setReadTimeout(10, TimeUnit.SECONDS);
            client.setWriteTimeout(30, TimeUnit.SECONDS);
            Request request = new Request.Builder().get().url(getUrl).build();
            Call call = client.newCall(request);
            Response response = call.execute();
            t.stop();
            if (response.code() == 200) {
                logger.info("Send getSegment request(segmentId={}) to {} used time={} and status={}", segId,
                    getUrl, t.elapsedTime(), response.code());
                return response.body().bytes();
            } else {
                logger.warn("Faild to get segment data from dm cluster, sendUrl={} segmentId={} and status={}", getUrl, segId, response.code());
                return null;
            }
        };
        return UidDmJobRunManager.submitGetRequest(newGetTask);
    }
}
