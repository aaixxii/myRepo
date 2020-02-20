package com.nec.aim.uid.client;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import com.nec.aim.uid.client.util.FileUtil;
import com.nec.aim.uid.client.util.ProtobufCreater;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import jp.co.nec.aim.message.proto.AIMEnumTypes.SegmentSyncCommandType;
import jp.co.nec.aim.message.proto.AIMMessages.PBDmSyncRequest;
import jp.co.nec.aim.message.proto.AIMMessages.PBDmSyncResponce;
import jp.co.nec.aim.message.proto.AIMMessages.PBTargetSegmentVersion;
import jp.co.nec.aim.message.proto.BatchTypeProto.BatchType;
import jp.co.nec.aim.message.proto.BusinessMessage.E_BIOMETRIC_DATA_FORMAT;
import jp.co.nec.aim.message.proto.BusinessMessage.E_REQUESET_TYPE;
import jp.co.nec.aim.message.proto.BusinessMessage.PBBiometricElement;
import jp.co.nec.aim.message.proto.BusinessMessage.PBBiometricsData;
import jp.co.nec.aim.message.proto.BusinessMessage.PBBusinessMessage;
import jp.co.nec.aim.message.proto.BusinessMessage.PBRequest;
import jp.co.nec.aim.message.proto.BusinessMessage.PBTemplateInfo;
import jp.co.nec.aim.message.proto.ExtractService.ExtractRequest;
import jp.co.nec.aim.message.proto.ExtractService.ExtractResponse;
import jp.co.nec.aim.message.proto.InquiryService.IdentifyRequest;
import jp.co.nec.aim.message.proto.InquiryService.IdentifyResponse;
import jp.co.nec.aim.message.proto.JobCommonService.PBDeleteJobRequest;
import jp.co.nec.aim.message.proto.SyncService.SyncRequest;
import jp.co.nec.aim.message.proto.SyncService.SyncResponse;

public class DmPoster {
    private static final MediaType MEDIA_TYPE_PLAINTEXT = MediaType.parse("text/plain; charset=utf-8");
    
    private ProtobufCreater protobufCreater = new ProtobufCreater();
    
    private static AtomicLong lastBatchJobId;
    
    private static AtomicLong lastReqeustId;
    
    private static AtomicLong lastEnrollmentId;
    
    private String sequecFilePath;
    
    @Before
    public void setUp() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource("uid.sequece.properties");
        sequecFilePath = url.getPath();
        PropertiesConfiguration propsConfig = new PropertiesConfiguration();
        propsConfig.setEncoding("UTF-8");
        propsConfig.load(sequecFilePath);
        long batchJobId = propsConfig.getLong("BATCH_JOB_ID");
        lastBatchJobId = new AtomicLong(batchJobId);
        long reqeustId = propsConfig.getLong("REQUEST_Id");
        lastReqeustId = new AtomicLong(reqeustId);
        long enrollmentId = propsConfig.getLong("ENROLLMENT_ID");
        lastEnrollmentId = new AtomicLong(enrollmentId);
        propsConfig = null;
    }
    
    @After
    public void tearDown() throws Exception {
        try {
            PropertiesConfiguration properties = new PropertiesConfiguration(sequecFilePath);
            properties.setProperty("BATCH_JOB_ID", String.valueOf(lastBatchJobId.get()));
            properties.setProperty("REQUEST_Id", String.valueOf(lastReqeustId.get()));
            properties.setProperty("ENROLLMENT_ID", String.valueOf(lastEnrollmentId.incrementAndGet()));
            properties.save();
            properties = null;
        } catch (ConfigurationException e) {
            System.out.println(e.getMessage());
        }
    }  
    
    @Test
    public void testSyncDmInsert2() throws IOException  {
//     	PBDmSyncRequest pbReq2 = createDmRequest(2, 2000, 2, "necuid2");
     	PBDmSyncRequest pbReq3 = createDmRequest(2, 2000, 2, "necuid2");
         OkHttpClient client = new OkHttpClient();
//        client.setReadTimeout(1000, TimeUnit.MINUTES);
//        Request request2 = new Request.Builder()                        
//                 .url("http://127.0.0.1:8080/zkpdm/dmSyncSegment")
//                .post(RequestBody.create(MEDIA_TYPE_PLAINTEXT, pbReq2.toByteArray())).build();
//        Response response2 = client.newCall(request2).execute();
//        PBDmSyncResponce dmRes2 = PBDmSyncResponce.parseFrom(response2.body().bytes());
//        boolean result2 = dmRes2.getSuccess();
//        System.out.println(result2);
        
        Request request3 = new Request.Builder()                        
                .url("http://127.0.0.1:8080/zkpdm/dmSyncSegment")
               .post(RequestBody.create(MEDIA_TYPE_PLAINTEXT, pbReq3.toByteArray())).build();
       Response response3 = client.newCall(request3).execute();
       PBDmSyncResponce dmRes3 = PBDmSyncResponce.parseFrom(response3.body().bytes());
       boolean result3 = dmRes3.getSuccess();
       System.out.println(result3);
     
    }
    
    @Test
    public void testSyncDmInsert() {
    	  final PBDmSyncRequest.Builder dmSyncReq = PBDmSyncRequest.newBuilder();
    	  dmSyncReq.setCmd(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
    	  dmSyncReq.setBioId(3);
          PBTemplateInfo.Builder templateInfo = PBTemplateInfo.newBuilder();
          templateInfo.setReferenceId("necuid1");
          byte[] data = FileUtil.getDataFromFile("C:\\Users\\xia\\Desktop\\0219\\ext_result.bat");
          templateInfo.setData(ByteString.copyFrom(data));
          dmSyncReq.setTemplateData(templateInfo.build());
          PBTargetSegmentVersion.Builder segVer = PBTargetSegmentVersion.newBuilder();
          segVer.setId(10);
          segVer.setVersion(1000); 
          dmSyncReq.setTargetSegments(segVer.build());
          OkHttpClient client = new OkHttpClient();
          client.setReadTimeout(1000, TimeUnit.MINUTES);
          Request request = new Request.Builder()
                  //.url("http://10.197.23.100:8887/matchmanager/AIMExtractService/extract")            
                   .url("http://127.0.0.1:8080/zkpdm/dmSyncSegment")
                  .post(RequestBody.create(MEDIA_TYPE_PLAINTEXT, dmSyncReq.build().toByteArray())).build();
              try {
                  Response response = client.newCall(request).execute();           
                  PBDmSyncResponce dmRes = PBDmSyncResponce.parseFrom(response.body().bytes());
                  boolean result = dmRes.getSuccess();
                  System.out.println(result);
                  
              } catch (IOException e) {
                  e.printStackTrace();
              }
    	
    }
    
    private PBDmSyncRequest createDmRequest(long bioId, long segId, long segVersion, String refId) {
    	
     	  final PBDmSyncRequest.Builder dmSyncReq = PBDmSyncRequest.newBuilder();
    	  dmSyncReq.setCmd(SegmentSyncCommandType.SEGMENT_SYNC_COMMAND_INSERT);
    	  dmSyncReq.setBioId(bioId);
          PBTemplateInfo.Builder templateInfo = PBTemplateInfo.newBuilder();
          templateInfo.setReferenceId(refId);
          byte[] data = FileUtil.getDataFromFile("C:\\Users\\xia\\Desktop\\0219\\ext_result.bat");
          templateInfo.setData(ByteString.copyFrom(data));
          dmSyncReq.setTemplateData(templateInfo.build());
          PBTargetSegmentVersion.Builder segVer = PBTargetSegmentVersion.newBuilder();
          segVer.setId(segId);
          segVer.setVersion(segVersion); 
          dmSyncReq.setTargetSegments(segVer.build());
          return dmSyncReq.build();
    	
    }
    
 
  
 
    
 
    
  
}
