package jp.co.alsok.g6.zzw.web.service;

import java.net.URL;
import java.security.NoSuchAlgorithmException;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;


public class EncryptionServiceTest {
	@Autowired
	EncryptionService  encryptionService ;

	@Test
	public void testConvStringToCipher() throws NoSuchAlgorithmException {
		encryptionService = new EncryptionService();
		String passwd = "alsok1";
		String result = encryptionService.convStringToCipher(passwd);
		System.out.println(result);		
	}
	
	@Test
	public void classLoaderTest() throws ClassNotFoundException  {
		
	    URL missurl = ClassLoader.getSystemResource(this.getClass().getName());	//XX   	      
	        URL myUrl = Thread.currentThread().getContextClassLoader ().getResource(""); //OK
	      //  URL xx = Class.forName("/F:/apache-tomcat-9.0.21/webapps/alsok-g6-app/WEB-INF/classes/zwu_appLog.xml").getClassLoader().getResource("");//XX
	        //System.out.print("myUrl is "+ myUrl.getFile());       
	      // URL applogUrl = ApplicationLog.class.getClassLoader().getResource("/");//XX  
	        String path = this.getClass().getClassLoader().getResource("").getPath();
	       
	       System.out.println(System.getProperty(path));	      
	 
	    }
	
}




//junit4test.java 
//@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = AppConfig.class)
//public class XxxxTest {	
//}
//
//AppConfig.java
//@Configuration
//@ComponentScan("com.xxx.repository") // DIしたいコンポーネントがあるpackage
//public class AppConfig {
//
//    @Bean
//    XxxBean xxxBean(){
//        xxxBean bean = new xxxBean();
//        bean.setXxx(initialization data); // 何かの初期化
//        return bean;
//    }



	
