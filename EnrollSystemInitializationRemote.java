package jp.co.nec.lsm.tme.service.sessionbean;

import javax.ejb.Remote;

/**
 * @author liuj
 */
@Remote
public interface EnrollSystemInitializationRemote {
	public void initializeTME();
}
