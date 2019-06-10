//========================================================================
//            COPYRIGHT  (C)  2018 NEC  CORPORATION
//               NEC  CONFIDENTIAL  AND  PROPRIETARY
//========================================================================
//  【ファイル名】    SZWUSession.java
//
//
//========================================================================
//  【作成者】       日本電気株式会社        2018/07/09
//  【修正名】       
//========================================================================

package jp.co.alsok.g6.zwu.web.session;

import java.util.Date;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.net.openssl.ciphers.Authentication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jp.co.alsok.g6.zwu.config.Config;
import jp.co.alsok.g6.zwu.config.ConfigKey;
import jp.co.alsok.g6.zwu.service.SZWU0000Service;
import jp.co.alsok.g6.zwu.web.constants.SZWUCommonConstants;
import jp.co.alsok.g6.zzw.web.G6Session;
import jp.co.alsok.g6.zzw.web.G6SessionControl;
import jp.co.alsok.g6.zzw.web.G6SessionImpl;

/**
 * <B>セッション管理部品</B><br/>
 * <br/>
 * 
 * @author NEC
 */
@Component
public class SZWUSession {

    /** Cookieへ埋め込むID名称 */
    private static final String COOKIE_SET_ID = "WU_COOKIE_ID";

    // セッション部品のテスト用
    @Autowired
    private G6SessionControl g6SessionControl;

    @Autowired
    HttpSession httpSession;

    @Autowired
    private HttpServletRequest request;

    @Autowired
    private HttpServletResponse response;
    /** ログイン画面のSERVICE. */
    @Autowired
    private SZWU0000Service szwu0000Service;
    
    private String userId;
    
	private Date sessionCreateDate;
    
    

    /**
     * <pre>
     * セッション情報登録
     * ・入力された引数（KEY,値）をセッションオブジェクトに登録する。
     * ・KEYに対する値が既に存在する場合は更新、しない場合は登録となる。
     * </pre>
     * 
     * @param key
     *            キー
     * @param value
     *            情報
     */
    public void setAttribute(String key, Object value) {
        httpSession.setAttribute(key, value);
        String time = SZWUCommonConstants.EMPTY;
        try {
            time = this.szwu0000Service.getPropValue("SZWU", "MAX_SESSION_TIMEOUT");
        } catch (Exception e) {
            time = "0";
        }
        if (StringUtils.isEmpty(time)) {
            time = "0";
        }
        int sessionTimeout = Integer.parseInt(time);
        httpSession.setMaxInactiveInterval(sessionTimeout);
    }

    /**
     * <pre>
     * セッション情報取得
     * ・セッショントから指定したKEYの値を取得する。
     * ・指定したキーがない場合はnullを返却する。
     * </pre>
     * 
     * @param <T>
     *            セッションオブジェクトの型
     * @param key
     *            キー
     * @return KEYに対応する値
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {

        if (StringUtils.isBlank(key)) {
            return null;
        }

        return (T) httpSession.getAttribute(key);
    }

    /**
     * <pre>
     * セッション情報取得
     * ・セッショントから指定したKEYの値を取得する。
     *  (取得後、セッションからKEYの値を削除する。)
     * </pre>
     * 
     * @param <T>
     *            セッションオブジェクトの型
     * @param key
     *            キー
     * @return KEYに対応する値
     */
    @SuppressWarnings("unchecked")
    public <T> T getFlashAttribute(String key) {

        if (StringUtils.isBlank(key)) {
            return null;
        }
        T val = (T) httpSession.getAttribute(key);
        httpSession.removeAttribute(key);

        return val;
    }

    /**
     * セッション(Http)の初期化を行います。 また、CookieにIDを登録します
     */
    public void initSession() {
        request.getSession(true).invalidate();
        this.getG6SessionID(true);
    }

    /**
     * セッション情報をDBへ保存します
     */
    public void saveG6Sassion() {
        String sessionId = getG6SessionID(false);
        Date createDate = new Date(request.getSession().getCreationTime());       
        G6Session g6s = new G6SessionImpl(sessionId, getUserId(), createDate);
        g6SessionControl.saveSession(g6s);
    }

    /**
     * 
     */
    public void removeAllSession() {
        String sessionId = getG6SessionID(false);
        Date createDate = new Date(request.getSession().getCreationTime());    
        G6Session g6session = new G6SessionImpl(sessionId, this.userId, createDate);
        g6session.removeAllSession();
        request.getSession(true).invalidate();
    }

    /**
     * 共通部品G6SesisonControlからセッション情報を返却します。 DBから取得します
     * 
     * @param key
     * @return
     */
    public Object getG6SessionValue(String key) {

        String sessionId = getG6SessionID(false);
        G6Session g6session = g6SessionControl.loadSession(sessionId, null);
        if (g6session == null) {
            return null;
        }
        return g6session.getSessionValue(key);
    }

    /**
     * 共通部品G6SesisonControlに情報を登録します DBへの保存も行います
     * 
     * @param key
     * @param obj
     */
    public void setG6SessionValue(String key, Object obj) {
        String sessionId = getG6SessionID(false);
        Date createDate = new Date(request.getSession().getCreationTime());
        G6Session g6s = new G6SessionImpl(sessionId, this.userId, createDate);
        g6s.putSessionValue(key, obj);
        g6SessionControl.saveSession(g6s);
    }

    /**
     * 共通部品G6SesisonControlから情報を削除します DBへの保存も行います
     * 
     * @param key
     */
    public void removeG6Session(String key) {
        String sessionId = getG6SessionID(false);
        Date createDate = new Date(request.getSession().getCreationTime());
        G6Session g6s = new G6SessionImpl(sessionId, this.userId, createDate);
        g6s.removeSession(key);
        g6SessionControl.saveSession(g6s);
        this.removeCookie();
    }

    /**
     * Cookieより、IDを取得します 取得できない場合、共通部品よりIDを取得し、 CookieへIDを格納します.
     */
    public String getG6SessionID() {
        return getG6SessionID(false);
    }

    /**
     * Cookieより、IDを取得します 取得できない場合、共通部品よりIDを取得し、 CookieへIDを格納します.
     * 
     * @return
     */
    private String getG6SessionID(boolean init) {

        String sessionId = null;

        // Cookieより、ターゲットを探す
        Cookie targetCookie;
        Cookie cookie = this.getCookie();

        if (cookie != null && init == false) {
            // IDが見つかった場合、取得し再設定。そして返却
            sessionId = cookie.getValue();
            request.setAttribute(COOKIE_SET_ID, sessionId);
            response.addCookie(cookie);
            return cookie.getValue();
        }

        // 見つからなかった場合、新しいIDを取得し設定。そして返却
        if (cookie != null) {
            sessionId = cookie.getValue();
            // Removeを行い、新たに作成
            
            G6Session g6s = new G6SessionImpl(sessionId, sessionId, null);
            g6s.removeAllSession();
            this.g6SessionControl.saveSession(g6s);
        }
        sessionId = g6SessionControl.generateSessionId();
        request.setAttribute(COOKIE_SET_ID, sessionId);
        targetCookie = new Cookie(COOKIE_SET_ID, sessionId);
        targetCookie.setPath("/");
        response.addCookie(targetCookie);
        return sessionId;
    }

    /**
     * responseにあるCookieを削除します 有効期間を0秒にすることで、ブラウザに削除処理をするよう促す
     * 
     */
    private void removeCookie() {

        // Cookieより、ターゲットを探す
        Cookie cookie = this.getCookie();

        if (cookie != null) {
            // IDが見つかった場合、取得し再設定。そして返却
            cookie.setPath("/");
            cookie.setMaxAge(0); // 有効期間を0秒にする
            response.addCookie(cookie);
        }

    }

    /**
     * cookieをrequestから探して返却します
     * 
     * @return cookie
     */
    private Cookie getCookie() {

        Cookie cookie = null;
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie c : cookies) {
                if (c != null && c.getName().equals(COOKIE_SET_ID)) {
                    if (!StringUtils.isEmpty(c.getValue())) {
                        cookie = c;
                    }
                    break;
                }
            }
        }

        return cookie;
    }

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Date getSessionCreateDate() {
		return sessionCreateDate;
	}

	public void setSessionCreateDate(Date sessionCreateDate) {
		this.sessionCreateDate = sessionCreateDate;
	}
	
	
    
}
