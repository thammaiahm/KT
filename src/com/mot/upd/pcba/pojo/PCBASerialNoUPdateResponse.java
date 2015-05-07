/**
 * 
 */
package com.mot.upd.pcba.pojo;

import java.io.Serializable;

/**
 * @author rviswa
 *
 */
public class PCBASerialNoUPdateResponse implements Serializable{
	
	private String    responseCode;
	private String responseMessage;
	
	public String getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}
	public String getResponseMessage() {
		return responseMessage;
	}
	public void setResponseMessage(String responseMessage) {
		this.responseMessage = responseMessage;
	}
	
	

}
