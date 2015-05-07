/**
 * 
 */
package com.mot.upd.pcba.pojo;

import java.io.Serializable;

/**
 * @author rviswa
 *
 */
public class PCBAProgramResponse implements Serializable{
	
	private String serialNO;
	private String    responseCode;
	private String responseMessage;
	
	public String getSerialNO() {
		return serialNO;
	}
	public void setSerialNO(String serialNO) {
		this.serialNO = serialNO;
	}
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
