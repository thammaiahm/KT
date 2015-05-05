package com.mot.upd.pcba.handler;

public class PCBASerialNumberModel {
	
	private String msnStatus;
	private String newSN;
	private String serialStatus;
	private int responseCode;
	private String responseMsg;
	
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	public String getResponseMsg() {
		return responseMsg;
	}
	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}

	public String getMsnStatus() {
		return msnStatus;
	}
	public void setMsnStatus(String msnStatus) {
		this.msnStatus = msnStatus;
	}
	public String getNewSN() {
		return newSN;
	}
	public void setNewSN(String newSN) {
		this.newSN = newSN;
	}
	public String getSerialStatus() {
		return serialStatus;
	}
	public void setSerialStatus(String serialStatus) {
		this.serialStatus = serialStatus;
	}
	
	
}
