package com.mot.upd.pcba.pojo;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Rs_R12UpdateQueryResult", propOrder={"serialIn", "serialOut", "responseCode","responseMsg"}
	,namespace="java:com.mot.upd.pcba.pojo")
public class R12SnSwapUpdateQueryResult implements Serializable{
	
	@XmlElement(name = "serialIn", nillable=true,required=true,namespace="java:com.mot.upd.pcba.pojo")
	private String serialIn;
	@XmlElement(name = "serialOut", nillable=true,required=true,namespace="java:com.mot.upd.pcba.pojo")
	private String serialOut;
	@XmlElement(name = "responseCode", nillable=true,required=true,namespace="java:com.mot.upd.pcba.pojo")
	private String responseCode;
	@XmlElement(name = "responseMsg", nillable=true,required=true,namespace="java:com.mot.upd.pcba.pojo")
	private String responseMsg;
	
	public String getSerialIn() {
		return serialIn;
	}
	public void setSerialIn(String serialIn) {
		this.serialIn = serialIn;
	}
	public String getSerialOut() {
		return serialOut;
	}
	public void setSerialOut(String serialOut) {
		this.serialOut = serialOut;
	}
	public String getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}
	public String getResponseMsg() {
		return responseMsg;
	}
	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}
	
	@Override
	public String toString() {
		return "Rs_R12UpdateQueryResult [serialIn=" + serialIn + ", serialOut="
				+ serialOut + ", responseCode=" + responseCode
				+ ", responseMsg=" + responseMsg + "]";
	}

}
