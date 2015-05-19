package com.mot.upd.pcba.pojo;

/**
 * @author HRDJ36 Thammaiah M B
 */

public class DispatchSerialRequestPOJO {

	


	@Override
	public String toString() {
		return "DispatchSerialRequestPOJO [modelName=" + modelName
				+ ", customer=" + customer + ", requestType=" + requestType
				+ ", snRequestType=" + snRequestType + ", numberOfUlma="
				+ numberOfUlma + ", gppdID=" + gppdID + ", mascID=" + mascID
				+ ", rsdID=" + rsdID + ", buildType=" + buildType
				+ ", protocol=" + protocol + ", trackID=" + trackID
				+ ", xcvrCode=" + xcvrCode + "]";
	}
	private String modelName;
	private String customer;
	private String requestType;
	private String snRequestType;
	private int numberOfUlma;
	private String gppdID;
	private String mascID;
	private String rsdID;
	private String buildType;
	private String protocol;
	private String trackID;
	//private String pcbAssemblyPartNumber;
	private String xcvrCode;
	public String getModelName() {
		return modelName;
	}
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
	public String getCustomer() {
		return customer;
	}
	public void setCustomer(String customer) {
		this.customer = customer;
	}
	public String getRequestType() {
		return requestType;
	}
	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}
	public String getSnRequestType() {
		return snRequestType;
	}
	public void setSnRequestType(String snRequestType) {
		this.snRequestType = snRequestType;
	}
	public int getNumberOfUlma() {
		return numberOfUlma;
	}
	public void setNumberOfUlma(int numberOfUlma) {
		this.numberOfUlma = numberOfUlma;
	}
	public String getGppdID() {
		return gppdID;
	}
	public void setGppdID(String gppdID) {
		this.gppdID = gppdID;
	}
	public String getMascID() {
		return mascID;
	}
	public void setMascID(String mascID) {
		this.mascID = mascID;
	}
	public String getRsdID() {
		return rsdID;
	}
	public void setRsdID(String rsdID) {
		this.rsdID = rsdID;
	}
	public String getBuildType() {
		return buildType;
	}
	public void setBuildType(String buildType) {
		this.buildType = buildType;
	}
	public String getProtocol() {
		return protocol;
	}
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	public String getTrackID() {
		return trackID;
	}
	public void setTrackID(String trackID) {
		this.trackID = trackID;
	}
	/*public String getPcbAssemblyPartNumber() {
		return pcbAssemblyPartNumber;
	}
	public void setPcbAssemblyPartNumber(String pcbAssemblyPartNumber) {
		this.pcbAssemblyPartNumber = pcbAssemblyPartNumber;
	}*/
	public String getXcvrCode() {
		return xcvrCode;
	}
	public void setXcvrCode(String xcvrCode) {
		this.xcvrCode = xcvrCode;
	}
	
	
	

	

}
