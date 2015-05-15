package com.mot.upd.pcba.pojo;

import java.io.Serializable;

import javax.ws.rs.Path;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@Path("/")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "r12SnSwapUpdateQueryInput",propOrder={"serialNO"},namespace="java:com.mot.upd.pcba.pojo")
public class R12SnSwapUpdateQueryInput implements Serializable{
	
	@XmlElement(name = "serialNO", nillable=true,required=true,namespace="java:com.mot.upd.pcba.pojo")
	private String serialNO;

	public String getSerialNO() {
		return serialNO;
	}

	public void setSerialNO(String serialNO) {
		this.serialNO = serialNO;
	}
	
	@Override
	public String toString() {
		return "R12SnSwapUpdateQueryInput [serialNO=" + serialNO + "]";
	}
}
