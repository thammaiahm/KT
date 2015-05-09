package com.mot.upd.pcba.dao;

import com.mot.upd.pcba.pojo.DispatchSerialRequestPOJO;
import com.mot.upd.pcba.pojo.DispatchSerialResponsePOJO;

/**
 * @author HRDJ36 Thammaiah M B
 */
public interface DispatchSerialNumberDAO {

	public DispatchSerialResponsePOJO dispatchSerialNumberIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	public DispatchSerialResponsePOJO updateDispatchStatusIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO);

	public DispatchSerialResponsePOJO validateSerialNumberIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	public DispatchSerialResponsePOJO dispatchSerialNumberMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	public DispatchSerialResponsePOJO updateDispatchStatusMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO);

	public DispatchSerialResponsePOJO validateSerialNumberMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	public DispatchSerialResponsePOJO dispatchULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO);

	public DispatchSerialResponsePOJO validateULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO);

	DispatchSerialResponsePOJO validateCustomerIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	DispatchSerialResponsePOJO validateCustomerMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	DispatchSerialResponsePOJO validateGPPIDIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

	DispatchSerialResponsePOJO validateGPPIDMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO);

}
