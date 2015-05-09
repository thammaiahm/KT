package com.mot.upd.pcba.restwebservice;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.PCBADataDictionary;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.dao.DispatchSerialNumberDAO;
import com.mot.upd.pcba.dao.DispatchSerialNumberMySQLDAO;
import com.mot.upd.pcba.dao.DispatchSerialNumberOracleDAO;
import com.mot.upd.pcba.pojo.DispatchSerialRequestPOJO;
import com.mot.upd.pcba.pojo.DispatchSerialResponsePOJO;
import com.mot.upd.pcba.utils.DBUtil;

/**
 * @author HRDJ36 Thammaiah M B
 */

@Path("/dispatchserialNumber")
public class UPDDispatchSerialRestWebservice {
	private static Logger logger = Logger
			.getLogger(UPDDispatchSerialRestWebservice.class);

	@POST
	@Produces("application/json")
	@Consumes("application/json")
	public Response doGetSerialNumber(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {

		logger.info("Service doGetSerialNumber called with request attribute"
				+ dispatchSerialRequestPOJO.toString());
		logger.debug("Service doGetSerialNumber called with request attribute"
				+ dispatchSerialRequestPOJO.toString());

		DispatchSerialResponsePOJO dispatchSerialResponsePOJO = new DispatchSerialResponsePOJO();
		boolean isMissing = false;
		boolean isValidRequest = false;
		boolean isValidSerial = false;
		boolean isValidBuildType = false;
		boolean isGreaterThanFive = false;
		boolean isValidGPPID = false;

		// Check for Mandatory Fields in input
		isMissing = validateMandatoryInputParam(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("Input parameters missing");
			logger.info("Input parameters missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INPUT_PARAM_MISSING);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INPUT_PARAM_MISSING_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		// check if request type is valid
		isValidRequest = validateRequestType(dispatchSerialRequestPOJO);

		if (!isValidRequest) {
			logger.debug("Request type is invalid"
					+ dispatchSerialRequestPOJO.getRequestType());
			logger.info("Request type is invalid"
					+ dispatchSerialRequestPOJO.getRequestType());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_REQUEST_TYPE);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_REQUEST_TYPE_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		// check if sn type is valid
		isValidSerial = validateSNType(dispatchSerialRequestPOJO);
		if (!isValidSerial) {
			logger.debug("Invalid Serial Type"
					+ dispatchSerialRequestPOJO.getSnRequestType());
			logger.info("Request type is invalid"
					+ dispatchSerialRequestPOJO.getSnRequestType());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_SN_TYPE);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_SN_TYPE_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		// check if build type is valid
		isValidBuildType = validateBuildType(dispatchSerialRequestPOJO);
		if (!isValidBuildType) {
			logger.debug("Invalid Build Type"
					+ dispatchSerialRequestPOJO.getBuildType());
			logger.info("Invalid Build Type"
					+ dispatchSerialRequestPOJO.getBuildType());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_BUILD_TYPE);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_BUILD_TYPE_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}
		// check if Number of ULMA greater than 5
		isGreaterThanFive = validateULMAAddress(dispatchSerialRequestPOJO);
		if (isGreaterThanFive) {
			logger.debug("ULMA Requested is greater than 5"
					+ dispatchSerialRequestPOJO.getNumberOfUlma());
			logger.info("ULMA Requested is greater than 5"
					+ dispatchSerialRequestPOJO.getNumberOfUlma());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.ULMA_ADDRESS_GREATER_THAN_FIVE);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.ULMA_ADDRESS_GREATER_THAN_FIVE_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		// check if GPPID is numeric
		isValidGPPID = validateGPPID(dispatchSerialRequestPOJO);
		if (!isValidGPPID) {
			logger.debug("Invalid GPPID"
					+ dispatchSerialRequestPOJO.getGppdID());
			logger.info("Invalid GPPID" + dispatchSerialRequestPOJO.getGppdID());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_GPPID);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_GPPID_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}
		// check if customer is valid
		dispatchSerialRequestPOJO = verifyCustomer(dispatchSerialRequestPOJO);

		DispatchSerialNumberDAO dispatchSerialNumberDAO = null;
		String updConfig = DBUtil.dbConfigCheck();

		// Oracle
		if (updConfig.equals("YES")) {
			dispatchSerialNumberDAO = new DispatchSerialNumberOracleDAO();
		}
		// MySQL
		else {
			dispatchSerialNumberDAO = new DispatchSerialNumberMySQLDAO();
		}

		// If IMEI
		if (dispatchSerialRequestPOJO.getSnRequestType().trim()
				.equals(PCBADataDictionary.IMEI)) {

			// validate customer in DB
			dispatchSerialResponsePOJO = dispatchSerialNumberDAO
					.validateCustomerIMEI(dispatchSerialRequestPOJO);
			if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.INVALID_CUSTOMER) {
				return Response.status(200).entity(dispatchSerialResponsePOJO)
						.build();
			}

			// validate gppid
			dispatchSerialResponsePOJO = dispatchSerialNumberDAO
					.validateGPPIDIMEI(dispatchSerialRequestPOJO);
			if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.INVALID_GPPID) {
				return Response.status(200).entity(dispatchSerialResponsePOJO)
						.build();
			}

			if (dispatchSerialRequestPOJO.getRequestType().trim()
					.equals(PCBADataDictionary.REQUEST_DISPATCH)) {
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.dispatchSerialNumberIMEI(dispatchSerialRequestPOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.SQL_EXCEPTION
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NO_DATASOURCE_FOUND) {

					logger.info("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					logger.debug("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}
				// If ULMA address not available return response
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.dispatchULMAAddress(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NO_ULMA_AVAILABLE
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.SQL_EXCEPTION
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NO_DATASOURCE_FOUND) {
					logger.info("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					logger.debug("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}

				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.updateDispatchStatusIMEI(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);
			}

			// Tested
			if (dispatchSerialRequestPOJO.getRequestType().trim()
					.equals(PCBADataDictionary.REQUEST_VALIDATE)) {
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.validateSerialNumberIMEI(dispatchSerialRequestPOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND)
				{
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.validateULMAAddress(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);
			}
		}

		// * End Checking for IMEI
		if (dispatchSerialRequestPOJO.getSnRequestType().trim()
				.equals(PCBADataDictionary.MEID)) {

			// Check if protocol is present
			// Tested
			if (dispatchSerialRequestPOJO.getProtocol() == null
					|| dispatchSerialRequestPOJO.getProtocol() == "") {
				dispatchSerialResponsePOJO
						.setResponseCode(ServiceMessageCodes.NO_PROTOCOL_FOUND);
				dispatchSerialResponsePOJO
						.setResponseMsg(ServiceMessageCodes.NO_PROTOCOL_FOUND_MSG);
				logger.info("Returning response"
						+ dispatchSerialResponsePOJO.toString());
				logger.debug("Returning response"
						+ dispatchSerialResponsePOJO.toString());
				return Response.status(200).entity(dispatchSerialResponsePOJO)
						.build();
			}

			// validate customer in DB
			dispatchSerialResponsePOJO = dispatchSerialNumberDAO
					.validateCustomerMEID(dispatchSerialRequestPOJO);
			if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.INVALID_CUSTOMER) {
				return Response.status(200).entity(dispatchSerialResponsePOJO)
						.build();
			}

			// validate gppid
			dispatchSerialResponsePOJO = dispatchSerialNumberDAO
					.validateGPPIDMEID(dispatchSerialRequestPOJO);
			if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.INVALID_GPPID) {
				return Response.status(200).entity(dispatchSerialResponsePOJO)
						.build();
			}

			if (dispatchSerialRequestPOJO.getRequestType().trim()
					.equals(PCBADataDictionary.REQUEST_DISPATCH)) {

				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.dispatchSerialNumberMEID(dispatchSerialRequestPOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.SQL_EXCEPTION) {
					logger.info("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					logger.debug("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}
				// If ULMA address not available return response
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.dispatchULMAAddress(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NO_ULMA_AVAILABLE
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.SQL_EXCEPTION
						|| dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NO_DATASOURCE_FOUND) {
					logger.info("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					logger.debug("Returning response"
							+ dispatchSerialResponsePOJO.toString());
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}

				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.updateDispatchStatusMEID(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);

			}

			if (dispatchSerialRequestPOJO.getRequestType().trim()
					.equals(PCBADataDictionary.REQUEST_VALIDATE)) {

				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.validateSerialNumberMEID(dispatchSerialRequestPOJO);
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND)
				{
					return Response.status(200)
							.entity(dispatchSerialResponsePOJO).build();
				}
				dispatchSerialResponsePOJO = dispatchSerialNumberDAO
						.validateULMAAddress(dispatchSerialRequestPOJO,
								dispatchSerialResponsePOJO);
			}
		}

		// End Checking for MEID
		logger.info("Returning response"
				+ dispatchSerialResponsePOJO.toString());
		logger.debug("Returning response"
				+ dispatchSerialResponsePOJO.toString());
		return Response.status(201).entity(dispatchSerialResponsePOJO).build();
	}

	private DispatchSerialRequestPOJO verifyCustomer(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {

		if ("".equalsIgnoreCase(dispatchSerialRequestPOJO.getCustomer())
				|| dispatchSerialRequestPOJO.getCustomer() == null
				|| dispatchSerialRequestPOJO.getCustomer().length() == 0) {
			dispatchSerialRequestPOJO
					.setCustomer(PCBADataDictionary.DEFAULT_CUSTOMER);
		}
		return dispatchSerialRequestPOJO;

	}

	private boolean validateGPPID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		try {
			Integer.parseInt(dispatchSerialRequestPOJO.getGppdID());
			return true;
		} catch (NumberFormatException e) {
			return false;
		}

	}

	private boolean validateULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getNumberOfUlma() > 5) {
			return true;
		}
		return false;
	}

	private boolean validateBuildType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getBuildType().trim()
				.equals(PCBADataDictionary.BUILD_TYPE1)
				|| dispatchSerialRequestPOJO.getBuildType().trim()
						.equals(PCBADataDictionary.BUILD_TYPE2)) {
			return true;
		}

		return false;

	}

	private boolean validateSNType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getSnRequestType().trim()
				.equals(PCBADataDictionary.IMEI)
				|| dispatchSerialRequestPOJO.getSnRequestType().trim()
						.equals(PCBADataDictionary.MEID)) {
			return true;
		}

		return false;

	}

	private boolean validateRequestType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getRequestType().trim()
				.equals(PCBADataDictionary.REQUEST_VALIDATE)
				|| dispatchSerialRequestPOJO.getRequestType().trim()
						.equals(PCBADataDictionary.REQUEST_DISPATCH)) {
			return true;
		}

		return false;
	}

	private boolean validateMandatoryInputParam(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub

		if (dispatchSerialRequestPOJO.getRequestType() == null
				|| dispatchSerialRequestPOJO.getSnRequestType() == null
				|| dispatchSerialRequestPOJO.getNumberOfUlma() == 0
				|| dispatchSerialRequestPOJO.getGppdID() == null
				|| dispatchSerialRequestPOJO.getMascID() == null
				|| dispatchSerialRequestPOJO.getRsdID() == null
				|| dispatchSerialRequestPOJO.getBuildType() == null
				|| dispatchSerialRequestPOJO.getTrackID() == null) {
			return true;

		}

		if (dispatchSerialRequestPOJO.getRequestType() == ""
				|| dispatchSerialRequestPOJO.getSnRequestType() == ""
				|| dispatchSerialRequestPOJO.getNumberOfUlma() == 0
				|| dispatchSerialRequestPOJO.getGppdID() == ""
				|| dispatchSerialRequestPOJO.getMascID() == ""
				|| dispatchSerialRequestPOJO.getRsdID() == ""
				|| dispatchSerialRequestPOJO.getBuildType() == ""
				|| dispatchSerialRequestPOJO.getTrackID() == "") {
			return true;

		}

		return false;
	}

}
