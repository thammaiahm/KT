package com.mot.upd.pcba.restwebservice;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NamingException;
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

/**
 * @author HRDJ36
 *
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
		boolean isValid = false;
		boolean hasSpecial = false;

		// Check for Mandatory Fields in input
		isMissing = checkTrackID(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("TrackID missing");
			logger.info("TrackID missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.TRACK_ID_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.TRACK_ID_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkBuildType(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("BuildType missing");
			logger.info("BuildType missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.BUILD_TYPE_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.BUILD_TYPE_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkRSDID(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("RSDID missing");
			logger.info("RSDID missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.RSDID_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.RSDID_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkMASCID(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("MASCID missing");
			logger.info("MASCID missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.MASCID_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.MASCID_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkGPPID(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("GPPID missing");
			logger.info("GPPID missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.GPPID_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.GPPID_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkULMA(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("ULMA missing");
			logger.info("ULMA missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.ULMA_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.ULMA_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkSNType(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("SNType missing");
			logger.info("SNType missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SN_TYPE_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SN_TYPE_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isMissing = checkRequestType(dispatchSerialRequestPOJO);
		if (isMissing) {
			logger.debug("RequestType missing");
			logger.info("RequestType missing");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.REQUEST_TYPE_NOT_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.REQUEST_TYPE_NOT_FOUND_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isValid = validate(dispatchSerialRequestPOJO.getTrackID());
		if (!isValid) {
			logger.debug("Invalid TrackID");
			logger.info("Invalid TrackID");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_TRACK_ID);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_TRACK_ID_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isValid = validate(dispatchSerialRequestPOJO.getRsdID());
		if (!isValid) {
			logger.debug("Invalid RSDID");
			logger.info("Invalid RSDID");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_RSDID);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_RSDID_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		/*
		 * isValid =validate(dispatchSerialRequestPOJO.getMascID()); if
		 * (!isValid) { logger.debug("Invalid MascID");
		 * logger.info("Invalid MascID"); dispatchSerialResponsePOJO
		 * .setResponseCode(ServiceMessageCodes.INVALID_MASCID);
		 * dispatchSerialResponsePOJO
		 * .setResponseMsg(ServiceMessageCodes.INVALID_MASCID_MSG);
		 * logger.info("Returning response" +
		 * dispatchSerialResponsePOJO.toString());
		 * logger.debug("Returning response" +
		 * dispatchSerialResponsePOJO.toString()); return
		 * Response.status(200).entity(dispatchSerialResponsePOJO) .build(); }
		 */

		hasSpecial = checkForSpecialCharacter(dispatchSerialRequestPOJO
				.getMascID());
		if (hasSpecial) {
			logger.debug("Invalid MascID");
			logger.info("Invalid MascID");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_MASCID);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_MASCID_MSG);
			logger.info("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			logger.debug("Returning response"
					+ dispatchSerialResponsePOJO.toString());
			return Response.status(200).entity(dispatchSerialResponsePOJO)
					.build();
		}

		isValid = validate(dispatchSerialRequestPOJO.getNumberOfUlma());
		if (!isValid) {
			logger.debug("Invalid ULMA");
			logger.info("Invalid ULMA");
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.INVALID_ULMA);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.INVALID_ULMA_MSG);
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
		String updConfig = null;
		try {
			updConfig = DBUtil.dbConfigCheck();
		} catch (NamingException e) {
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e);
		} catch (SQLException e) {
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e);
		}

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
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND) {
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
			// check if protocol has special character
			hasSpecial = checkForSpecialCharacter(
					dispatchSerialRequestPOJO.getProtocol(), "_");
			if (hasSpecial) {
				logger.debug("Invalid Protocol");
				logger.info("Invalid Protocol");
				dispatchSerialResponsePOJO
						.setResponseCode(ServiceMessageCodes.INVALID_PROTOCOL);
				dispatchSerialResponsePOJO
						.setResponseMsg(ServiceMessageCodes.INVALID_PROTOCOL_MSG);
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
				if (dispatchSerialResponsePOJO.getResponseCode() == ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND) {
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

	/*
	 * private boolean validateMandatoryInputParam( DispatchSerialRequestPOJO
	 * dispatchSerialRequestPOJO) { // TODO Auto-generated method stub
	 * 
	 * if (dispatchSerialRequestPOJO.getRequestType() == null ||
	 * dispatchSerialRequestPOJO.getSnRequestType() == null ||
	 * dispatchSerialRequestPOJO.getNumberOfUlma() == 0 ||
	 * dispatchSerialRequestPOJO.getGppdID() == null ||
	 * dispatchSerialRequestPOJO.getMascID() == null ||
	 * dispatchSerialRequestPOJO.getRsdID() == null ||
	 * dispatchSerialRequestPOJO.getBuildType() == null ||
	 * dispatchSerialRequestPOJO.getTrackID() == null) { return true;
	 * 
	 * }
	 * 
	 * if (dispatchSerialRequestPOJO.getRequestType() == "" ||
	 * dispatchSerialRequestPOJO.getSnRequestType() == "" ||
	 * dispatchSerialRequestPOJO.getNumberOfUlma() == 0 ||
	 * dispatchSerialRequestPOJO.getGppdID() == "" ||
	 * dispatchSerialRequestPOJO.getMascID() == "" ||
	 * dispatchSerialRequestPOJO.getRsdID() == "" ||
	 * dispatchSerialRequestPOJO.getBuildType() == "" ||
	 * dispatchSerialRequestPOJO.getTrackID() == "") { return true;
	 * 
	 * }
	 * 
	 * return false; }
	 */

	/*
	 * chech if RequestType is missing
	 */
	private boolean checkRequestType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		if (dispatchSerialRequestPOJO.getRequestType() == null
				|| dispatchSerialRequestPOJO.getRequestType() == "") {
			return true;
		}

		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * check if SN is missing
	 */
	private boolean checkSNType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		if (dispatchSerialRequestPOJO.getSnRequestType() == null
				|| dispatchSerialRequestPOJO.getSnRequestType() == "") {
			return true;
		}
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * check if ULMA is missing
	 */
	private boolean checkULMA(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		if (dispatchSerialRequestPOJO.getNumberOfUlma() == 0) {
			return true;
		}
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * check if gppID is missing
	 */
	private boolean checkGPPID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {

		if (dispatchSerialRequestPOJO.getGppdID() == ""
				|| dispatchSerialRequestPOJO.getGppdID() == null) {
			return true;
		}
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * check if mascID is missing
	 */
	private boolean checkMASCID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getMascID() == ""
				|| dispatchSerialRequestPOJO.getMascID() == null) {
			return true;
		}
		return false;
	}

	/*
	 * check id rsdID is missing
	 */
	private boolean checkRSDID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getRsdID() == ""
				|| dispatchSerialRequestPOJO.getRsdID() == null) {
			return true;
		}
		return false;
	}

	/*
	 * check if buildType is missing
	 */
	private boolean checkBuildType(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		if (dispatchSerialRequestPOJO.getBuildType() == null
				|| dispatchSerialRequestPOJO.getBuildType() == null) {
			return true;
		}
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * check if trackID is missing
	 */
	private boolean checkTrackID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getTrackID() == ""
				|| dispatchSerialRequestPOJO.getTrackID() == null) {
			return true;
		}
		return false;
	}

	/*
	 * validate input string if it has negative number
	 */
	private boolean validate(String id) {
		// TODO Auto-generated method stub
		try {
			int number = Integer.parseInt(id);
			if (number < 0) {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	/*
	 * validate input int if it has negative number
	 */
	private boolean validate(int id) {
		// TODO Auto-generated method stub
		if (id < 0) {
			return false;
		}
		return true;
	}

	/*
	 * check if any special character apart from alphanumeric and desired
	 * character
	 */
	private boolean checkForSpecialCharacter(String id, String extrachar) {
		// TODO Auto-generated method stub
		Pattern pattren = Pattern.compile("[^a-zA-Z0-9" + extrachar + " ]",
				Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattren.matcher(id);
		boolean status = matcher.find();
		if (status) {
			return true;
		}
		return false;

	}

	/*
	 * check if any special character apart from alphanumeric
	 */
	private boolean checkForSpecialCharacter(String id) {
		// TODO Auto-generated method stub

		Pattern pattren = Pattern.compile("[^a-zA-Z0-9 ]",
				Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattren.matcher(id);
		boolean status = matcher.find();
		if (status) {
			return true;
		}
		return false;
	}

	/*
	 * verify if customer is null if null add default customer
	 */
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

	/*
	 * validate if gppID is not a number
	 */
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

	/*
	 * validate if ULMA is greater than 5
	 */
	private boolean validateULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		// TODO Auto-generated method stub
		if (dispatchSerialRequestPOJO.getNumberOfUlma() > 5) {
			return true;
		}
		return false;
	}

	/*
	 * validate if buildType is anything other than PROD and PROTO
	 */
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

	/*
	 * validate if snType is other than IMEI and MEID
	 */
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

	/*
	 * validate if requestType is other than V and D
	 */
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

}
