package com.mot.upd.pcba.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.PCBADataDictionary;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.pojo.DispatchSerialMEID;
import com.mot.upd.pcba.pojo.DispatchSerialRequestPOJO;
import com.mot.upd.pcba.pojo.DispatchSerialResponsePOJO;
import com.mot.upd.pcba.utils.DBUtil;
import com.mot.upd.pcba.utils.InitProperty;

/**
 * @author HRDJ36 Thammaiah M B
 */
public class DispatchSerialNumberOracleDAO implements DispatchSerialNumberDAO {
	PropertyResourceBundle bundle = InitProperty
			.getProperty("pcbasqlORA.properties");
	private static Logger logger = Logger
			.getLogger(DispatchSerialNumberOracleDAO.class);
	private DataSource ds;
	private Connection con = null;
	private PreparedStatement preparedStmt = null;
	private ResultSet rs = null;
	private DispatchSerialResponsePOJO response = new DispatchSerialResponsePOJO();
	
	
	/*
	 * validate IMEI customer is associated with serial number
	 */
	@Override
	public DispatchSerialResponsePOJO validateCustomerIMEI(DispatchSerialRequestPOJO dispatchSerialRequestPOJO)
	{
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateCustomerIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateCustomerIMEI");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}
		
		try {
			// get database connection
			con = DBUtil.getConnection(ds);
			String selectSerialNumber = bundle.getString("IMEI.validateCustomer");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, dispatchSerialRequestPOJO.getCustomer());
				rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					
					return response;
				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateCustomerIMEI:Customer is Invalid");
				logger.info("DispatchSerialNumberOracleDAO::validateCustomerIMEI:Customer is Invalid");
				response.setResponseCode(ServiceMessageCodes.INVALID_CUSTOMER);
				response.setResponseMsg(ServiceMessageCodes.INVALID_CUSTOMER_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		return response;
		
		
		
	}
	
	
	/*
	 * validate MEID customer is associated with serial number
	 */
	@Override
	public DispatchSerialResponsePOJO validateCustomerMEID(DispatchSerialRequestPOJO dispatchSerialRequestPOJO)
	{
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateCustomerIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateCustomerIMEI");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}
		
		try {
			// get database connection
			con = DBUtil.getConnection(ds);
			String selectSerialNumber = bundle.getString("MEID.validateCustomer");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, dispatchSerialRequestPOJO.getCustomer());
				rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					
					return response;
				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateCustomerIMEI:Customer is Invalid");
				logger.info("DispatchSerialNumberOracleDAO::validateCustomerIMEI:Customer is Invalid");
				response.setResponseCode(ServiceMessageCodes.INVALID_CUSTOMER);
				response.setResponseMsg(ServiceMessageCodes.INVALID_CUSTOMER_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		return response;
		
		
		
	}
	
	/*
	 * validate MEID GPPID is associated with serial number
	 */
	@Override
	public DispatchSerialResponsePOJO validateGPPIDIMEI(DispatchSerialRequestPOJO dispatchSerialRequestPOJO)
	{
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateGPPIDIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateGPPIDIMEI");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}
		
		try {
			// get database connection
			con = DBUtil.getConnection(ds);
			String selectSerialNumber = bundle.getString("IMEI.validateGPPID");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
				rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					
					return response;
				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateGPPIDIMEI: is Invalid");
				logger.info("DispatchSerialNumberOracleDAO::validateGPPIDIMEI:Customer is Invalid");
				response.setResponseCode(ServiceMessageCodes.INVALID_GPPID);
				response.setResponseMsg(ServiceMessageCodes.INVALID_GPPID_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateCustomerIMEI");
		return response;
		
		
		
	}
	
	
	/*
	 * validate MEID customer is associated with serial number
	 */
	@Override
	public DispatchSerialResponsePOJO validateGPPIDMEID(DispatchSerialRequestPOJO dispatchSerialRequestPOJO)
	{
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateGPPIDMEID");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateGPPIDMEID");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}
		
		try {
			// get database connection
			con = DBUtil.getConnection(ds);
			String selectSerialNumber = bundle.getString("MEID.validateGPPID");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
				rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					
					return response;
				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateGPPIDMEID: is Invalid");
				logger.info("DispatchSerialNumberOracleDAO::validateGPPIDMEID:Customer is Invalid");
				response.setResponseCode(ServiceMessageCodes.INVALID_GPPID);
				response.setResponseMsg(ServiceMessageCodes.INVALID_GPPID_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateGPPIDMEID");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateGPPIDMEID");
		return response;
		
		
		
	}
	
	

	/*
	 * Get the serial number which has to be dispatched when request is for IMEI
	 */
	public DispatchSerialResponsePOJO dispatchSerialNumberIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method dispatchSerialNumberIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method dispatchSerialNumberIMEI");

		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			// String selectSerialNumber =
			// "SELECT SERIAL_NO,BUILD_TYPE,CUSTOMER,GPPD_ID FROM UPD_PCBA_PGM_IMEI WHERE GPPD_ID=? and CUSTOMER=? and Build_Type=? and Dispatch_Date is null and Dispatch_Status=? and rownum=1";
			String selectSerialNumber = bundle.getString("IMEI.selectSerial");

			preparedStmt = con.prepareStatement(selectSerialNumber);

			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getCustomer());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getBuildType());
			preparedStmt.setString(4, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setString(5, PCBADataDictionary.PROGRAM_FACILITY);

			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					response.setNewSerialNo(rs.getString("SERIAL_NO"));
					response.setBuildType(rs.getString("BUILD_TYPE"));
					response.setGppdID(rs.getString("GPPD_ID"));
					response.setCustomer(rs.getString("CUSTOMER"));
				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::dispatchSerialNumberIMEI:No Serial number available in DB for dispatch");
				logger.info("DispatchSerialNumberOracleDAO::dispatchSerialNumberIMEI:No Serial number available in DB for dispatch");
				response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND);
				response.setResponseMsg(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println("error=" + e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method dispatchSerialNumberIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method dispatchSerialNumberIMEI");
		return response;
	}

	/*
	 * update Dispatch date,MascId RSDID and dispatch the serial by updating the
	 * status for IMEI
	 */
	public DispatchSerialResponsePOJO updateDispatchStatusIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO) {
		// TODO Auto-generated method stub
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method updateDispatchStatusIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method updateDispatchStatusIMEI");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e.getMessage());
			return dispatchSerialResponsePOJO;
		}
		try {
			// get database connection

			con = DBUtil.getConnection(ds);

			// update repo table

			con.setAutoCommit(false);
			// String updateDispatchStatusIMEI =
			// "UPDATE UPD_PCBA_PGM_IMEI SET DISPATCH_DATE=SYSDATE,DISPATCH_STATUS=?,RSD_ID=?,MASC_ID=?,CLIENT_REQUEST_DATETIME=SYSTIMESTAMP,LAST_MOD_BY=?,LAST_MOD_DATETIME=SYSTIMESTAMP WHERE SERIAL_NO=?";
			String updateDispatchStatusIMEI = bundle
					.getString("IMEI.updateDispatchStatus");
			preparedStmt = con.prepareStatement(updateDispatchStatusIMEI);
			preparedStmt.setString(1, PCBADataDictionary.DISPATCHED);
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getRsdID());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getMascID());
			preparedStmt.setString(4, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(5, dispatchSerialResponsePOJO
					.getNewSerialNo().trim());
			preparedStmt.executeUpdate();

			// inerst SN Repos table
			preparedStmt = null;

			// String inserSNReposIMEI =
			// "INSERT INTO UPD_SN_REPOS (SERIAL_NO,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,attribute_37) VALUES (?,?,SYSTIMESTAMP,?,SYSTIMESTAMP,'VOI-'||sysdate)";
			String inserSNReposIMEI = bundle.getString("IMEI.insertSNDeatail");
			preparedStmt = con.prepareStatement(inserSNReposIMEI);
			preparedStmt.setString(1,
					dispatchSerialResponsePOJO.getNewSerialNo());
			preparedStmt.setString(2, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(3, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(4, null);
			preparedStmt.setString(5, null);
			preparedStmt.setString(6, null);
			preparedStmt.setString(7, null);
			preparedStmt.setString(8, null);
			
			List<String> ulmaAddresses = dispatchSerialResponsePOJO.getUlmaAddress();
			int i=4;
			for (String ulma : ulmaAddresses) {
				
				preparedStmt.setString(i, ulma);
				i++;
			}
			preparedStmt.setString(9, dispatchSerialRequestPOJO.getTrackID());
			preparedStmt.setString(10, dispatchSerialRequestPOJO.getGppdID());
			
			preparedStmt.executeUpdate();

			/*
			 * Setting response parameter
			 */
			preparedStmt = null;
			dispatchSerialResponsePOJO.setMascID(dispatchSerialRequestPOJO
					.getMascID());
			dispatchSerialResponsePOJO.setRsdID(dispatchSerialRequestPOJO
					.getRsdID());
			// String selectDispatchStatus =
			// "SELECT DISPATCH_DATE FROM UPD_PCBA_PGM_IMEI WHERE SERIAL_NO=? ";
			String selectDispatchStatus = bundle
					.getString("IMEI.selectDispatchDate");

			preparedStmt = con.prepareStatement(selectDispatchStatus);
			preparedStmt.setString(1,
					dispatchSerialResponsePOJO.getNewSerialNo());
			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					dispatchSerialResponsePOJO.setDispatchedDate(rs
							.getString("DISPATCH_DATE"));

				} while (rs.next());

			}
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SUCCESS);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.OPERATION_SUCCESS);
			dispatchSerialResponsePOJO.setRequestType(dispatchSerialRequestPOJO
					.getRequestType());
			// End setting remaining response parameter

			// Updating ULMA Address
			List<String> ulmaAddress = dispatchSerialResponsePOJO
					.getUlmaAddress();
			String ulmaAddressString = null;
			for (String address : ulmaAddress) {

				if (ulmaAddressString == null) {
					ulmaAddressString = "'" + address + "'";
				} else {
					ulmaAddressString = ulmaAddressString + "," + "'" + address
							+ "'";
				}

			}
			preparedStmt = null;
			/*String updateDispatchStatusForULMA = "UPDATE upd_ulma_detail  SET LAST_MOD_USER=?,LAST_MOD_DATETIME=SYSDATE,SERIAL_NO=?,ATTRIBUTE_STATE=? WHERE ATTRIBUTE_VALUE IN("
					+ ulmaAddressString + ")";*/
			String updateDispatchStatusForULMA = "update upd.upd_ulma_repos set LAST_MOD_BY=?,dispatched_datetime=SYSDATE,is_dispatched=?,LAST_MOD_DATETIME=SYSDATE,SERIAL_NO=? where ulma in("
					+ ulmaAddressString + ")";
			
			System.out.println("Thammaiah"+updateDispatchStatusForULMA);
			preparedStmt = con.prepareStatement(updateDispatchStatusForULMA);
			preparedStmt.setString(1, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(2, PCBADataDictionary.DISPATCHED);
			preparedStmt.setString(3, dispatchSerialResponsePOJO
					.getNewSerialNo().trim());
			int rows = preparedStmt.executeUpdate();
			// update ULMA table

			con.commit();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		} catch (Exception e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method updateDispatchStatusIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method updateDispatchStatusIMEI");
		return dispatchSerialResponsePOJO;
	}

	/*
	 * Validate if there are serial number available for disatch for IMEI
	 * 
	 * @param Request attribute
	 */

	public DispatchSerialResponsePOJO validateSerialNumberIMEI(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateSerialNumberIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateSerialNumberIMEI");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			// String selectSerialNumber =
			// "SELECT SERIAL_NO,BUILD_TYPE,CUSTOMER,GPPD_ID FROM UPD_PCBA_PGM_IMEI WHERE GPPD_ID=? and CUSTOMER=? and Build_Type=? and Dispatch_Date is null and Dispatch_Status=? and rownum=1";
			String selectSerialNumber = bundle.getString("IMEI.validateSerial");

			preparedStmt = con.prepareStatement(selectSerialNumber);

			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getCustomer());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getBuildType());
			preparedStmt.setString(4, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setString(5, PCBADataDictionary.PROGRAM_FACILITY);

			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_AVAILABLE);
					response.setResponseMsg(ServiceMessageCodes.SERIAL_NO_AVAILABLE_FOR_DISPATCH_MSG);

				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateSerialNumberIMEI:No Serial Number  in DB");
				logger.info("DispatchSerialNumberOracleDAO::validateSerialNumberIMEI:No Serial Number  in DB");
				response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND);
				response.setResponseMsg(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {
			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateSerialNumberIMEI");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateSerialNumberIMEI");
		return response;
	}

	/*
	 * Select the serial number to be dispatched for MEID
	 */
	public DispatchSerialResponsePOJO dispatchSerialNumberMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method dispatchSerialNumberMEID");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method dispatchSerialNumberMEID");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			// String selectSerialNumber =
			// "SELECT SERIAL_NO,BUILD_TYPE,CUSTOMER,GPPD_ID FROM UPD_PCBA_PGM_MEID WHERE GPPD_ID=? and CUSTOMER=? and Build_Type=? and Dispatch_Date is null and Dispatch_Status=? and Protocol_name=? and rownum=1";
			String selectSerialNumber = bundle.getString("MEID.selectSerial");

			preparedStmt = con.prepareStatement(selectSerialNumber);

			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getCustomer());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getBuildType());
			preparedStmt.setString(4, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setString(5, dispatchSerialRequestPOJO.getProtocol());
			preparedStmt.setString(6, PCBADataDictionary.PROGRAM_FACILITY);

			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					response.setNewSerialNo(rs.getString("SERIAL_NO"));
					response.setBuildType(rs.getString("BUILD_TYPE"));
					response.setGppdID(rs.getString("GPPD_ID"));
					response.setCustomer(rs.getString("CUSTOMER"));
					DispatchSerialMEID.setaKey1Type(rs.getString("AKEY1_TYPE"));
					DispatchSerialMEID.setaKey1Value(rs.getString("AKEY1_VALUE"));
					DispatchSerialMEID.setaKey2Type(rs.getString("AKEY2_TYPE"));
					DispatchSerialMEID.setaKey2Value(rs.getString("AKEY2_VALUE"));
					DispatchSerialMEID.setMasterSubLockCode(rs.getString("MASTER_SUBLOCK_CODE"));
					DispatchSerialMEID.setOneTimeSublockCode(rs.getString("ONETIME_SBLOCK_CODE"));
					

				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::dispatchSerialNumberMEID:No Serial number available for dispatch");
				logger.info("DispatchSerialNumberOracleDAO:dispatchSerialNumberMEID:No Serial number available for dispatch");
				response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND);
				response.setResponseMsg(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method dispatchSerialNumberMEID");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method dispatchSerialNumberMEID");
		return response;
	}

	/*
	 * DIspatch MEID by updating the status
	 */
	public DispatchSerialResponsePOJO updateDispatchStatusMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method updateDispatchStatusMEID");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method updateDispatchStatusMEID");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e.getMessage());
			return dispatchSerialResponsePOJO;
		}
		try {
			// get database connection

			con = DBUtil.getConnection(ds);

			// update repo table

			con.setAutoCommit(false);

			// String updateDispatchStatusMEID =
			// "UPDATE UPD_PCBA_PGM_MEID SET DISPATCH_DATE=SYSDATE,DISPATCH_STATUS=?,RSD_ID=?,MASC_ID=?,CLIENT_REQUEST_DATETIME=SYSTIMESTAMP,LAST_MOD_BY=?,LAST_MOD_DATETIME=SYSTIMESTAMP WHERE SERIAL_NO=?";
			String updateDispatchStatusMEID = bundle
					.getString("MEID.updateDispatchStatus");

			preparedStmt = con.prepareStatement(updateDispatchStatusMEID);
			preparedStmt.setString(1, PCBADataDictionary.DISPATCHED);
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getRsdID());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getMascID());
			preparedStmt.setString(4, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(5, dispatchSerialResponsePOJO
					.getNewSerialNo().trim());
			preparedStmt.executeUpdate();

			// inerst SN Repos table
			preparedStmt = null;
			// String inserSNReposIMEI =
			// "INSERT INTO UPD_SN_REPOS (SERIAL_NO,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,attribute_37) VALUES (?,?,SYSTIMESTAMP,?,SYSTIMESTAMP,'VOI-'||sysdate)";
			String inserSNReposMEID = bundle.getString("MEID.insertSNDeatail");

			preparedStmt = con.prepareStatement(inserSNReposMEID);
			preparedStmt.setString(1,
					dispatchSerialResponsePOJO.getNewSerialNo());
			preparedStmt.setString(2, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(3, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(4, null);
			preparedStmt.setString(5, null);
			preparedStmt.setString(6, null);
			preparedStmt.setString(7, null);
			preparedStmt.setString(8, null);
			
			List<String> ulmaAddresses = dispatchSerialResponsePOJO.getUlmaAddress();
			int i=4;
			for (String ulma : ulmaAddresses) {
				
				preparedStmt.setString(i, ulma);
				i++;
			}
			
			preparedStmt.setString(9, dispatchSerialRequestPOJO.getTrackID());
			preparedStmt.setString(10, dispatchSerialRequestPOJO.getGppdID());
			preparedStmt.setString(11, null);
			preparedStmt.setString(12, null);
			preparedStmt.setString(13, null);
			preparedStmt.setString(14, null);
			
			//LOCK CODES AKEY1
			if(DispatchSerialMEID.getaKey1Type().trim().equalsIgnoreCase("ZERO"))
			{
				preparedStmt.setString(12, DispatchSerialMEID.getaKey1Value());
			}
			
			if(DispatchSerialMEID.getaKey1Type().trim().equalsIgnoreCase("RANDOM") || DispatchSerialMEID.getaKey1Type().trim().equalsIgnoreCase("SPRINT RANDOM"))
			{
				preparedStmt.setString(11, DispatchSerialMEID.getaKey1Value());
			}
			
			//LOCK CODES AKEY1
			if(DispatchSerialMEID.getaKey2Type()!=null && DispatchSerialMEID.getaKey2Value()!=null)
			{
				preparedStmt.setString(13, DispatchSerialMEID.getaKey2Type());
				preparedStmt.setString(14, DispatchSerialMEID.getaKey2Value());
			}
			
			preparedStmt.setString(15, DispatchSerialMEID.getMasterSubLockCode());
			preparedStmt.setString(16, DispatchSerialMEID.getOneTimeSublockCode());
			
			
			
			
			
			
			
			
			
			preparedStmt.executeUpdate();

			/*
			 * Setting response parameter
			 */
			preparedStmt = null;
			dispatchSerialResponsePOJO.setMascID(dispatchSerialRequestPOJO
					.getMascID());
			dispatchSerialResponsePOJO.setRsdID(dispatchSerialRequestPOJO
					.getRsdID());
			// String selectDispatchStatus =
			// "SELECT DISPATCH_DATE FROM UPD_PCBA_PGM_MEID WHERE SERIAL_NO=? ";
			String selectDispatchStatus = bundle
					.getString("MEID.selectDispatchDate");

			preparedStmt = con.prepareStatement(selectDispatchStatus);
			preparedStmt.setString(1,
					dispatchSerialResponsePOJO.getNewSerialNo());
			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					dispatchSerialResponsePOJO.setDispatchedDate(rs
							.getString("DISPATCH_DATE"));

				} while (rs.next());

			}

			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SUCCESS);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.OPERATION_SUCCESS);
			dispatchSerialResponsePOJO.setRequestType(dispatchSerialRequestPOJO
					.getRequestType());
			// End setting remaining response parameter

			// Updating ULMA Address
			List<String> ulmaAddress = dispatchSerialResponsePOJO
					.getUlmaAddress();
			String ulmaAddressString = null;
			for (String address : ulmaAddress) {

				if (ulmaAddressString == null) {
					ulmaAddressString = "'" + address + "'";
				} else {
					ulmaAddressString = ulmaAddressString + "," + "'" + address
							+ "'";
				}

			}

			// update ULMA table
			preparedStmt = null;
			/*String updateDispatchStatusForULMA = "UPDATE upd_ulma_detail  SET LAST_MOD_USER=?,LAST_MOD_DATETIME=SYSDATE,SERIAL_NO=?,ATTRIBUTE_STATE=? WHERE ATTRIBUTE_VALUE IN("
					+ ulmaAddressString + ")";*/
			String updateDispatchStatusForULMA = "update upd.upd_ulma_repos set LAST_MOD_BY=?,dispatched_datetime=SYSDATE,is_dispatched=?,LAST_MOD_DATETIME=SYSDATE,SERIAL_NO=? where ulma in("
					+ ulmaAddressString + ")";
			preparedStmt = con.prepareStatement(updateDispatchStatusForULMA);
			preparedStmt.setString(1, PCBADataDictionary.MODIFIED_BY);
			preparedStmt.setString(2, PCBADataDictionary.DISPATCHED);
			preparedStmt.setString(3, dispatchSerialResponsePOJO
					.getNewSerialNo().trim());
			int rows = preparedStmt.executeUpdate();
			// update ULMA table

			con.commit();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		} catch (Exception e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method updateDispatchStatusMEID");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method updateDispatchStatusMEID");

		return dispatchSerialResponsePOJO;
	}

	/*
	 * Validate if MEID available for Dispatch
	 * 
	 * @see
	 * com.mot.upd.pcba.dao.DispatchSerialNumberDAO#validateSerialNumberMEID
	 * (com.mot.upd.pcba.pojo.DispatchSerialRequestPOJO)
	 */
	public DispatchSerialResponsePOJO validateSerialNumberMEID(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateSerialNumberMEID");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateSerialNumberMEID");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
					+ e.getMessage());
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			// String selectSerialNumber =
			// "SELECT SERIAL_NO,BUILD_TYPE,CUSTOMER,GPPD_ID FROM UPD_PCBA_PGM_MEID WHERE GPPD_ID=? and CUSTOMER=? and Build_Type=? and Dispatch_Date is null and Dispatch_Status=? and Protocol_name=? and rownum=1";
			String selectSerialNumber = bundle.getString("MEID.validateSerial");

			preparedStmt = con.prepareStatement(selectSerialNumber);

			preparedStmt.setString(1, dispatchSerialRequestPOJO.getGppdID());
			preparedStmt.setString(2, dispatchSerialRequestPOJO.getCustomer());
			preparedStmt.setString(3, dispatchSerialRequestPOJO.getBuildType());
			preparedStmt.setString(4, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setString(5, dispatchSerialRequestPOJO.getProtocol());
			preparedStmt.setString(6, PCBADataDictionary.PROGRAM_FACILITY);

			rs = preparedStmt.executeQuery();
			if (rs.next()) {
				do {
					response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_AVAILABLE);
					response.setResponseMsg(ServiceMessageCodes.SERIAL_NO_AVAILABLE_FOR_DISPATCH_MSG);

				} while (rs.next());

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateSerialNumberMEID:No Serial number in DB");
				logger.info("DispatchSerialNumberOracleDAO::validateSerialNumberMEID:No Serial number in DB");
				response.setResponseCode(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND);
				response.setResponseMsg(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			response.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateSerialNumberMEID");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method validateSerialNumberMEID");
		return response;

	}

	/*
	 * Dispatch ULMA Adress
	 * 
	 * @see
	 * com.mot.upd.pcba.dao.DispatchSerialNumberDAO#dispatchULMAAddress(com.
	 * mot.upd.pcba.pojo.DispatchSerialRequestPOJO,
	 * com.mot.upd.pcba.pojo.DispatchSerialResponsePOJO)
	 */
	@Override
	public DispatchSerialResponsePOJO dispatchULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method dispatchULMAAddress");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method dispatchULMAAddress");
		List<String> ulmaAddress = new ArrayList<String>();
		// TODO Auto-generated method stub
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e.getMessage());
			return dispatchSerialResponsePOJO;
		}
		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			//String selectSerialNumber = "SELECT ATTRIBUTE_VALUE FROM upd_ulma_detail where ATTRIBUTE_STATE=? AND ROWNUM<=?";
			String selectSerialNumber  = bundle.getString("WS.dispatchULMA");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setInt(2, dispatchSerialRequestPOJO.getNumberOfUlma());
			rs = preparedStmt.executeQuery();
			int count =0;
			if (rs.next()) {
				do {

					ulmaAddress.add(rs.getString("ulma"));
					count++;

				} while (rs.next());
				dispatchSerialResponsePOJO.setUlmaAddress(ulmaAddress);
				if(count!=dispatchSerialRequestPOJO.getNumberOfUlma())
				{
					logger.debug("DispatchSerialNumberMySQLDAO::validateULMAAddress:No ULMA Available in DB");
					logger.info("DispatchSerialNumberMySQLDAO::validateULMAAddress:No ULMA Available in DB");
					dispatchSerialResponsePOJO.reset();
					dispatchSerialResponsePOJO
							.setResponseCode(ServiceMessageCodes.NO_ULMA_AVAILABLE);
					dispatchSerialResponsePOJO
							.setResponseMsg(ServiceMessageCodes.NO_ULMA_AVAILABLE_MSG);

					return dispatchSerialResponsePOJO;
				}
			} else {
				logger.debug("DispatchSerialNumberOracleDAO::dispatchULMAAddress:No ULMA to dispatch");
				logger.info("DispatchSerialNumberOracleDAO::dispatchULMAAddress:No ULMA to dispatch");
				dispatchSerialResponsePOJO.reset();
				dispatchSerialResponsePOJO
						.setResponseCode(ServiceMessageCodes.NO_ULMA_AVAILABLE);
				dispatchSerialResponsePOJO
						.setResponseMsg(ServiceMessageCodes.NO_ULMA_AVAILABLE_MSG);

				return dispatchSerialResponsePOJO;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method dispatchULMAAddress");
		logger.info("DispatchSerialNumberOracleDAO:Leaving Method dispatchULMAAddress");

		return dispatchSerialResponsePOJO;

	}

	/*
	 * validate if ULMA available for dispatch
	 * 
	 * @see
	 * com.mot.upd.pcba.dao.DispatchSerialNumberDAO#validateULMAAddress(com.
	 * mot.upd.pcba.pojo.DispatchSerialRequestPOJO,
	 * com.mot.upd.pcba.pojo.DispatchSerialResponsePOJO)
	 */
	@Override
	public DispatchSerialResponsePOJO validateULMAAddress(
			DispatchSerialRequestPOJO dispatchSerialRequestPOJO,
			DispatchSerialResponsePOJO dispatchSerialResponsePOJO) {
		logger.debug("DispatchSerialNumberOracleDAO:Entered Method validateULMAAddress");
		logger.info("DispatchSerialNumberOracleDAO:Entered Method validateULMAAddress");
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
							+ e.getMessage());
			return response;
		}
		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			//String selectSerialNumber = "SELECT ATTRIBUTE_VALUE FROM upd_ulma_detail where ATTRIBUTE_STATE=? AND ROWNUM<=?";
			String selectSerialNumber  = bundle.getString("WS.validateULMA");

			preparedStmt = con.prepareStatement(selectSerialNumber);
			preparedStmt.setString(1, PCBADataDictionary.UNDISPATCHED);
			preparedStmt.setInt(2, dispatchSerialRequestPOJO.getNumberOfUlma());
			rs = preparedStmt.executeQuery();
			int count =0;
			if (rs.next()) {
				do {
					// Nothing
					count++;

				} while (rs.next());
				if(count!=dispatchSerialRequestPOJO.getNumberOfUlma())
				{
					logger.debug("DispatchSerialNumberMySQLDAO::validateULMAAddress:No ULMA Available in DB");
					logger.info("DispatchSerialNumberMySQLDAO::validateULMAAddress:No ULMA Available in DB");
					dispatchSerialResponsePOJO.reset();
					dispatchSerialResponsePOJO
							.setResponseCode(ServiceMessageCodes.NO_ULMA_AVAILABLE);
					dispatchSerialResponsePOJO
							.setResponseMsg(ServiceMessageCodes.NO_ULMA_AVAILABLE_MSG);

					return dispatchSerialResponsePOJO;
				}

			} else {
				logger.debug("DispatchSerialNumberOracleDAO::validateULMAAddress:No ULMA Available in DB");
				logger.info("DispatchSerialNumberOracleDAO::validateULMAAddress:No ULMA Available in DB");
				dispatchSerialResponsePOJO.reset();
				dispatchSerialResponsePOJO
						.setResponseCode(ServiceMessageCodes.NO_ULMA_AVAILABLE);
				dispatchSerialResponsePOJO
						.setResponseMsg(ServiceMessageCodes.NO_ULMA_AVAILABLE_MSG);

				return response;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			dispatchSerialResponsePOJO.reset();
			dispatchSerialResponsePOJO
					.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			dispatchSerialResponsePOJO
					.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());

		} finally {

			DBUtil.closeConnections(con, preparedStmt, rs);

		}
		logger.debug("DispatchSerialNumberOracleDAO:Leaving Method validateULMAAddress");
		logger.info("DispatchSerialNumberOracleDAO:Leavingd Method validateULMAAddress");
		return dispatchSerialResponsePOJO;
	}

}
