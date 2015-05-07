/**
 * 
 */
package com.mot.upd.pcba.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.pojo.PCBAProgramQueryInput;
import com.mot.upd.pcba.pojo.PCBAProgramResponse;
import com.mot.upd.pcba.utils.DBUtil;

/**
 * @author rviswa
 *
 */
public class UPDSerialSuccessFailureSQLDAO implements UPDSerialSuccessFailureInterfaceDAO{
	private static Logger logger = Logger.getLogger(UPDSerialSuccessFailureSQLDAO.class);

	
	private DataSource ds;
	private Connection con = null;
	private PreparedStatement preparedStmt = null;
	private Connection conn = null;
	private PreparedStatement pstmt = null;
	private Connection connection = null;
	private PreparedStatement prestmt = null;
	PCBAProgramResponse response = new PCBAProgramResponse();

	public PCBAProgramResponse updateIMEIStatusSuccess(PCBAProgramQueryInput pcbaProgramQueryInput){

		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateIMEIStatusSuccess:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);
			String IMEIStatusSuccess_SQL="update upd.upd_lock_code  set LAST_MOD_BY='pcba_pgm_success',motorola_master='"+pcbaProgramQueryInput.getMsl()+"',LAST_MOD_DATETIME=NOW() WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			preparedStmt = con.prepareStatement(IMEIStatusSuccess_SQL);
			preparedStmt.execute();
			logger.info("IMEI MY SQL Query:"+IMEIStatusSuccess_SQL);

			String MYSQL_QueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(MYSQL_QueryIMEI);
			pstmt.execute();
			logger.info("IMEIStatusSuccess-MY SQLQueryIMEI:"+MYSQL_QueryIMEI);

			String MYSQL_QueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(MYSQL_QueryMEID);
			prestmt.execute();
			logger.info("IMEIStatusSuccess-MY SQLQueryMEID:"+MYSQL_QueryMEID);

			response.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
			response.setResponseMessage(ServiceMessageCodes.IMEI_SUCCES_MSG);

		}catch(Exception e){
			logger.info("Update IMEIStatusSuccess error:"+e);
			response.setResponseCode(""+ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMessage(ServiceMessageCodes.SQL_EXCEPTION_MSG);
		}
		finally{
			DBUtil.connectionClosed(con, preparedStmt);
			DBUtil.connectionClosed(conn, pstmt);
			DBUtil.connectionClosed(connection, prestmt);
		}

		return response;			

	}

	public PCBAProgramResponse updateIMEIStatusFailure(PCBAProgramQueryInput pcbaProgramQueryInput){

		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateIMEIStatusFailure:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			String IMEIStatusFailure_SQL="update upd.upd_lock_code  set LAST_MOD_BY='pcba_pgm_failure',motorola_master='"+pcbaProgramQueryInput.getMsl()+"',LAST_MOD_DATETIME=NOW() WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			preparedStmt = con.prepareStatement(IMEIStatusFailure_SQL);
			preparedStmt.execute();
			logger.info("IMEI MY SQL Query:"+IMEIStatusFailure_SQL);

			String MYSQL_QueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(MYSQL_QueryIMEI);
			pstmt.execute();
			logger.info("IMEIStatusFailure-MY SQLQueryIMEI:"+MYSQL_QueryIMEI);

			String MYSQL_QueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(MYSQL_QueryMEID);
			prestmt.execute();
			logger.info("IMEIStatusFailure-MY SQLQueryMEID:"+MYSQL_QueryMEID);

			response.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			response.setResponseCode(""+ServiceMessageCodes.IMEI_FAILURE);
			response.setResponseMessage(ServiceMessageCodes.IMEI_FAILURE_MSG);

		}catch(Exception e){
			logger.info("Update IMEIStatusFailure error:"+e);
			response.setResponseCode(""+ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMessage(ServiceMessageCodes.SQL_EXCEPTION_MSG);
		}
		finally{
			DBUtil.connectionClosed(con, preparedStmt);
			DBUtil.connectionClosed(conn, pstmt);
			DBUtil.connectionClosed(connection, prestmt);
		}

		return response;
	}
	public PCBAProgramResponse updateMEIDStatusSuccess(PCBAProgramQueryInput pcbaProgramQueryInput){
		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateMEIDStatusSuccess:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}
		try{
			// get database connection
			con = DBUtil.getConnection(ds);

			StringBuffer sb=new StringBuffer();
			String MEIDStatusSuccess_SQL="update upd.upd_lock_code set LAST_MOD_BY='pcba_pgm_success',LAST_MOD_DATETIME=NOW() WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			preparedStmt = con.prepareStatement(MEIDStatusSuccess_SQL);
			preparedStmt.execute();

			logger.info("MEID MY SQL Query:"+sb.toString());

			String MYSQL_QueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(MYSQL_QueryIMEI);
			pstmt.execute();
			logger.info("MEIDStatusSuccess-MY SQLQueryIMEI:"+MYSQL_QueryIMEI);

			String MYSQL_QueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(MYSQL_QueryMEID);
			prestmt.execute();
			logger.info("MEIDStatusSuccess - MY SQLQueryMEID:"+MYSQL_QueryMEID);

			response.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
			response.setResponseMessage(ServiceMessageCodes.MEID_SUCCES_MSG);


		}catch(Exception e){
			logger.info("Update MEIDStatusSuccess error:"+e);
			response.setResponseCode(""+ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMessage(ServiceMessageCodes.SQL_EXCEPTION_MSG+e.getMessage());
		}
		finally{
			DBUtil.connectionClosed(con, preparedStmt);
			DBUtil.connectionClosed(conn, pstmt);
			DBUtil.connectionClosed(connection, prestmt);
		}

		return response;

	}

	public PCBAProgramResponse updateMEIDStatusFailure(PCBAProgramQueryInput pcbaProgramQueryInput){
		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateMEIDStatusFailure:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}
		try{
			// get database connection
			con = DBUtil.getConnection(ds);
			String MEIDStatusFailure_SQL="update upd.upd_lock_code  set LAST_MOD_BY='pcba_pgm_failure',LAST_MOD_DATETIME=NOW() WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";

			preparedStmt = con.prepareStatement(MEIDStatusFailure_SQL);
			preparedStmt.execute();

			logger.info("updateMEIDStatusFailure MY SQL Query:"+MEIDStatusFailure_SQL);

			String MYSQL_QueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(MYSQL_QueryIMEI);
			pstmt.execute();
			logger.info("MEIDStatusFailure-MY SQLQueryIMEI:"+MYSQL_QueryIMEI);

			String MYSQL_QueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=NOW(),PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(MYSQL_QueryMEID);
			prestmt.execute();
			logger.info("MEIDStatusFailure-SQLQueryMEID:"+MYSQL_QueryMEID);

			response.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			response.setResponseCode(""+ServiceMessageCodes.MEID_FAILURE);
			response.setResponseMessage(ServiceMessageCodes.MEID_FAILURE_MSG);

		}catch(Exception e){
			logger.info("Update MEIDStatusSuccess error:"+e);
			response.setResponseCode(""+ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMessage(ServiceMessageCodes.SQL_EXCEPTION_MSG);

		}
		finally{
			DBUtil.connectionClosed(con, preparedStmt);
			DBUtil.connectionClosed(conn, pstmt);
			DBUtil.connectionClosed(connection, prestmt);
		}

		return response;

	}

}
