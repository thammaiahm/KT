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
public class UPDSerialSuccessFailureOracleDAO implements UPDSerialSuccessFailureInterfaceDAO{

	private static Logger logger = Logger.getLogger(UPDSerialSuccessFailureOracleDAO.class);

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

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateIMEIStatusSuccess:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			StringBuffer sb=new StringBuffer();
			sb.append("update upd.UPD_SN_REPOS set LAST_MOD_BY='pcba_pgm_success',");

			if(pcbaProgramQueryInput.getMsl()!=null && !(pcbaProgramQueryInput.getMsl().equals(""))){
				sb.append("ATTRIBUTE_25='"+pcbaProgramQueryInput.getMsl()+"',");
			}
			if(pcbaProgramQueryInput.getOtksl()!=null && !(pcbaProgramQueryInput.getOtksl().equals(""))){
				sb.append("ATTRIBUTE_28='"+pcbaProgramQueryInput.getOtksl()+"',");
			}
			if(pcbaProgramQueryInput.getServicePassCode()!=null && !(pcbaProgramQueryInput.getServicePassCode().equals(""))){
				sb.append("ATTRIBUTE_29='"+pcbaProgramQueryInput.getServicePassCode()+"',");
			}

			sb.append("LAST_MOD_DATETIME=sysdate,ATTRIBUTE_60='GSM' WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'");
			preparedStmt = con.prepareStatement(sb.toString());
			preparedStmt.execute();
			logger.info("IMEI SQL Query:"+sb.toString());

			String SQLQueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(SQLQueryIMEI);
			pstmt.execute();
			logger.info("IMEIStatusSuccess-SQLQueryIMEI: "+SQLQueryIMEI);

			String SQLQueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(SQLQueryMEID);
			prestmt.execute();
			logger.info("IMEIStatusSuccess-SQLQueryMEID:"+SQLQueryMEID);

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

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateIMEIStatusFailure:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			StringBuffer sb=new StringBuffer();

			sb.append("update upd.UPD_SN_REPOS set LAST_MOD_BY='pcba_pgm_failure',");

			if(pcbaProgramQueryInput.getMsl()!=null && !(pcbaProgramQueryInput.getMsl().equals(""))){
				sb.append("ATTRIBUTE_25='"+pcbaProgramQueryInput.getMsl()+"',");
			}
			if(pcbaProgramQueryInput.getOtksl()!=null && !(pcbaProgramQueryInput.getOtksl().equals(""))){
				sb.append("ATTRIBUTE_28='"+pcbaProgramQueryInput.getOtksl()+"',");
			}
			if(pcbaProgramQueryInput.getServicePassCode()!=null && !(pcbaProgramQueryInput.getServicePassCode().equals(""))){
				sb.append("ATTRIBUTE_29='"+pcbaProgramQueryInput.getServicePassCode()+"',");
			}
			sb.append("LAST_MOD_DATETIME=sysdate,ATTRIBUTE_60='GSM' WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'");

			preparedStmt = con.prepareStatement(sb.toString());
			preparedStmt.execute();
			logger.info("IMEI SQL Query:"+sb.toString());

			String SQLQueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(SQLQueryIMEI);
			pstmt.execute();
			logger.info("IMEIStatusFailure-SQLQueryIMEI:"+SQLQueryIMEI);

			String SQLQueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(SQLQueryMEID);
			prestmt.execute();
			logger.info("IMEIStatusFailure-SQLQueryMEID:"+SQLQueryMEID);

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

			ds = DBUtil.getOracleDataSource();
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
			sb.append("update upd.UPD_SN_REPOS set LAST_MOD_BY='pcba_pgm_success',");
			sb.append("LAST_MOD_DATETIME=sysdate,ATTRIBUTE_60='CDMA' WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'");
			preparedStmt = con.prepareStatement(sb.toString());
			preparedStmt.execute();

			logger.info("MEID SQL Query:"+sb.toString());

			String SQLQueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(SQLQueryIMEI);
			pstmt.execute();
			logger.info("MEIDStatusSuccess-SQLQueryIMEI:"+SQLQueryIMEI);

			String SQLQueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_success' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(SQLQueryMEID);
			prestmt.execute();
			logger.info("MEIDStatusSuccess-SQLQueryMEID:"+SQLQueryMEID);

			response.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
			response.setResponseMessage(ServiceMessageCodes.MEID_SUCCES_MSG);


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
	public PCBAProgramResponse updateMEIDStatusFailure(PCBAProgramQueryInput pcbaProgramQueryInput){
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in updateMEIDStatusFailure:"+e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}
		try{
			// get database connection
			con = DBUtil.getConnection(ds);

			StringBuffer sb=new StringBuffer();
			sb.append("update upd.UPD_SN_REPOS set LAST_MOD_BY='pcba_pgm_failure',");
			sb.append("LAST_MOD_DATETIME=sysdate,ATTRIBUTE_60='CDMA' WHERE serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'");
			preparedStmt = con.prepareStatement(sb.toString());
			preparedStmt.execute();

			logger.info("updateMEIDStatusFailure SQL Query:"+sb.toString());

			String SQLQueryIMEI ="update upd.upd_pcba_pgm_imei  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			conn=DBUtil.getConnection(ds);
			pstmt = conn.prepareStatement(SQLQueryIMEI);
			pstmt.execute();
			logger.info("MEIDStatusFailure-SQLQueryIMEI:"+SQLQueryIMEI);

			String SQLQueryMEID ="update upd.upd_pcba_pgm_meid  set PGM_DATE=sysdate,PGM_STATUS='pcba_pgm_failure' where serial_no='"+pcbaProgramQueryInput.getSerialNO()+"'";
			connection=DBUtil.getConnection(ds);
			prestmt = connection.prepareStatement(SQLQueryMEID);
			prestmt.execute();
			logger.info("MEIDStatusFailure-SQLQueryMEID:"+SQLQueryMEID);

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
