/**
 * 
 */
package com.mot.upd.pcba.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.PropertyResourceBundle;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.handler.PCBASerialNumberModel;

/**
 * @author rviswa
 *
 */
public class DBUtil {

	private static Logger logger = Logger.getLogger(DBUtil.class);
	private static PropertyResourceBundle bundle = InitProperty
			.getProperty("pcbaJNDI.properties");
	

	public static DataSource getOracleDataSource() throws NamingException
	{
		logger.info("DBUtil Inside  DataSource method inside");
		DataSource ds = null;
		String jndiName = bundle.getString("pcbaOracleJNDI");
		try {
			Context ctx = new InitialContext();
			ds = (DataSource) ctx.lookup(jndiName);
		} catch (NamingException e) {
			throw e;
		}
		
		logger.info("DataSource method end");

		return ds;
	}
	public static DataSource getMySqlDataSource() throws NamingException
	{
		logger.info("DBUtil Inside  DataSource method inside");
		DataSource ds = null;
		String jndiName = bundle.getString("pcbaMySQLJNDI");
		try {
			Context ctx = new InitialContext();
			ds = (DataSource) ctx.lookup(jndiName);
		} catch (NamingException e) {
			throw e;
		}
		logger.info("DataSource method end");

		return ds;
	}
	public static String dbConfigCheck(){
		
		DataSource ds;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String updConfig = null;
		String dbconfig = bundle.getString("dbConfig");
		String query = "select value from upd.upd_config where key = " +
				 "'"+ dbconfig + "'";
		logger.info("updconfig query " + query);
		PCBASerialNumberModel pCBASerialNumberModel = new PCBASerialNumberModel();
		
		try{
			
				ds = DBUtil.getOracleDataSource();
				conn  = ds.getConnection();
				System.out.println("connection " + conn);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(query);
				while(rs.next()){
					updConfig = rs.getString(1);
	
					}

		}catch(NamingException e){
			 e.printStackTrace();
		}catch(SQLException e){
			pCBASerialNumberModel.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			pCBASerialNumberModel.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());
		}finally{
			
				DBUtil.closeConnections(conn, stmt, rs);
		}
		return updConfig;
	}
	public static Connection getConnection(DataSource ds) throws SQLException
	{
		logger.info("DBUtil Inside  Connection method inside");
		PCBASerialNumberModel pCBASerialNumberModel = new PCBASerialNumberModel();
		Connection con = null;
		try {
			con = ds.getConnection();
		} catch (SQLException e) {
			pCBASerialNumberModel.setResponseCode(ServiceMessageCodes.SQL_EXCEPTION);
			pCBASerialNumberModel.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());
		}
		return con;
	}
	public static void closeConnections(Connection con,Statement stmt,ResultSet rs){
		logger.info("DBUtil Inside  closeConnections method inside");

		try {
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void closeConnection(Connection con,PreparedStatement preparedStmt,ResultSet rs){
		logger.info("DBUtil Inside  closeConnection method inside.");
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (preparedStmt != null) {
				preparedStmt.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void connectionClosed(Connection con,PreparedStatement preparedStmt){
		logger.info("DBUtil Inside  closeConnection method inside.");
		
		try {
			if (preparedStmt != null) {
				preparedStmt.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
