package com.mot.upd.pcba.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PropertyResourceBundle;
import java.util.Set;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.handler.PCBASerialNumberModel;
import com.mot.upd.pcba.utils.DBUtil;
import com.mot.upd.pcba.utils.InitProperty;





/**
 * @author murugan
 *
 */
public class R12SnSwapOracleDAO {

	private static Logger logger = Logger.getLogger(R12SnSwapOracleDAO.class);
	PropertyResourceBundle bundle = InitProperty.getProperty("pcbasqlORA.properties");
	
		/** 
		 * Fetching oldest scrapped record for given serial number */
	public PCBASerialNumberModel fetchOldestSCROracleValue(String serialIn){

		//String serialOut = null;
		DataSource ds;
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String oldSnValue =null;
		PCBASerialNumberModel pCBASerialNumberModel = new PCBASerialNumberModel();
			try {
				
				ds = DBUtil.getOracleDataSource();
				} catch (NamingException e) {
				pCBASerialNumberModel.setResponseCode(Integer.parseInt(ServiceMessageCodes.NO_DATASOURCE_FOUND));
				pCBASerialNumberModel.setResponseMsg(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG
						+ e.getMessage());
				return pCBASerialNumberModel;
			}
			try{

				String query = bundle.getString("R12.OlddestSNFetch");
				logger.info("query : = " + query);
				conn  = ds.getConnection();
				logger.info("connection established for Oracle: " + conn);
				pstmt = conn.prepareStatement(query); 
				pstmt.setString(1, serialIn);
				
				rs = pstmt.executeQuery();
				HashMap<String, String> map = new  HashMap();
					while(rs.next()){
						map.put(rs.getString(1), rs.getString(2));
					}
					 oldSnValue = checkOddKey(map,serialIn);
					 logger.info("oldSnValue -------------- " + oldSnValue);
					pCBASerialNumberModel.setOldSN(oldSnValue);

				}catch(SQLException e){
					e.printStackTrace();
					pCBASerialNumberModel.setResponseCode(Integer.parseInt(ServiceMessageCodes.SQL_EXCEPTION));
					pCBASerialNumberModel.setResponseMsg(ServiceMessageCodes.SQL_EXCEPTION_MSG
							+ e.getMessage());
				}finally{
					DBUtil.closeConnections(conn, stmt, rs);
				}
	
			logger.info("PCBASerialNumberModel : " + pCBASerialNumberModel.toString());
			return pCBASerialNumberModel;
		}
		
	 public String checkOddKey(HashMap<String,String> contentsMap, String inputValue) {
		
			if(contentsMap.containsValue(inputValue)) {
				Iterator it = contentsMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String currentKey = (String)pair.getKey();
					String currentValue = (String)pair.getValue();
					//System.out.println(currentKey + " = " + currentValue);
					if(!currentKey.equals(inputValue)&&currentValue.equals(inputValue)) {
						inputValue = checkOddKey(contentsMap, currentKey);
					} //else System.out.println("first value"+currentKey+"--curernt Value"+currentValue);
				}
			} else {
				//logger.info("Result::"+inputValue);
				if(inputValue!=null)
				return inputValue;
			}
			return inputValue;
		}
	
	
}