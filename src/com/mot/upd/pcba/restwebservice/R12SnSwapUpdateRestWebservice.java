package com.mot.upd.pcba.restwebservice;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.PCBADataDictionary;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.dao.R12SnSwapMySQLDAO;
import com.mot.upd.pcba.dao.R12SnSwapOracleDAO;
import com.mot.upd.pcba.handler.PCBASerialNumberModel;
import com.mot.upd.pcba.pojo.R12SnSwapUpdateQueryInput;
import com.mot.upd.pcba.pojo.R12SnSwapUpdateQueryResult;
import com.mot.upd.pcba.utils.DBUtil;


/**
 * @author Quinnox Dev Team
 *
 */
@Path("/")
public class R12SnSwapUpdateRestWebservice {
	private static final Logger logger = Logger.getLogger(R12SnSwapUpdateRestWebservice.class);
	@GET
	@Path("/{serialIn}")
	@Produces(MediaType.APPLICATION_JSON)
	public R12SnSwapUpdateQueryResult r12SnSwapUpdateService(@PathParam("serialIn") String serialIn){
		//String serialOut = null;
		//serialIn = "353339060930372";

		logger.info(" Request serialIn value from rest webservice = " +serialIn);
		PCBASerialNumberModel pCBASerialNumberModel =null;
		R12SnSwapUpdateQueryInput r12UpdateQueryInput = new R12SnSwapUpdateQueryInput();
		R12SnSwapUpdateQueryResult r12UpdateQueryResult = new R12SnSwapUpdateQueryResult();
		try {

			r12UpdateQueryInput.setSerialNO(serialIn);
			if(isSerialLengthValid(serialIn)==true && !serialIn.equals(null)){

				R12SnSwapOracleDAO r12SwapUpdateOraDAO = new R12SnSwapOracleDAO();
				R12SnSwapMySQLDAO r12SwapUpdateMysqlDAO = new R12SnSwapMySQLDAO();

				//PCBASerialNumberModel pCBASerialNumberModel = r12SwapUpdateDAO.fetchR12SerialOutValue(r12UpdateQueryInput.getSerialNO());
				String updConfig = DBUtil.dbConfigCheck();
				logger.info("updConfig value : = " + updConfig);
				if(updConfig.equals(PCBADataDictionary.DBCONFIG)){
				 pCBASerialNumberModel = r12SwapUpdateOraDAO.fetchOldestSCROracleValue(r12UpdateQueryInput.getSerialNO());
				}else{
					pCBASerialNumberModel = r12SwapUpdateMysqlDAO.fetchOldestSCRMysqlValue(r12UpdateQueryInput.getSerialNO());
				}

				if(pCBASerialNumberModel.getNewSN() !=null ){
					r12UpdateQueryResult.setSerialIn(r12UpdateQueryInput.getSerialNO());
					r12UpdateQueryResult.setSerialOut(pCBASerialNumberModel.getNewSN());
					r12UpdateQueryResult.setResponseCode(ServiceMessageCodes.SUCCESS);
					r12UpdateQueryResult.setResponseMsg(ServiceMessageCodes.OLDEST_SCR_SERIAL_FOUND_SUCCSS_MSG);

				}else{
					r12UpdateQueryResult.setSerialIn(r12UpdateQueryInput.getSerialNO());
					r12UpdateQueryResult.setSerialOut(pCBASerialNumberModel.getNewSN());
					r12UpdateQueryResult.setResponseCode(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND);
					r12UpdateQueryResult.setResponseMsg(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_MSG);
				}
			}else{
				//rs_R12UpdateQueryResult.setSerialIn(rs_R12UpdateQueryInput.getSerialNO());
				r12UpdateQueryResult.setResponseCode(ServiceMessageCodes.SERIAL_NO_NOT_VALID);
				r12UpdateQueryResult.setResponseMsg(ServiceMessageCodes.SERIAL_NO_NOT_VALID_MSG);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return r12UpdateQueryResult;
	}
	public static boolean isSerialLengthValid(String serial_no)
	{
		int length = serial_no.length();
		if (length == 15){
			return true;
		}
		return false;
	}
}
