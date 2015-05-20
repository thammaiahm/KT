package com.mot.upd.pcba.restwebservice;

import java.sql.SQLException;

import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.PCBADataDictionary;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.dao.UPDSerialSuccessFailureInterfaceDAO;
import com.mot.upd.pcba.dao.UPDSerialSuccessFailureOracleDAO;
import com.mot.upd.pcba.dao.UPDSerialSuccessFailureSQLDAO;
import com.mot.upd.pcba.pojo.PCBAProgramQueryInput;
import com.mot.upd.pcba.pojo.PCBAProgramResponse;
import com.mot.upd.pcba.utils.DBUtil;
import com.mot.upd.pcba.utils.MEIDException;






/**
 * @author Quinnox Dev Team
 *
 */
@Path("/successFailureRS")
public class UPDSerialSuccessFailureRestWebservice {
	private static Logger logger = Logger.getLogger(UPDSerialSuccessFailureRestWebservice.class);

	@POST
	@Produces("application/json")
	@Consumes("application/json")
	public Response updateStatusOfSerialNO(PCBAProgramQueryInput pcbaProgramQueryInput){

		PCBAProgramResponse pcbaProgramResponse = new PCBAProgramResponse();
		UPDSerialSuccessFailureInterfaceDAO updSerialSuccessFailureInterfaceDAO =null;

		String updConfig = null;
		try {
			updConfig = DBUtil.dbConfigCheck();
		}catch (NamingException e) {		
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG + e);
		} catch (SQLException e) {
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG + e);
		}

		if(updConfig!=null && updConfig.equals("YES")){
			updSerialSuccessFailureInterfaceDAO = new UPDSerialSuccessFailureOracleDAO();

		}else{
			updSerialSuccessFailureInterfaceDAO = new UPDSerialSuccessFailureSQLDAO();
		}


		boolean isMissing=false;
		boolean isValidSerial=false;
		boolean isValidStatus=false;
		boolean isValidsntype=false;
		boolean isValidStatusCheck=false;
		boolean isValidSntypeCheck=false;

		//Check for serialNo Mandatory Field
		isMissing =validateMandatoryInputSerialNO(pcbaProgramQueryInput);
		if(isMissing){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(""+ServiceMessageCodes.INPUT_PARAM_MISSING);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.PCBA_INPUT_PARAM_SERIAL_NO_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}
		//check for sntype=IMEI and status=s
		isValidsntype = validateMandatoryInputsntype(pcbaProgramQueryInput);
		if(isValidsntype){


			boolean isValid =false;
			if(pcbaProgramQueryInput.getMsl()!=null && !pcbaProgramQueryInput.getMsl().equals("")){
				isValid = true;
			}if(pcbaProgramQueryInput.getOtksl()!=null && !pcbaProgramQueryInput.getOtksl().equals("")){
				isValid = true;
			}
			if(pcbaProgramQueryInput.getServicePassCode()!=null && !pcbaProgramQueryInput.getServicePassCode().equals("")){
				isValid = true;
			}

			if(!isValid){
				pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
				pcbaProgramResponse.setResponseCode(ServiceMessageCodes.MANDATORY_STATUS_CODE);
				pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.MANDATORY_STATUS_MSG);
				return Response.status(200).entity(pcbaProgramResponse).build();

			}

		}

		//Check Valid serialNo
		if(pcbaProgramQueryInput.getSerialNO()!=null && !(pcbaProgramQueryInput.getSerialNO().equals(""))){

			String statusOfSerialNoIn = null;
			try {
				statusOfSerialNoIn = DBUtil.checkValidSerialNumber(pcbaProgramQueryInput.getSerialNO(),"SerialNo");
			} catch (MEIDException e) {
				pcbaProgramResponse.setResponseCode(ServiceMessageCodes.INVALID+"SerialNO");
				pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
			}
			if(statusOfSerialNoIn.length() == 15){
				pcbaProgramQueryInput.setSerialNO(statusOfSerialNoIn);
			}else{
				pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
				pcbaProgramResponse.setResponseCode(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
				pcbaProgramResponse.setResponseMessage(statusOfSerialNoIn);
				return Response.status(200).entity(pcbaProgramResponse).build();

			}
		}
		//mandatory filed check for SN Type
		isValidSntypeCheck=validateMandatroySNType(pcbaProgramQueryInput);

		if(isValidSntypeCheck){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.SNTYPE_CODE);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.SNTYPE_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}

		//check if sn type is valid
		isValidSerial=validateSNType(pcbaProgramQueryInput);

		if(!isValidSerial){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(""+ServiceMessageCodes.INVALID_SN_TYPE);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.INVALID_SN_TYPE_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}


		isValidStatusCheck=validStatusCheck(pcbaProgramQueryInput);

		if(isValidStatusCheck){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.STATUS_CODE);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.STATUS_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}



		//
		// check if ststus is valid
		isValidStatus=validateStatus(pcbaProgramQueryInput);
		if(!isValidStatus){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(""+ServiceMessageCodes.INVALID_STATUS);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.INVALID_STATUS_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}

		if(pcbaProgramQueryInput.getSnType()!=null && pcbaProgramQueryInput.getSnType().trim().equals(PCBADataDictionary.IMEI)){
			if(pcbaProgramQueryInput.getStatus().trim().equalsIgnoreCase("s")){
				PCBAProgramResponse pCBAProgramResponseForIMEI = updSerialSuccessFailureInterfaceDAO.updateIMEIStatusSuccess(pcbaProgramQueryInput);
				return Response.status(200).entity(pCBAProgramResponseForIMEI).build();
			}else{
				PCBAProgramResponse pCBAProgramResponseForIMEI = updSerialSuccessFailureInterfaceDAO.updateIMEIStatusFailure(pcbaProgramQueryInput);
				return Response.status(200).entity(pCBAProgramResponseForIMEI).build();

			}

		}else if(pcbaProgramQueryInput.getSnType()!=null && pcbaProgramQueryInput.getSnType().trim().equals(PCBADataDictionary.MEID)){
			if(pcbaProgramQueryInput.getStatus().trim().equalsIgnoreCase("s")){
				PCBAProgramResponse pcbaProgramResponseForMEID=updSerialSuccessFailureInterfaceDAO.updateMEIDStatusSuccess(pcbaProgramQueryInput);
				return Response.status(200).entity(pcbaProgramResponseForMEID).build();
			}else{
				PCBAProgramResponse pcbaProgramResponseForMEID=updSerialSuccessFailureInterfaceDAO.updateMEIDStatusFailure(pcbaProgramQueryInput);
				return Response.status(200).entity(pcbaProgramResponseForMEID).build();

			}

		}

		return Response.status(200).entity(pcbaProgramResponse).build();
	}


	private boolean validateStatus(PCBAProgramQueryInput pcbaProgramQueryInput) {
		// TODO Auto-generated method stub
		if(pcbaProgramQueryInput.getStatus().trim().equalsIgnoreCase(PCBADataDictionary.STATUS_S) || pcbaProgramQueryInput.getStatus().trim().equalsIgnoreCase(PCBADataDictionary.STAYUS_F)){
			return true;
		}
		return false;
	}


	private boolean validateSNType(PCBAProgramQueryInput pcbaProgramQueryInput) {
		// TODO Auto-generated method stub
		if(pcbaProgramQueryInput.getSnType().trim().equals(PCBADataDictionary.IMEI) || pcbaProgramQueryInput.getSnType().trim().equals(PCBADataDictionary.MEID)){
			return true;
		}
		return false;
	}


	private boolean validateMandatoryInputSerialNO(
			PCBAProgramQueryInput pcbaProgramQueryInput) {
		// TODO Auto-generated method stub

		if(pcbaProgramQueryInput.getSerialNO()==null){
			return true;
		}
		if(pcbaProgramQueryInput.getSerialNO().equals("")){
			return true;
		}

		return false;
	}

	private boolean validateMandatoryInputsntype(PCBAProgramQueryInput pcbaProgramQueryInput) {
		if(pcbaProgramQueryInput.getSnType()!=null && pcbaProgramQueryInput.getStatus()!=null){
			if(((!(pcbaProgramQueryInput.getSnType().equals(""))) && (pcbaProgramQueryInput.getSnType().equals("IMEI"))) && (((!(pcbaProgramQueryInput.getStatus().equals(""))) && pcbaProgramQueryInput.getStatus().equals("S")))){
				return true;	
			}
		}

		return false;
	}

	private boolean validateMandatroySNType(
			PCBAProgramQueryInput pcbaProgramQueryInput) {
		if(pcbaProgramQueryInput.getSnType()==null){
			return true;
		}
		if(pcbaProgramQueryInput.getSnType().equals("")){
			return true;
		}

		return false;
	}
	private boolean validStatusCheck(
			PCBAProgramQueryInput pcbaProgramQueryInput) {
		if(pcbaProgramQueryInput.getStatus()==null){
			return true;
		}
		if(pcbaProgramQueryInput.getStatus().equals("")){
			return true;
		}

		return false;
	}

}
