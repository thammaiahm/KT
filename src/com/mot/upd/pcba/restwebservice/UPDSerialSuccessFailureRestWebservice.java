package com.mot.upd.pcba.restwebservice;

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




/**
 * @author Quinnox Dev Team
 *
 */
//@Path("/updateStatusOfSerialNO")
@Path("/")
public class UPDSerialSuccessFailureRestWebservice {
	private static Logger logger = Logger.getLogger(UPDSerialSuccessFailureRestWebservice.class);

	@POST
	@Produces("application/json")
	@Consumes("application/json")
	public Response updateStatusOfSerialNO(PCBAProgramQueryInput pcbaProgramQueryInput){

		PCBAProgramResponse pcbaProgramResponse = new PCBAProgramResponse();
		UPDSerialSuccessFailureInterfaceDAO updSerialSuccessFailureInterfaceDAO =null;
		
		
		String updConfig =DBUtil.dbConfigCheck();
		
		if(updConfig!=null && updConfig.equals("YES")){
			updSerialSuccessFailureInterfaceDAO = new UPDSerialSuccessFailureOracleDAO();
			
		}else{
			updSerialSuccessFailureInterfaceDAO = new UPDSerialSuccessFailureSQLDAO();
		}
		

		boolean isMissing=false;
		boolean isValidSerial=false;
		boolean isValidStatus=false;

		//Check for Mandatory Fields in input
		isMissing =validateMandatoryInputParam(pcbaProgramQueryInput);
		if(isMissing){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.INPUT_PARAM_MISSING);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.PCBA_INPUT_PARAM_MISSING_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}

		//check if sn type is valid
		isValidSerial=validateSNType(pcbaProgramQueryInput);

		if(!isValidSerial){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.INVALID_SN_TYPE);
			pcbaProgramResponse.setResponseMessage(ServiceMessageCodes.INVALID_SN_TYPE_MSG);
			return Response.status(200).entity(pcbaProgramResponse).build();
		}
		// check if ststus is valid
		isValidStatus=validateStatus(pcbaProgramQueryInput);
		if(!isValidStatus){
			pcbaProgramResponse.setSerialNO(pcbaProgramQueryInput.getSerialNO());
			pcbaProgramResponse.setResponseCode(ServiceMessageCodes.INVALID_STATUS);
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


	private boolean validateMandatoryInputParam(
			PCBAProgramQueryInput pcbaProgramQueryInput) {
		// TODO Auto-generated method stub
		if(pcbaProgramQueryInput.getSerialNO()==null || pcbaProgramQueryInput.getRsdID()==null || pcbaProgramQueryInput.getMascID()==null || pcbaProgramQueryInput.getStatus()==null || pcbaProgramQueryInput.getSnType()==null){
			return true;
		}
		if(pcbaProgramQueryInput.getSerialNO().equals("") || pcbaProgramQueryInput.getRsdID().equals("") || pcbaProgramQueryInput.getMascID().equals("") || pcbaProgramQueryInput.getStatus().equals("") || pcbaProgramQueryInput.getSnType().equals("")){
			return true;
		}
		
		if(pcbaProgramQueryInput.getMsl()==null && pcbaProgramQueryInput.getOtksl()==null && pcbaProgramQueryInput.getServicePassCode()==null){
			return true;
		}
		
		if(pcbaProgramQueryInput.getMsl().equals("") && pcbaProgramQueryInput.getOtksl().equals("") && pcbaProgramQueryInput.getServicePassCode().equals("")){
			return true;
		}
		
		return false;
	}

}
