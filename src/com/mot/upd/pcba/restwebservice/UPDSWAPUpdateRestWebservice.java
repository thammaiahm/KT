package com.mot.upd.pcba.restwebservice;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateInterfaceDAO;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateOracleDAO;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateSQLDAO;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateQueryInput;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateResponse;
import com.mot.upd.pcba.utils.DBUtil;


/**
 * @author rviswa
 *
 */
@Path("/swapSerialNOData")
public class UPDSWAPUpdateRestWebservice {
	

	@POST
	@Produces("application/json")
	@Consumes("application/json")
	public Response swapSerialNOData(PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput){


		PCBASerialNoUPdateResponse pcbaSerialNoUPdateResponse = new PCBASerialNoUPdateResponse();

		boolean isMissing=false;
		boolean isValidSerialNoIn=false;
		boolean isValidSerialNoOut=false;
		boolean isValidDualSerialNo=false;
		boolean isValidTriSerialNo=false;

		String updConfig =DBUtil.dbConfigCheck();
		PCBASwapUPDUpdateInterfaceDAO pcbaSwapUPDUpdateInterfaceDAO =null;

		if(updConfig!=null && updConfig.equals("YES")){
			pcbaSwapUPDUpdateInterfaceDAO = new PCBASwapUPDUpdateOracleDAO();
		}else{
			pcbaSwapUPDUpdateInterfaceDAO = new PCBASwapUPDUpdateSQLDAO();
		}

		//Check for Mandatory Fields in input
		isMissing =validateMandatoryInputParam(pCBASerialNoUPdateQueryInput);
		if(isMissing){
			pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.INPUT_PARAM_MISSING);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.PCBA_INPUT_PARAM_MISSING);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
		}

		//check if SerialNoIN is valid

		isValidSerialNoIn =validateSerialNoIn(pCBASerialNoUPdateQueryInput);

		if(isValidSerialNoIn){
			pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

		}

		//check if SerialNoOut is valid

		isValidSerialNoOut =validateSerialNoOut(pCBASerialNoUPdateQueryInput);

		if(isValidSerialNoOut){
			pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.NEW_SERIAL_NO_NOT_FOUND_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

		}
		
		// check if DualSerialNo is valid
//		if(pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null || pCBASerialNoUPdateQueryInput.getDualSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals("")) && !(pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals(""))){
			
		if((pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals(""))) || (pCBASerialNoUPdateQueryInput.getDualSerialNoOut()!=null  && !(pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals("")))){
					
		
			isValidDualSerialNo = validateDualSerialNo(pCBASerialNoUPdateQueryInput);
			if(isValidDualSerialNo){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.DUAL_SERIAL_NOT_FOUND);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.DUAL_SERIAL_NOT_FOUND_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}
		}
		
		// check if TriSerialNo is valid
		
		if((pCBASerialNoUPdateQueryInput.getTriSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals(""))) || (pCBASerialNoUPdateQueryInput.getTriSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoOut().equals("")))){
			isValidTriSerialNo=validateTriSerialNo(pCBASerialNoUPdateQueryInput);
			if(isValidTriSerialNo){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.TRI_SERIAL_NOT_FOUND);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.TRI_SERIAL_NOT_FOUND_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}
			
		}

		PCBASerialNoUPdateResponse response = pcbaSwapUPDUpdateInterfaceDAO.serialNumberInfo(pCBASerialNoUPdateQueryInput);


		return Response.status(200).entity(response).build();

	}



	private boolean validateTriSerialNo(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if(pCBASerialNoUPdateQueryInput.getTriSerialNoIn()==null || pCBASerialNoUPdateQueryInput.getTriSerialNoOut()==null || pCBASerialNoUPdateQueryInput.getDualSerialNoIn()==null || pCBASerialNoUPdateQueryInput.getDualSerialNoOut()==null){
			return true;
		}
		if(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals("") || pCBASerialNoUPdateQueryInput.getTriSerialNoOut().equals("") || pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals("") || pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals("")){
			return true;
		}

		
		return false;
	}



	private boolean validateDualSerialNo(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if(pCBASerialNoUPdateQueryInput.getDualSerialNoIn()==null || pCBASerialNoUPdateQueryInput.getDualSerialNoOut()==null){
			return true;
		}
		if(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals("") || pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals("")){
			return true;
		}
		return false;
	}



	private boolean validateSerialNoOut(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if(pCBASerialNoUPdateQueryInput.getSerialNoOut() == null || pCBASerialNoUPdateQueryInput.getSerialNoOut().equals("")){
			return true;
		}
		return false;
	}



	private boolean validateSerialNoIn(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if(pCBASerialNoUPdateQueryInput.getSerialNoIn() == null || pCBASerialNoUPdateQueryInput.getSerialNoIn().equals("")){
			return true;
		}
		return false;
	}


	private boolean validateMandatoryInputParam(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub

		if(pCBASerialNoUPdateQueryInput.getClientIP()==null || pCBASerialNoUPdateQueryInput.getMascID()==null || pCBASerialNoUPdateQueryInput.getSerialNoType()==null || pCBASerialNoUPdateQueryInput.getRepairdate()==null){
			return true;
		}
		if(pCBASerialNoUPdateQueryInput.getClientIP().equals("") || pCBASerialNoUPdateQueryInput.getMascID().equals("") || pCBASerialNoUPdateQueryInput.getSerialNoType().equals("") || pCBASerialNoUPdateQueryInput.getRepairdate().equals("")){
			return true;
		}
		return false;
	}

}
