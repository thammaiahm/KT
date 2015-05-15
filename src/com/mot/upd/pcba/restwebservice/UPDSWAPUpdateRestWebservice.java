package com.mot.upd.pcba.restwebservice;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.mot.upd.pcba.constants.PCBADataDictionary;
import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateInterfaceDAO;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateOracleDAO;
import com.mot.upd.pcba.dao.PCBASwapUPDUpdateSQLDAO;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateQueryInput;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateResponse;
import com.mot.upd.pcba.utils.DBUtil;
import com.mot.upd.pcba.utils.MEIDException;

/**
 * @author rviswa
 *
 */
@Path("/swapUpdateRS")
public class UPDSWAPUpdateRestWebservice {


	@POST
	@Produces("application/json")
	@Consumes("application/json")
	public Response swapSerialNOData(PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) throws MEIDException{


		PCBASerialNoUPdateResponse pcbaSerialNoUPdateResponse = new PCBASerialNoUPdateResponse();

		boolean isMissing=false;
		boolean isValidSerialNoIn=false;
		boolean isValidSerialNoOut=false;
		boolean isValidDualSerialNo=false;
		boolean isValidTriSerialNo=false;
		boolean isValidSerial=false;
		boolean isValidSerialIn=false;
		boolean isValidDualSerialIn=false;
		boolean isValidTriSerialIn=false;

		String updConfig = null;
		try {
			updConfig = DBUtil.dbConfigCheck();
		}catch (NamingException e) {

			pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG + e);
		} catch (SQLException e) {
			pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.NO_DATASOURCE_FOUND);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG + e);
		}
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

		//check if sn type is valid
		isValidSerial=validateSNType(pCBASerialNoUPdateQueryInput);

		if(!isValidSerial){
			pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.INVALID_SN_TYPE);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.INVALID_SN_TYPE_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
		}

		//Check Valid serialNoIn
		if(pCBASerialNoUPdateQueryInput.getSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getSerialNoIn().equals(""))){

			//String statusOfSerialNoIn = DBUtil.checkValidSerialNumber(pCBASerialNoUPdateQueryInput.getSerialNoIn(),"SerialNoIn");
			String statusOfSerialNoIn = null;
			try {
				statusOfSerialNoIn = DBUtil.checkValidSerialNumber(pCBASerialNoUPdateQueryInput.getSerialNoIn(),"SerialNoIn");
			} catch (MEIDException e) {
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.INVALID_SN_TYPE);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.INVALID_SN_TYPE_MSG);
			}
			if(statusOfSerialNoIn.length() == 15){
				pCBASerialNoUPdateQueryInput.setSerialNoIn(statusOfSerialNoIn);
			}else{
				pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(statusOfSerialNoIn);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

			}
		}
		//Check Valid serialNoOut

		if(pCBASerialNoUPdateQueryInput.getSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getSerialNoOut().equals(""))){
			String statusOfSerialNoOut = DBUtil.checkValidSerialNumber(pCBASerialNoUPdateQueryInput.getSerialNoOut(),"SerialNoOut");
			if(statusOfSerialNoOut.length() == 15){
				pCBASerialNoUPdateQueryInput.setSerialNoOut(statusOfSerialNoOut);
			}else{
				pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(statusOfSerialNoOut);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

			}
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
		//Check SerialIn and SerialOut different
		isValidSerialIn=validateNormalSerialNoIn(pCBASerialNoUPdateQueryInput);
		if(!isValidSerialIn){
			pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.SERIAL_IN_OUT_DIFF);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.SERIAL_IN_OUT_DIFF_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

		}
		//Check SerialIn,SerialOut,DualSerialNoIn and DualSerialNoOut are Different
		isValidDualSerialIn = validateDualSerialNoIn(pCBASerialNoUPdateQueryInput);
		if(!isValidDualSerialIn){
			pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.DUAL_SERIAL_IN_OUT_DIFF);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.DUAL_SERIAL_IN_OUT_DIFF_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

		}
		// Check SerialIn,SerialOut,DualSerialNoIn,DualSerialNoOut,TriSerialNoIn and TriSerialNoOut are Different
		isValidTriSerialIn = validateTriSerialNoIn(pCBASerialNoUPdateQueryInput);
		if(!isValidTriSerialIn){
			pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.TRI_SERIAL_IN_OUT_DIFF);
			pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.TRI_SERIAL_IN_OUT_DIFF_MSG);
			return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

		}
		//Check Valid DualSerialNoIn
		if(pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals(""))){
			String statusOfDualSerialNoIn = DBUtil.checkValidSerialNumber(pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),"DualSerialNoIn");
			if(statusOfDualSerialNoIn.length() == 15){
				pCBASerialNoUPdateQueryInput.setDualSerialNoIn(statusOfDualSerialNoIn);
			}else{
				pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(statusOfDualSerialNoIn);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

			}
		}


		// check if DualSerialNo is valid

		if((pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals(""))) || (pCBASerialNoUPdateQueryInput.getDualSerialNoOut()!=null  && !(pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals(""))) || (pCBASerialNoUPdateQueryInput.getDualSerialNoType()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoType().equals("")))){


			isValidDualSerialNo = validateDualSerialNo(pCBASerialNoUPdateQueryInput);
			if(isValidDualSerialNo){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.DUAL_SERIAL_NOT_FOUND);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.DUAL_SERIAL_NOT_FOUND_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}
		}

		if(pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null &&!(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals(""))){
			int dualSerialCount = pcbaSwapUPDUpdateInterfaceDAO.checkValidSerialNoIn(pCBASerialNoUPdateQueryInput.getDualSerialNoIn());
			if(dualSerialCount!=2){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.DUAL_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.DUAL_SERIAL_NO_CODE_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}

		}

		//Check Valid TriSerialNoIn
		if(pCBASerialNoUPdateQueryInput.getTriSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals(""))){
			String statusOfTriSerialNoIn = DBUtil.checkValidSerialNumber(pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),"TriSerialNoIn");
			if(statusOfTriSerialNoIn.length() == 15){
				pCBASerialNoUPdateQueryInput.setTriSerialNoIn(statusOfTriSerialNoIn);
			}else{
				pcbaSerialNoUPdateResponse.setResponseCode(ServiceMessageCodes.INVALID_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(statusOfTriSerialNoIn);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();

			}
		}

		// check if TriSerialNo is valid

		if((pCBASerialNoUPdateQueryInput.getTriSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals(""))) || (pCBASerialNoUPdateQueryInput.getTriSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoOut().equals(""))) || 
				pCBASerialNoUPdateQueryInput.getTriSerialNoType()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoType().equals(""))){
			isValidTriSerialNo=validateTriSerialNo(pCBASerialNoUPdateQueryInput);
			if(isValidTriSerialNo){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.TRI_SERIAL_NOT_FOUND);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.TRI_SERIAL_NOT_FOUND_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}

		}


		if(pCBASerialNoUPdateQueryInput.getTriSerialNoIn()!=null &&!(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals(""))){
			int triSerialCount = pcbaSwapUPDUpdateInterfaceDAO.checkValidSerialNoIn(pCBASerialNoUPdateQueryInput.getTriSerialNoIn());
			if(triSerialCount!=3){
				pcbaSerialNoUPdateResponse.setResponseCode(""+ServiceMessageCodes.TRI_SERIAL_NO_CODE);
				pcbaSerialNoUPdateResponse.setResponseMessage(ServiceMessageCodes.TRI_SERIAL_NO_CODE_MSG);
				return Response.status(200).entity(pcbaSerialNoUPdateResponse).build();
			}			
		}


		PCBASerialNoUPdateResponse response = pcbaSwapUPDUpdateInterfaceDAO.serialNumberInfo(pCBASerialNoUPdateQueryInput);


		return Response.status(200).entity(response).build();

	}



	private boolean validateNormalSerialNoIn(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if((pCBASerialNoUPdateQueryInput.getSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getSerialNoIn().equals(""))) && 
				(pCBASerialNoUPdateQueryInput.getSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getSerialNoOut().equals("")))){

			if(!(pCBASerialNoUPdateQueryInput.getSerialNoIn().equals(pCBASerialNoUPdateQueryInput.getSerialNoOut()))){
				return true;
			}					
		}
		return false;
	}

	private boolean validateDualSerialNoIn(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {

		if((pCBASerialNoUPdateQueryInput.getDualSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().equals(""))) && 
				(pCBASerialNoUPdateQueryInput.getDualSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getDualSerialNoOut().equals("")))){

			Set<String > set = new HashSet<String>();

			set.add(pCBASerialNoUPdateQueryInput.getSerialNoIn());
			set.add(pCBASerialNoUPdateQueryInput.getSerialNoOut());
			set.add(pCBASerialNoUPdateQueryInput.getDualSerialNoIn());
			set.add(pCBASerialNoUPdateQueryInput.getDualSerialNoOut());
			if(set.size()==4){
				return true;
			}else{
				return false;
			}
		}
		return true;
	}

	private boolean validateTriSerialNoIn(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {

		if((pCBASerialNoUPdateQueryInput.getTriSerialNoIn()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().equals(""))) && 
				(pCBASerialNoUPdateQueryInput.getTriSerialNoOut()!=null && !(pCBASerialNoUPdateQueryInput.getTriSerialNoOut().equals("")))){

			Set<String > set = new HashSet<String>();

			set.add(pCBASerialNoUPdateQueryInput.getSerialNoIn());
			set.add(pCBASerialNoUPdateQueryInput.getSerialNoOut());
			set.add(pCBASerialNoUPdateQueryInput.getDualSerialNoIn());
			set.add(pCBASerialNoUPdateQueryInput.getDualSerialNoOut());
			set.add(pCBASerialNoUPdateQueryInput.getTriSerialNoIn());
			set.add(pCBASerialNoUPdateQueryInput.getTriSerialNoOut());
			if(set.size()==6){
				return true;
			}else{
				return false;
			}
		}
		return true;
	}	

	// TODO Auto-generated method stub



	private boolean validateSNType(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		// TODO Auto-generated method stub
		if(pCBASerialNoUPdateQueryInput.getSerialNoType().trim().equals(PCBADataDictionary.IMEI) || pCBASerialNoUPdateQueryInput.getSerialNoType().trim().equals(PCBADataDictionary.MEID)){
			return true;
		}
		return false;
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
