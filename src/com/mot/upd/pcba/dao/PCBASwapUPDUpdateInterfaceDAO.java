/**
 * 
 */
package com.mot.upd.pcba.dao;

import com.mot.upd.pcba.pojo.PCBASerialNoUPdateQueryInput;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateResponse;

/**
 * @author rviswa
 *
 */
public interface PCBASwapUPDUpdateInterfaceDAO {
	

	PCBASerialNoUPdateResponse serialNumberInfo(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput);

	int checkValidSerialNoIn(String SerialNoIn);

	
	
	

}
