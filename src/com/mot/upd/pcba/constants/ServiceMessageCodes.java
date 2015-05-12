package com.mot.upd.pcba.constants;

public class ServiceMessageCodes {

	public ServiceMessageCodes() {

	}

	// Thammaiah added
	// Common codes(Application Exception)
	public static final String NO_DATASOURCE_FOUND = "8001";
	public static final String SQL_EXCEPTION = "8001";

	// Common Messages
	public static final String NO_DATASOURCE_FOUND_DISPATCH_SERIAL_MSG = "Error:Please Contact Support Team    Error Details:";
	public static final String SQL_EXCEPTION_MSG = "Error:Please Contact Support Team    Error Details:";
	// Thammaiah added
	// Codes for dispatch serial no

	public static final String SUCCESS = "0000";
	public static final String NO_NEW_SERIAL_NO_AVAILABLE = "5001";
	public static final String INVALID_REQUEST_TYPE = "5002";
	public static final String INVALID_BUILD_TYPE = "5003";
	public static final String SERIAL_NO_NOT_VALID = "5009";
	public static final String INPUT_PARAM_MISSING = "5005";
	public static final String NO_ULMA_AVAILABLE = "5006";
	public static final String NEW_SERIAL_NO_AVAILABLE = "5007";
	public static final String NEW_ULMA_AVAILABLE = "5008";
	public static final String INVALID_SN_TYPE = "5015";
	public static final String NO_PROTOCOL_FOUND = "5022";
	public static final String ULMA_ADDRESS_GREATER_THAN_FIVE = "5017";
	public static final String INVALID_GPPID = "5018";
	public static final String INVALID_CUSTOMER = "5022";
	public static final int SN_15_DIGIT = 15;
	public static final int SN_14_DIGIT = 14;

	// Viswanath added

	// public static final int MEID_SUCCESS = 5009;
	public static final String MEID_FAILURE = "5010";
	public static final String INVALID_STATUS = "5011";
	public static final String NO_LOCK_CODE_FOUND = "5012";
	public static final String OLD_SERIAL_NO_NOT_FOUND = "5013";
	public static final String NEW_SERIAL_NO_NOT_FOUND = "5014";
	// public static final int IMEI_SUCCESS = 5016;
	public static final String IMEI_FAILURE = "5017";
	public static final String OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE = "5018";
	public static final String OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE = "5019";
	public static final String DUAL_SERIAL_NOT_FOUND = "5020";
	public static final String TRI_SERIAL_NOT_FOUND = "5021";

	// Thammaiah added
	// Messages for dispatch serial no WS
	public static final String OPERATION_SUCCESS = "Successfully dispatched serial number";
	public static final String NO_SERIAL_NO_AVAILABLE_FOR_DISPATCH_MSG = "No New Serial number available for Dispatch";
	public static final String INVALID_REQUEST_TYPE_MSG = "Request type Value must be V or D";
	public static final String SERIAL_NO_NOT_VALID_MSG = "Serial number not valid";
	public static final String INPUT_PARAM_MISSING_MSG = "Following Fields are mandatory- Build type,Request-type,SerialNumber Request-type(MEID/IMEI),RSD id,MASC ID,Number of ULMA,Track ID.Please re-enter and try again.";
	public static final String NO_ULMA_AVAILABLE_MSG = "No ULMA Available For Dispatch";
	public static final String NEW_ULMA_AVAILABLE_MSG = "New ULMA Available For Dispatch";
	public static final String SERIAL_NO_AVAILABLE_FOR_DISPATCH_MSG = "New Serial number available for Dispatch";
	public static final String INVALID_SN_TYPE_MSG = "Serial number type should be either MEID or IMEI";
	public static final String NO_PROTOCOL_FOUND_MSG = "Protocol Name is mandatory for MEID";
	public static final String ULMA_ADDRESS_GREATER_THAN_FIVE_MSG = "Ulma address requested is greater than five";
	public static final String INVALID_GPPID_MSG = "Invalid GPPID";
	public static final String INVALID_BUILD_TYPE_MSG = "Build type Value must be PROD/PROTO";
	public static final String INVALID_CUSTOMER_MSG = "Invalid Customer";

	// Viswanath added
	
	public static final String MEID_SUCCES_MSG = "Success";
	public static final String MEID_FAILURE_MSG = "pcba pgm failure";
	public static final String INVALID_STATUS_MSG = "Status should be either S or F";
	public static final String NO_LOCK_CODE_DETAILS_FOUND_MSG = "No Lock Code Details Found";
	public static final String OLD_SERIAL_NO_NOT_FOUND_MSG = "Old Serial Not Found";
	public static final String NEW_SERIAL_NO_NOT_FOUND_MSG = "New Serial Not Found";
	public static final String NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG = "No DataSource found for SerialNO.";
	public static final String IMEI_SUCCES_MSG = "Success";
	public static final String IMEI_FAILURE_MSG = "pcba pgm failure";
	public static final String PCBA_INPUT_PARAM_MISSING_MSG = "Following Fields are mandatory- Serial Number,rsdID,mascID,status,snType and either one is mandatory msl,otksl and servicePassCode.Please re-enter and try again.";
	public static final String PCBA_INPUT_PARAM_MISSING = "Following Fields are mandatory- ClientIP,mascID,serialNoIn,serialNoOut,serialNoType,repairdate.Please re-enter and try again.";
	public static final String OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE_MSG = "Old Serialno not found in Shipment table";
	public static final String OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE_MSG = "Old Serialno not found in warranty info table";
	public static final String DUAL_SERIAL_NOT_FOUND_MSG = "Required both DualSerialNoIn and DualSerialNoOut.";
	public static final String TRI_SERIAL_NOT_FOUND_MSG = "Required TriSerialNoIn,TriSerialNoOut,DualSerialNoIn and DualSerialNoOut.";
	public static final String EMAIL_MSG_CODE = "8002";
	public static final String EMAIL_MSG = "Error - Contact Support Team.";
	public static final String DUAL_SERIAL_NO_CODE = "5022";
	public static final String DUAL_SERIAL_NO_CODE_MSG = "This DualSerialNoIn is not eligible for Dual Case.";
	public static final String TRI_SERIAL_NO_CODE = "5023";
	public static final String TRI_SERIAL_NO_CODE_MSG = "This TriSerialNoIn is not eligible for Tri Case.";
	public static final String INVALID="invalid ";
	public static final String INVALID_SERIAL_NO_CODE="5024";
	// R12 scrap serial
	public static final String OLD_SERIAL_FOUND_SUCCSS_MSG = "Success";
	public static final String OLD_SN_SUCCESS = "0000";
	public static final String R12_OLD_SN_NOT_AVAILABLE = "5015";
	public static final String R12_SN_NOT_VALID = "5016";
	public static final String R12_OLD_SN_NOT_VALID = "5025";
	public static final String R12_OLD_SN_NOT_VALID_MSG = "OLD SerialNumber not valid";
	
	//public static final String JNDI_NOT_FOUND = "8003";
	//public static final String JNDI_NOT_FOUND_MSG = "JNDI IS NOT AVAILABLE";

}
