/**
 * 
 */
package com.mot.upd.pcba.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.text.SimpleDateFormat;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mot.upd.pcba.constants.ServiceMessageCodes;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateQueryInput;
import com.mot.upd.pcba.pojo.PCBASerialNoUPdateResponse;
import com.mot.upd.pcba.utils.DBUtil;
import com.mot.upd.pcba.utils.MailUtil;


/**
 * @author rviswa
 * 
 */
public class PCBASwapUPDUpdateOracleDAO implements
PCBASwapUPDUpdateInterfaceDAO {
	private static Logger logger = Logger
			.getLogger(PCBASwapUPDUpdateOracleDAO.class);

	private DataSource ds;
	private Connection con = null;
	private Connection connection = null;
	private PreparedStatement preparedStmt = null;
	private PreparedStatement pstmt = null;
	private PreparedStatement pstmt1 = null;
	private PreparedStatement prestmt = null;
	private ResultSet rs = null;

	PCBASerialNoUPdateResponse response = new PCBASerialNoUPdateResponse();

	StringBuffer SQLQuery = new StringBuffer();

	public PCBASerialNoUPdateResponse serialNumberInfo(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in MEID:" + e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			StringBuffer sb = new StringBuffer();

			sb.append("select  SERIAL_NO, REQUEST_ID, REGION_ID, SYSTEM_ID, ATTRIBUTE_01, ATTRIBUTE_02, ATTRIBUTE_03, ATTRIBUTE_04, ATTRIBUTE_05,ATTRIBUTE_06,  ATTRIBUTE_07,  ATTRIBUTE_08,");
			sb.append("ATTRIBUTE_09, ATTRIBUTE_10,   ATTRIBUTE_11,  ATTRIBUTE_12,  ATTRIBUTE_13,  ATTRIBUTE_14, ATTRIBUTE_15,  ATTRIBUTE_16,  ATTRIBUTE_17,  ATTRIBUTE_18,  ATTRIBUTE_19,");
			sb.append("ATTRIBUTE_20,  ATTRIBUTE_21,  ATTRIBUTE_22,  ATTRIBUTE_23,  ATTRIBUTE_24,  ATTRIBUTE_34, ATTRIBUTE_35,  ATTRIBUTE_37,  ATTRIBUTE_38,  ATTRIBUTE_39,  ATTRIBUTE_40,");
			sb.append("ATTRIBUTE_41,  ATTRIBUTE_42,  ATTRIBUTE_43,  ATTRIBUTE_44,  ATTRIBUTE_45,  ATTRIBUTE_46, ATTRIBUTE_47,  ATTRIBUTE_48,  ATTRIBUTE_49,  ATTRIBUTE_50,  ATTRIBUTE_51,");
			sb.append("ATTRIBUTE_52,  ATTRIBUTE_53,  ATTRIBUTE_54,  ATTRIBUTE_55,  ATTRIBUTE_56,  ATTRIBUTE_57, ATTRIBUTE_58,  ATTRIBUTE_59,  ATTRIBUTE_60,  ATTRIBUTE_61,  ATTRIBUTE_62,");
			sb.append("ATTRIBUTE_63,  ATTRIBUTE_64,  ATTRIBUTE_65,  ATTRIBUTE_66,  ATTRIBUTE_67,  ATTRIBUTE_68, ATTRIBUTE_69,  ATTRIBUTE_70,  ATTRIBUTE_71,  ATTRIBUTE_72,  ATTRIBUTE_73,");
			sb.append("ATTRIBUTE_74,  ATTRIBUTE_75,  ATTRIBUTE_76,  ATTRIBUTE_77,  ATTRIBUTE_78,  ATTRIBUTE_79, ATTRIBUTE_80,  ATTRIBUTE_81,  ATTRIBUTE_82,  ATTRIBUTE_84,  ATTRIBUTE_85,");
			sb.append("ATTRIBUTE_86,  ATTRIBUTE_87,  ATTRIBUTE_88,  ATTRIBUTE_89,  ATTRIBUTE_90,  ATTRIBUTE_91, ATTRIBUTE_92,  ATTRIBUTE_93,  ATTRIBUTE_94,  ATTRIBUTE_95,  ATTRIBUTE_96,");
			sb.append("ATTRIBUTE_97,  ATTRIBUTE_98,  ATTRIBUTE_99,  ATTRIBUTE_100, ATTRIBUTE_101, ATTRIBUTE_105,ATTRIBUTE_106, ATTRIBUTE_107, ATTRIBUTE_108, ATTRIBUTE_109, ATTRIBUTE_110,");
			sb.append("ATTRIBUTE_111, ATTRIBUTE_112, ATTRIBUTE_113, ATTRIBUTE_117, ATTRIBUTE_118, ATTRIBUTE_114,ATTRIBUTE_115, ATTRIBUTE_116, ATTRIBUTE_119, ATTRIBUTE_120, ATTRIBUTE_121,");
			sb.append("ATTRIBUTE_122, ATTRIBUTE_123 from UPD_SN_REPOS  where serial_no=?");

			preparedStmt = con.prepareStatement(sb.toString());
			preparedStmt.setString(1,
					pCBASerialNoUPdateQueryInput.getSerialNoIn());
			rs = preparedStmt.executeQuery();

			if (rs.next()) {

				connection = DBUtil.getConnection(ds);
				connection.setAutoCommit(false);


				String serialNoOfstatus=rs.getString("ATTRIBUTE_37");
				if(serialNoOfstatus.startsWith("VOI")){

					StringBuffer stb = new StringBuffer();
					stb.append("insert into shipment_notavail_sn(SERIAL_NO_IN,SERIAL_NO_OUT,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,STATUS) values(?,?,?,?,?,?,?)");
					prestmt = connection.prepareStatement(stb.toString());
					prestmt.setString(1,
							pCBASerialNoUPdateQueryInput.getSerialNoIn());
					prestmt.setString(2,
							pCBASerialNoUPdateQueryInput.getSerialNoOut());
					prestmt.setString(3, "PCBA_PGM");
					prestmt.setDate(4,
							new java.sql.Date(System.currentTimeMillis()));
					prestmt.setString(5, "PCBA_PGM");
					prestmt.setDate(6,
							new java.sql.Date(System.currentTimeMillis()));
					prestmt.setString(7, "S");
					prestmt.execute();

					MailUtil.sendEmail(pCBASerialNoUPdateQueryInput.getSerialNoIn(),pCBASerialNoUPdateQueryInput.getSerialNoOut());

					response.setResponseCode(ServiceMessageCodes.EMAIL_MSG_CODE);
					response.setResponseMessage(ServiceMessageCodes.EMAIL_MSG);
					//return response;
				}else{

					SQLQuery.append("insert into UPD_SN_REPOS(SERIAL_NO, REQUEST_ID, REGION_ID, SYSTEM_ID, ATTRIBUTE_01, ATTRIBUTE_02, ATTRIBUTE_03, ATTRIBUTE_04, ATTRIBUTE_05,ATTRIBUTE_06,  ATTRIBUTE_07,  ATTRIBUTE_08,");
					SQLQuery.append("ATTRIBUTE_09, ATTRIBUTE_10,   ATTRIBUTE_11,  ATTRIBUTE_12,  ATTRIBUTE_13,  ATTRIBUTE_14, ATTRIBUTE_15,  ATTRIBUTE_16,  ATTRIBUTE_17,  ATTRIBUTE_18,  ATTRIBUTE_19,");
					SQLQuery.append("ATTRIBUTE_20,  ATTRIBUTE_21,  ATTRIBUTE_22,  ATTRIBUTE_23,  ATTRIBUTE_24,  ATTRIBUTE_34, ATTRIBUTE_35,  ATTRIBUTE_37,  ATTRIBUTE_38,  ATTRIBUTE_39,  ATTRIBUTE_40,");
					SQLQuery.append("ATTRIBUTE_41,  ATTRIBUTE_42,  ATTRIBUTE_43,  ATTRIBUTE_44,  ATTRIBUTE_45,  ATTRIBUTE_46, ATTRIBUTE_47,  ATTRIBUTE_48,  ATTRIBUTE_49,  ATTRIBUTE_50,  ATTRIBUTE_51,");
					SQLQuery.append("ATTRIBUTE_52,  ATTRIBUTE_53,  ATTRIBUTE_54,  ATTRIBUTE_55,  ATTRIBUTE_56,  ATTRIBUTE_57, ATTRIBUTE_58,  ATTRIBUTE_59,  ATTRIBUTE_60,  ATTRIBUTE_61,  ATTRIBUTE_62,");
					SQLQuery.append("ATTRIBUTE_63,  ATTRIBUTE_64,  ATTRIBUTE_65,  ATTRIBUTE_66,  ATTRIBUTE_67,  ATTRIBUTE_68, ATTRIBUTE_69,  ATTRIBUTE_70,  ATTRIBUTE_71,  ATTRIBUTE_72,  ATTRIBUTE_73,");
					SQLQuery.append("ATTRIBUTE_74,  ATTRIBUTE_75,  ATTRIBUTE_76,  ATTRIBUTE_77,  ATTRIBUTE_78,  ATTRIBUTE_79, ATTRIBUTE_80,  ATTRIBUTE_81,  ATTRIBUTE_82,  ATTRIBUTE_84,  ATTRIBUTE_85,");
					SQLQuery.append("ATTRIBUTE_86,  ATTRIBUTE_87,  ATTRIBUTE_88,  ATTRIBUTE_89,  ATTRIBUTE_90,  ATTRIBUTE_91, ATTRIBUTE_92,  ATTRIBUTE_93,  ATTRIBUTE_94,  ATTRIBUTE_95,  ATTRIBUTE_96,");
					SQLQuery.append("ATTRIBUTE_97,  ATTRIBUTE_98,  ATTRIBUTE_99,  ATTRIBUTE_100, ATTRIBUTE_101, ATTRIBUTE_105,ATTRIBUTE_106, ATTRIBUTE_107, ATTRIBUTE_108, ATTRIBUTE_109, ATTRIBUTE_110,");
					SQLQuery.append("ATTRIBUTE_111, ATTRIBUTE_112, ATTRIBUTE_113, ATTRIBUTE_117, ATTRIBUTE_118, ATTRIBUTE_114,ATTRIBUTE_115, ATTRIBUTE_116, ATTRIBUTE_119, ATTRIBUTE_120, ATTRIBUTE_121,");
					SQLQuery.append("ATTRIBUTE_122, ATTRIBUTE_123) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

					pstmt = connection.prepareStatement(SQLQuery.toString());
					pstmt.setString(1,
							pCBASerialNoUPdateQueryInput.getSerialNoOut());
					pstmt.setString(2, rs.getString("REQUEST_ID"));
					pstmt.setString(3, rs.getString("REGION_ID"));
					pstmt.setString(4, rs.getString("SYSTEM_ID"));
					pstmt.setString(5, rs.getString("ATTRIBUTE_01"));
					pstmt.setDate(6, rs.getDate("ATTRIBUTE_02"));
					pstmt.setString(7, rs.getString("ATTRIBUTE_03"));
					pstmt.setString(8, rs.getString("ATTRIBUTE_04"));
					pstmt.setString(9, rs.getString("ATTRIBUTE_05"));
					pstmt.setString(10, rs.getString("ATTRIBUTE_06"));
					pstmt.setString(11, rs.getString("ATTRIBUTE_07"));
					pstmt.setString(12, rs.getString("ATTRIBUTE_08"));
					pstmt.setString(13, rs.getString("ATTRIBUTE_09"));
					pstmt.setDate(14, rs.getDate("ATTRIBUTE_10"));
					pstmt.setString(15, rs.getString("ATTRIBUTE_11"));
					pstmt.setString(16, rs.getString("ATTRIBUTE_12"));
					pstmt.setString(17, rs.getString("ATTRIBUTE_13"));
					pstmt.setString(18, rs.getString("ATTRIBUTE_14"));
					pstmt.setString(19, rs.getString("ATTRIBUTE_15"));
					pstmt.setString(20, rs.getString("ATTRIBUTE_16"));
					pstmt.setString(21, rs.getString("ATTRIBUTE_17"));
					pstmt.setDate(22, rs.getDate("ATTRIBUTE_18"));
					pstmt.setString(23, rs.getString("ATTRIBUTE_19"));
					pstmt.setString(24, rs.getString("ATTRIBUTE_20"));
					pstmt.setString(25, rs.getString("ATTRIBUTE_21"));
					pstmt.setString(26, rs.getString("ATTRIBUTE_22"));
					pstmt.setString(27, rs.getString("ATTRIBUTE_23"));
					pstmt.setString(28, rs.getString("ATTRIBUTE_24"));
					pstmt.setString(29, rs.getString("ATTRIBUTE_34"));
					pstmt.setString(30, rs.getString("ATTRIBUTE_35"));

					Date curDate = new Date();
					SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyy");
					String DateToStr = format.format(curDate);

					pstmt.setString(31, "ACT  " + DateToStr);// Status
					pstmt.setString(32, rs.getString("ATTRIBUTE_38"));
					pstmt.setString(33, rs.getString("ATTRIBUTE_39"));
					pstmt.setString(34, rs.getString("ATTRIBUTE_40"));
					pstmt.setString(35, rs.getString("ATTRIBUTE_41"));
					pstmt.setString(36, rs.getString("ATTRIBUTE_42"));
					pstmt.setString(37, rs.getString("ATTRIBUTE_43"));
					pstmt.setString(38, rs.getString("ATTRIBUTE_44"));
					pstmt.setDate(39, rs.getDate("ATTRIBUTE_45"));
					pstmt.setString(40, rs.getString("ATTRIBUTE_46"));
					pstmt.setString(41, rs.getString("ATTRIBUTE_47"));
					pstmt.setString(42, rs.getString("ATTRIBUTE_48"));
					pstmt.setDate(43, rs.getDate("ATTRIBUTE_49"));
					pstmt.setDate(44, rs.getDate("ATTRIBUTE_50"));
					pstmt.setDate(45, rs.getDate("ATTRIBUTE_51"));
					pstmt.setDate(46, rs.getDate("ATTRIBUTE_52"));
					pstmt.setDate(47, rs.getDate("ATTRIBUTE_53"));
					pstmt.setLong(48, rs.getLong("ATTRIBUTE_54")); // Number
					pstmt.setLong(49, rs.getLong("ATTRIBUTE_55")); // Number
					pstmt.setLong(50, rs.getLong("ATTRIBUTE_56")); // Number
					pstmt.setLong(51, rs.getLong("ATTRIBUTE_57"));// Number
					pstmt.setString(52, rs.getString("ATTRIBUTE_58"));
					pstmt.setString(53, rs.getString("ATTRIBUTE_59"));
					pstmt.setString(54, rs.getString("ATTRIBUTE_60"));
					pstmt.setString(55, rs.getString("ATTRIBUTE_61"));
					pstmt.setString(56, rs.getString("ATTRIBUTE_62"));
					pstmt.setString(57, rs.getString("ATTRIBUTE_63"));
					pstmt.setString(58, rs.getString("ATTRIBUTE_64"));
					pstmt.setString(59, rs.getString("ATTRIBUTE_65"));
					pstmt.setString(60, rs.getString("ATTRIBUTE_66"));
					pstmt.setString(61, rs.getString("ATTRIBUTE_67"));
					pstmt.setString(62, rs.getString("ATTRIBUTE_68"));
					pstmt.setString(63, rs.getString("ATTRIBUTE_69"));
					pstmt.setString(64, rs.getString("ATTRIBUTE_70"));
					pstmt.setString(65, rs.getString("ATTRIBUTE_71"));
					pstmt.setString(66, rs.getString("ATTRIBUTE_72"));
					pstmt.setString(67, rs.getString("ATTRIBUTE_73"));
					pstmt.setString(68, rs.getString("ATTRIBUTE_74"));
					pstmt.setString(69, rs.getString("ATTRIBUTE_75"));
					pstmt.setString(70, rs.getString("ATTRIBUTE_76"));
					pstmt.setString(71, rs.getString("ATTRIBUTE_77"));
					pstmt.setString(72, rs.getString("ATTRIBUTE_78"));
					pstmt.setDate(73, rs.getDate("ATTRIBUTE_79"));
					pstmt.setString(74, rs.getString("ATTRIBUTE_80"));
					pstmt.setDate(75, rs.getDate("ATTRIBUTE_81"));
					pstmt.setString(76, rs.getString("ATTRIBUTE_82"));
					pstmt.setString(77, rs.getString("ATTRIBUTE_84"));
					pstmt.setString(78, rs.getString("ATTRIBUTE_85"));
					pstmt.setString(79, rs.getString("ATTRIBUTE_86"));
					pstmt.setString(80, rs.getString("ATTRIBUTE_87"));
					pstmt.setString(81, rs.getString("ATTRIBUTE_88"));
					pstmt.setString(82, rs.getString("ATTRIBUTE_89"));
					pstmt.setString(83, rs.getString("ATTRIBUTE_90"));
					pstmt.setString(84, rs.getString("ATTRIBUTE_91"));
					pstmt.setString(85, rs.getString("ATTRIBUTE_92"));
					pstmt.setString(86, rs.getString("ATTRIBUTE_93"));
					pstmt.setString(87, rs.getString("ATTRIBUTE_94"));
					pstmt.setString(88, rs.getString("ATTRIBUTE_95"));
					pstmt.setString(89, rs.getString("ATTRIBUTE_96"));
					pstmt.setString(90, rs.getString("ATTRIBUTE_97"));
					pstmt.setString(91, rs.getString("ATTRIBUTE_98"));
					pstmt.setString(92, rs.getString("ATTRIBUTE_99"));
					pstmt.setString(93, rs.getString("ATTRIBUTE_100"));
					pstmt.setString(94, rs.getString("ATTRIBUTE_101"));
					pstmt.setString(95, rs.getString("ATTRIBUTE_105"));
					pstmt.setString(96, rs.getString("ATTRIBUTE_106"));
					pstmt.setString(97, rs.getString("ATTRIBUTE_107"));
					pstmt.setString(98, rs.getString("ATTRIBUTE_108"));
					pstmt.setString(99, rs.getString("ATTRIBUTE_109"));
					pstmt.setString(100, rs.getString("ATTRIBUTE_110"));
					pstmt.setString(101, rs.getString("ATTRIBUTE_111"));
					pstmt.setString(102, rs.getString("ATTRIBUTE_112"));
					pstmt.setString(103, rs.getString("ATTRIBUTE_113"));
					pstmt.setDate(104, rs.getDate("ATTRIBUTE_117"));
					pstmt.setLong(105, rs.getLong("ATTRIBUTE_118"));// Number
					pstmt.setDate(106, rs.getDate("ATTRIBUTE_114"));
					pstmt.setDate(107, rs.getDate("ATTRIBUTE_115"));
					pstmt.setString(108, rs.getString("ATTRIBUTE_116"));
					pstmt.setString(109, rs.getString("ATTRIBUTE_119"));
					pstmt.setString(110, rs.getString("ATTRIBUTE_120"));
					pstmt.setString(111, rs.getString("ATTRIBUTE_121"));
					pstmt.setString(112, rs.getString("ATTRIBUTE_122"));
					pstmt.setString(113, rs.getString("ATTRIBUTE_123"));

					boolean status = pstmt.execute();

					if (!status) {

						String updateOldserialNOStatus = "update UPD_SN_REPOS set ATTRIBUTE_37='SCR  "
								+ DateToStr
								+ "',ATTRIBUTE_41='"+pCBASerialNoUPdateQueryInput.getSerialNoOut()+"' where serial_no='"
								+ pCBASerialNoUPdateQueryInput.getSerialNoIn()
								+ "'";
						pstmt1 = connection
								.prepareStatement(updateOldserialNOStatus);
						pstmt1.execute();

					}

					response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
					response.setResponseMessage(ServiceMessageCodes.READING_OLD_SERIAL_NO_INTO_NEW_SERIAL_NO);
				}

			} else {

				response.setResponseCode(""+ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE);
				response.setResponseMessage(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE_MSG);
				return response;

			}

			// Above its normal case

			// Dual Case

			if (pCBASerialNoUPdateQueryInput.getDualSerialNoIn() != null)

			{
				if (!(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().trim() == "")) {
					connection = updateReferenceTable(
							pCBASerialNoUPdateQueryInput.getSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getSerialNoOut(),
							connection);

					connection = updateBasedOnSerial(
							pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getDualSerialNoOut(),
							ds, connection);

					connection = updateReferenceTable(
							pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getDualSerialNoOut(),
							connection);
				}
			}

			// Tri Case
			if (pCBASerialNoUPdateQueryInput.getTriSerialNoIn() != null) {
				if (!(pCBASerialNoUPdateQueryInput.getTriSerialNoIn() == "")) {
					connection = updateBasedOnSerial(
							pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getTriSerialNoOut(),
							ds, connection);
					connection = updateReferenceTable(
							pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getTriSerialNoOut(),
							connection);
				}
			}

			// dualConnection.commit();
			connection.commit();
			connection.close();

		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
			logger.error(e.getMessage());
			response.setResponseCode(""+ServiceMessageCodes.SQL_EXCEPTION);
			response.setResponseMessage(ServiceMessageCodes.SQL_EXCEPTION_MSG
					+ e.getMessage());
		} finally {
			DBUtil.closeConnection(con, preparedStmt, rs);
			DBUtil.connectionClosed(connection, pstmt);			
		}

		return response;
	}

	// Dual Case and
	// Tri Case
	public Connection updateBasedOnSerial(String serialNoIn,
			String serialNoOut, DataSource ds, Connection connection2)
					throws Exception {

		Connection innerselectcon = null;
		PreparedStatement pst = null;
		PreparedStatement pstUpdate = null;
		ResultSet rs = null;

		try {
			// get database connection
			innerselectcon = DBUtil.getConnection(ds);

			StringBuffer sbuffer = new StringBuffer();
			StringBuffer SQLInnerQuery = new StringBuffer();

			sbuffer.append("select  SERIAL_NO, REQUEST_ID, REGION_ID, SYSTEM_ID, ATTRIBUTE_01, ATTRIBUTE_02, ATTRIBUTE_03, ATTRIBUTE_04, ATTRIBUTE_05,ATTRIBUTE_06,  ATTRIBUTE_07,  ATTRIBUTE_08,");
			sbuffer.append("ATTRIBUTE_09, ATTRIBUTE_10,   ATTRIBUTE_11,  ATTRIBUTE_12,  ATTRIBUTE_13,  ATTRIBUTE_14, ATTRIBUTE_15,  ATTRIBUTE_16,  ATTRIBUTE_17,  ATTRIBUTE_18,  ATTRIBUTE_19,");
			sbuffer.append("ATTRIBUTE_20,  ATTRIBUTE_21,  ATTRIBUTE_22,  ATTRIBUTE_23,  ATTRIBUTE_24,  ATTRIBUTE_34, ATTRIBUTE_35,  ATTRIBUTE_37,  ATTRIBUTE_38,  ATTRIBUTE_39,  ATTRIBUTE_40,");
			sbuffer.append("ATTRIBUTE_41,  ATTRIBUTE_42,  ATTRIBUTE_43,  ATTRIBUTE_44,  ATTRIBUTE_45,  ATTRIBUTE_46, ATTRIBUTE_47,  ATTRIBUTE_48,  ATTRIBUTE_49,  ATTRIBUTE_50,  ATTRIBUTE_51,");
			sbuffer.append("ATTRIBUTE_52,  ATTRIBUTE_53,  ATTRIBUTE_54,  ATTRIBUTE_55,  ATTRIBUTE_56,  ATTRIBUTE_57, ATTRIBUTE_58,  ATTRIBUTE_59,  ATTRIBUTE_60,  ATTRIBUTE_61,  ATTRIBUTE_62,");
			sbuffer.append("ATTRIBUTE_63,  ATTRIBUTE_64,  ATTRIBUTE_65,  ATTRIBUTE_66,  ATTRIBUTE_67,  ATTRIBUTE_68, ATTRIBUTE_69,  ATTRIBUTE_70,  ATTRIBUTE_71,  ATTRIBUTE_72,  ATTRIBUTE_73,");
			sbuffer.append("ATTRIBUTE_74,  ATTRIBUTE_75,  ATTRIBUTE_76,  ATTRIBUTE_77,  ATTRIBUTE_78,  ATTRIBUTE_79, ATTRIBUTE_80,  ATTRIBUTE_81,  ATTRIBUTE_82,  ATTRIBUTE_84,  ATTRIBUTE_85,");
			sbuffer.append("ATTRIBUTE_86,  ATTRIBUTE_87,  ATTRIBUTE_88,  ATTRIBUTE_89,  ATTRIBUTE_90,  ATTRIBUTE_91, ATTRIBUTE_92,  ATTRIBUTE_93,  ATTRIBUTE_94,  ATTRIBUTE_95,  ATTRIBUTE_96,");
			sbuffer.append("ATTRIBUTE_97,  ATTRIBUTE_98,  ATTRIBUTE_99,  ATTRIBUTE_100, ATTRIBUTE_101, ATTRIBUTE_105,ATTRIBUTE_106, ATTRIBUTE_107, ATTRIBUTE_108, ATTRIBUTE_109, ATTRIBUTE_110,");
			sbuffer.append("ATTRIBUTE_111, ATTRIBUTE_112, ATTRIBUTE_113, ATTRIBUTE_117, ATTRIBUTE_118, ATTRIBUTE_114,ATTRIBUTE_115, ATTRIBUTE_116, ATTRIBUTE_119, ATTRIBUTE_120, ATTRIBUTE_121,");
			sbuffer.append("ATTRIBUTE_122, ATTRIBUTE_123 from UPD_SN_REPOS  where serial_no=?");

			pst = innerselectcon.prepareStatement(sbuffer.toString());
			pst.setString(1, serialNoIn);
			rs = pst.executeQuery();

			if (rs.next()) {

				String serialNoOfstatus=rs.getString("ATTRIBUTE_37");
				if(serialNoOfstatus.startsWith("VOI")){

					StringBuffer stb = new StringBuffer();
					stb.append("insert into shipment_notavail_sn(SERIAL_NO_IN,SERIAL_NO_OUT,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,STATUS) values(?,?,?,?,?,?,?)");
					prestmt = innerselectcon.prepareStatement(stb.toString());
					prestmt.setString(1,serialNoIn);
					prestmt.setString(2,serialNoOut);
					prestmt.setString(3, "PCBA_PGM");
					prestmt.setDate(4,
							new java.sql.Date(System.currentTimeMillis()));
					prestmt.setString(5, "PCBA_PGM");
					prestmt.setDate(6,
							new java.sql.Date(System.currentTimeMillis()));
					prestmt.setString(7, "S");
					prestmt.execute();

					MailUtil.sendEmail(serialNoIn,serialNoOut);
					response.setResponseCode(ServiceMessageCodes.EMAIL_MSG_CODE);
					response.setResponseMessage(ServiceMessageCodes.EMAIL_MSG);


				}else{


					SQLInnerQuery
					.append("insert into UPD_SN_REPOS(SERIAL_NO, REQUEST_ID, REGION_ID, SYSTEM_ID, ATTRIBUTE_01, ATTRIBUTE_02, ATTRIBUTE_03, ATTRIBUTE_04, ATTRIBUTE_05,ATTRIBUTE_06,  ATTRIBUTE_07,  ATTRIBUTE_08,");
					SQLInnerQuery
					.append("ATTRIBUTE_09, ATTRIBUTE_10,   ATTRIBUTE_11,  ATTRIBUTE_12,  ATTRIBUTE_13,  ATTRIBUTE_14, ATTRIBUTE_15,  ATTRIBUTE_16,  ATTRIBUTE_17,  ATTRIBUTE_18,  ATTRIBUTE_19,");
					SQLInnerQuery
					.append("ATTRIBUTE_20,  ATTRIBUTE_21,  ATTRIBUTE_22,  ATTRIBUTE_23,  ATTRIBUTE_24,  ATTRIBUTE_34, ATTRIBUTE_35,  ATTRIBUTE_37,  ATTRIBUTE_38,  ATTRIBUTE_39,  ATTRIBUTE_40,");
					SQLInnerQuery
					.append("ATTRIBUTE_41,  ATTRIBUTE_42,  ATTRIBUTE_43,  ATTRIBUTE_44,  ATTRIBUTE_45,  ATTRIBUTE_46, ATTRIBUTE_47,  ATTRIBUTE_48,  ATTRIBUTE_49,  ATTRIBUTE_50,  ATTRIBUTE_51,");
					SQLInnerQuery
					.append("ATTRIBUTE_52,  ATTRIBUTE_53,  ATTRIBUTE_54,  ATTRIBUTE_55,  ATTRIBUTE_56,  ATTRIBUTE_57, ATTRIBUTE_58,  ATTRIBUTE_59,  ATTRIBUTE_60,  ATTRIBUTE_61,  ATTRIBUTE_62,");
					SQLInnerQuery
					.append("ATTRIBUTE_63,  ATTRIBUTE_64,  ATTRIBUTE_65,  ATTRIBUTE_66,  ATTRIBUTE_67,  ATTRIBUTE_68, ATTRIBUTE_69,  ATTRIBUTE_70,  ATTRIBUTE_71,  ATTRIBUTE_72,  ATTRIBUTE_73,");
					SQLInnerQuery
					.append("ATTRIBUTE_74,  ATTRIBUTE_75,  ATTRIBUTE_76,  ATTRIBUTE_77,  ATTRIBUTE_78,  ATTRIBUTE_79, ATTRIBUTE_80,  ATTRIBUTE_81,  ATTRIBUTE_82,  ATTRIBUTE_84,  ATTRIBUTE_85,");
					SQLInnerQuery
					.append("ATTRIBUTE_86,  ATTRIBUTE_87,  ATTRIBUTE_88,  ATTRIBUTE_89,  ATTRIBUTE_90,  ATTRIBUTE_91, ATTRIBUTE_92,  ATTRIBUTE_93,  ATTRIBUTE_94,  ATTRIBUTE_95,  ATTRIBUTE_96,");
					SQLInnerQuery
					.append("ATTRIBUTE_97,  ATTRIBUTE_98,  ATTRIBUTE_99,  ATTRIBUTE_100, ATTRIBUTE_101, ATTRIBUTE_105,ATTRIBUTE_106, ATTRIBUTE_107, ATTRIBUTE_108, ATTRIBUTE_109, ATTRIBUTE_110,");
					SQLInnerQuery
					.append("ATTRIBUTE_111, ATTRIBUTE_112, ATTRIBUTE_113, ATTRIBUTE_117, ATTRIBUTE_118, ATTRIBUTE_114,ATTRIBUTE_115, ATTRIBUTE_116, ATTRIBUTE_119, ATTRIBUTE_120, ATTRIBUTE_121,");
					SQLInnerQuery
					.append("ATTRIBUTE_122, ATTRIBUTE_123) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

					pstUpdate = connection2.prepareStatement(SQLInnerQuery
							.toString());

					pstUpdate.setString(1, serialNoOut);
					pstUpdate.setString(2, rs.getString("REQUEST_ID"));
					pstUpdate.setString(3, rs.getString("REGION_ID"));
					pstUpdate.setString(4, rs.getString("SYSTEM_ID"));
					pstUpdate.setString(5, rs.getString("ATTRIBUTE_01"));
					pstUpdate.setDate(6, rs.getDate("ATTRIBUTE_02"));
					pstUpdate.setString(7, rs.getString("ATTRIBUTE_03"));
					pstUpdate.setString(8, rs.getString("ATTRIBUTE_04"));
					pstUpdate.setString(9, rs.getString("ATTRIBUTE_05"));
					pstUpdate.setString(10, rs.getString("ATTRIBUTE_06"));
					pstUpdate.setString(11, rs.getString("ATTRIBUTE_07"));
					pstUpdate.setString(12, rs.getString("ATTRIBUTE_08"));
					pstUpdate.setString(13, rs.getString("ATTRIBUTE_09"));
					pstUpdate.setDate(14, rs.getDate("ATTRIBUTE_10"));
					pstUpdate.setString(15, rs.getString("ATTRIBUTE_11"));
					pstUpdate.setString(16, rs.getString("ATTRIBUTE_12"));
					pstUpdate.setString(17, rs.getString("ATTRIBUTE_13"));
					pstUpdate.setString(18, rs.getString("ATTRIBUTE_14"));
					pstUpdate.setString(19, rs.getString("ATTRIBUTE_15"));
					pstUpdate.setString(20, rs.getString("ATTRIBUTE_16"));
					pstUpdate.setString(21, rs.getString("ATTRIBUTE_17"));
					pstUpdate.setDate(22, rs.getDate("ATTRIBUTE_18"));
					pstUpdate.setString(23, rs.getString("ATTRIBUTE_19"));
					pstUpdate.setString(24, rs.getString("ATTRIBUTE_20"));
					pstUpdate.setString(25, rs.getString("ATTRIBUTE_21"));
					pstUpdate.setString(26, rs.getString("ATTRIBUTE_22"));
					pstUpdate.setString(27, rs.getString("ATTRIBUTE_23"));
					pstUpdate.setString(28, rs.getString("ATTRIBUTE_24"));
					pstUpdate.setString(29, rs.getString("ATTRIBUTE_34"));
					pstUpdate.setString(30, rs.getString("ATTRIBUTE_35"));

					Date curDate = new Date();
					SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyy");
					String DateToStr = format.format(curDate);

					pstUpdate.setString(31, "ACT  " + DateToStr);// Status
					pstUpdate.setString(32, rs.getString("ATTRIBUTE_38"));
					pstUpdate.setString(33, rs.getString("ATTRIBUTE_39"));
					pstUpdate.setString(34, rs.getString("ATTRIBUTE_40"));
					pstUpdate.setString(35, rs.getString("ATTRIBUTE_41"));
					pstUpdate.setString(36, rs.getString("ATTRIBUTE_42"));
					pstUpdate.setString(37, rs.getString("ATTRIBUTE_43"));
					pstUpdate.setString(38, rs.getString("ATTRIBUTE_44"));
					pstUpdate.setDate(39, rs.getDate("ATTRIBUTE_45"));
					pstUpdate.setString(40, rs.getString("ATTRIBUTE_46"));
					pstUpdate.setString(41, rs.getString("ATTRIBUTE_47"));
					pstUpdate.setString(42, rs.getString("ATTRIBUTE_48"));
					pstUpdate.setDate(43, rs.getDate("ATTRIBUTE_49"));
					pstUpdate.setDate(44, rs.getDate("ATTRIBUTE_50"));
					pstUpdate.setDate(45, rs.getDate("ATTRIBUTE_51"));
					pstUpdate.setDate(46, rs.getDate("ATTRIBUTE_52"));
					pstUpdate.setDate(47, rs.getDate("ATTRIBUTE_53"));
					pstUpdate.setLong(48, rs.getLong("ATTRIBUTE_54")); // Number
					pstUpdate.setLong(49, rs.getLong("ATTRIBUTE_55")); // Number
					pstUpdate.setLong(50, rs.getLong("ATTRIBUTE_56")); // Number
					pstUpdate.setLong(51, rs.getLong("ATTRIBUTE_57"));// Number
					pstUpdate.setString(52, rs.getString("ATTRIBUTE_58"));
					pstUpdate.setString(53, rs.getString("ATTRIBUTE_59"));
					pstUpdate.setString(54, rs.getString("ATTRIBUTE_60"));
					pstUpdate.setString(55, rs.getString("ATTRIBUTE_61"));
					pstUpdate.setString(56, rs.getString("ATTRIBUTE_62"));
					pstUpdate.setString(57, rs.getString("ATTRIBUTE_63"));
					pstUpdate.setString(58, rs.getString("ATTRIBUTE_64"));
					pstUpdate.setString(59, rs.getString("ATTRIBUTE_65"));
					pstUpdate.setString(60, rs.getString("ATTRIBUTE_66"));
					pstUpdate.setString(61, rs.getString("ATTRIBUTE_67"));
					pstUpdate.setString(62, rs.getString("ATTRIBUTE_68"));
					pstUpdate.setString(63, rs.getString("ATTRIBUTE_69"));
					pstUpdate.setString(64, rs.getString("ATTRIBUTE_70"));
					pstUpdate.setString(65, rs.getString("ATTRIBUTE_71"));
					pstUpdate.setString(66, rs.getString("ATTRIBUTE_72"));
					pstUpdate.setString(67, rs.getString("ATTRIBUTE_73"));
					pstUpdate.setString(68, rs.getString("ATTRIBUTE_74"));
					pstUpdate.setString(69, rs.getString("ATTRIBUTE_75"));
					pstUpdate.setString(70, rs.getString("ATTRIBUTE_76"));
					pstUpdate.setString(71, rs.getString("ATTRIBUTE_77"));
					pstUpdate.setString(72, rs.getString("ATTRIBUTE_78"));
					pstUpdate.setDate(73, rs.getDate("ATTRIBUTE_79"));
					pstUpdate.setString(74, rs.getString("ATTRIBUTE_80"));
					pstUpdate.setDate(75, rs.getDate("ATTRIBUTE_81"));
					pstUpdate.setString(76, rs.getString("ATTRIBUTE_82"));
					pstUpdate.setString(77, rs.getString("ATTRIBUTE_84"));
					pstUpdate.setString(78, rs.getString("ATTRIBUTE_85"));
					pstUpdate.setString(79, rs.getString("ATTRIBUTE_86"));
					pstUpdate.setString(80, rs.getString("ATTRIBUTE_87"));
					pstUpdate.setString(81, rs.getString("ATTRIBUTE_88"));
					pstUpdate.setString(82, rs.getString("ATTRIBUTE_89"));
					pstUpdate.setString(83, rs.getString("ATTRIBUTE_90"));
					pstUpdate.setString(84, rs.getString("ATTRIBUTE_91"));
					pstUpdate.setString(85, rs.getString("ATTRIBUTE_92"));
					pstUpdate.setString(86, rs.getString("ATTRIBUTE_93"));
					pstUpdate.setString(87, rs.getString("ATTRIBUTE_94"));
					pstUpdate.setString(88, rs.getString("ATTRIBUTE_95"));
					pstUpdate.setString(89, rs.getString("ATTRIBUTE_96"));
					pstUpdate.setString(90, rs.getString("ATTRIBUTE_97"));
					pstUpdate.setString(91, rs.getString("ATTRIBUTE_98"));
					pstUpdate.setString(92, rs.getString("ATTRIBUTE_99"));
					pstUpdate.setString(93, rs.getString("ATTRIBUTE_100"));
					pstUpdate.setString(94, rs.getString("ATTRIBUTE_101"));
					pstUpdate.setString(95, rs.getString("ATTRIBUTE_105"));
					pstUpdate.setString(96, rs.getString("ATTRIBUTE_106"));
					pstUpdate.setString(97, rs.getString("ATTRIBUTE_107"));
					pstUpdate.setString(98, rs.getString("ATTRIBUTE_108"));
					pstUpdate.setString(99, rs.getString("ATTRIBUTE_109"));
					pstUpdate.setString(100, rs.getString("ATTRIBUTE_110"));
					pstUpdate.setString(101, rs.getString("ATTRIBUTE_111"));
					pstUpdate.setString(102, rs.getString("ATTRIBUTE_112"));
					pstUpdate.setString(103, rs.getString("ATTRIBUTE_113"));
					pstUpdate.setDate(104, rs.getDate("ATTRIBUTE_117"));
					pstUpdate.setLong(105, rs.getLong("ATTRIBUTE_118"));// Number
					pstUpdate.setDate(106, rs.getDate("ATTRIBUTE_114"));
					pstUpdate.setDate(107, rs.getDate("ATTRIBUTE_115"));
					pstUpdate.setString(108, rs.getString("ATTRIBUTE_116"));
					pstUpdate.setString(109, rs.getString("ATTRIBUTE_119"));
					pstUpdate.setString(110, rs.getString("ATTRIBUTE_120"));
					pstUpdate.setString(111, rs.getString("ATTRIBUTE_121"));
					pstUpdate.setString(112, rs.getString("ATTRIBUTE_122"));
					pstUpdate.setString(113, rs.getString("ATTRIBUTE_123"));

					boolean status = pstUpdate.execute();

					if (!status) {

						String updateOldserialNOStatus = "update UPD_SN_REPOS set ATTRIBUTE_37='SCR  "
								+ DateToStr
								+ "',ATTRIBUTE_41='"+serialNoOut+"' where serial_no='"
								+ serialNoIn
								+ "'";
						pstmt1 = connection2
								.prepareStatement(updateOldserialNOStatus);
						pstmt1.execute();

					}

					response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
					response.setResponseMessage(ServiceMessageCodes.READING_OLD_SERIAL_NO_INTO_NEW_SERIAL_NO);
				}

			} else {

				response.setResponseCode(""+ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE);
				response.setResponseMessage(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_SHIPMENT_TABLE_MSG);				

			}

		} catch (Exception e) {
			// innerupdatecon.rollback();
			e.printStackTrace();
			throw e;

		} finally {
			if (innerselectcon != null) {
				innerselectcon.close();
			}
		}

		return connection2;
	}

	public Connection updateReferenceTable(String serialNoIn,
			String serialNoOut, Connection con) throws SQLException, Exception {
		Connection innerselectcon = null;
		PreparedStatement pst = null;
		PreparedStatement pstUpdate = null;
		ResultSet rs = null;

		StringBuffer sbuffer = new StringBuffer();
		StringBuffer SQLInnerQuery = new StringBuffer();
		sbuffer.append("SELECT SERIAL_NO,REFERENCE_KEY,STATUS FROM upd_sn_repos_ref WHERE SERIAL_NO=? AND STATUS IS NULL");
		try {
			innerselectcon = DBUtil.getConnection(ds);
			pst = innerselectcon.prepareStatement(sbuffer.toString());
			pst.setString(1, serialNoIn);
			rs = pst.executeQuery();
			if (rs.next()) {

				SQLInnerQuery
				.append("INSERT INTO upd_sn_repos_ref (SERIAL_NO,REFERENCE_KEY,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME) VALUES (?,?,'pcba_pgm',sysdate,'pcba_pgm',sysdate)");
				pstUpdate = con.prepareStatement(SQLInnerQuery.toString());
				pstUpdate.setString(1, serialNoOut);
				pstUpdate.setString(2, rs.getString("REFERENCE_KEY"));
				boolean status = pstUpdate.execute();

				if (!status) {

					String updateOldserialNOStatus = "update upd_sn_repos_ref set STATUS='SCR'  where serial_no='"
							+ serialNoIn + "'";
					pstmt1 = con.prepareStatement(updateOldserialNOStatus);
					pstmt1.execute();

				}

			} else {
				throw new Exception("Serial not found in UPD_SN_REPO_REF table");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			throw e;
		}

		return con;
	}

	@Override
	public int checkValidSerialNoIn(String SerialNoIn) {
		// TODO Auto-generated method stub
		Connection conn1=null;
		Connection conn2=null;
		PreparedStatement pstmt1=null;
		PreparedStatement pstmt2=null;
		ResultSet rs1=null;
		ResultSet rs2=null;
		int referenceKeyCount =0;
		// TODO Auto-generated method stub
		try {

			ds = DBUtil.getOracleDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in MEID:" + e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG+e.getMessage());

		}

		try {
			// get database connection
			conn1 = DBUtil.getConnection(ds);
			String query="select Attribute_99 from UPD_SN_REPOS where serial_no=?";
			pstmt1 = conn1.prepareStatement(query);
			pstmt1.setString(1,SerialNoIn);
			rs1 = pstmt1.executeQuery();
			String referenceKey = null;
			String referenceKeyQuery="select count(*) from upd_sn_repos_ref where status is null and reference_key=?";

			if (rs1.next()) {
				referenceKey=rs1.getString("Attribute_99");
				if(referenceKey!=null && !(referenceKey.equals(""))){

					conn2 = DBUtil.getConnection(ds);
					pstmt2 = conn2.prepareStatement(referenceKeyQuery);
					pstmt2.setString(1, referenceKey);
					rs2 = pstmt2.executeQuery();
					if(rs2.next()){
						referenceKeyCount = rs2.getInt(1);
					}


				}
			}

		}catch(Exception e){
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG+e.getMessage());

		}finally{
			DBUtil.closeConnection(conn1, pstmt1, rs1);
			DBUtil.connectionClosed(conn2, pstmt2);
		}
		return referenceKeyCount;
	}

}
