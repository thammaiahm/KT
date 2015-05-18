/**
 * 
 */
package com.mot.upd.pcba.dao;

import java.sql.Connection;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class PCBASwapUPDUpdateSQLDAO implements PCBASwapUPDUpdateInterfaceDAO {
	private static Logger logger = Logger
			.getLogger(PCBASwapUPDUpdateSQLDAO.class);

	private DataSource ds;
	private Connection con = null;
	private Connection conn = null;
	private Connection con1 = null;
	private PreparedStatement preparedStmt = null;
	private PreparedStatement prestmt = null;
	private PreparedStatement pstmt1 = null;
	private ResultSet rs = null;

	PCBASerialNoUPdateResponse response = new PCBASerialNoUPdateResponse();

	public PCBASerialNoUPdateResponse serialNumberInfo(
			PCBASerialNoUPdateQueryInput pCBASerialNoUPdateQueryInput) {
		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in MEID:" + e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG);
			return response;
		}

		try {
			// get database connection
			con = DBUtil.getConnection(ds);

			String mySQLQuery = "select fsi.factory_code,fsi.gen_date,fsi.protocol,fsi.apc,fsi.trans_model,fsi.cust_model,fsi.mkt_model,fsi.item_code,fsi.warr_code,fsi.ship_date,"
					+ "fsi.ship_to_cust_id,fsi.ship_to_cust_addr,fsi.ship_to_cust_name,fsi.ship_to_cust_city,fsi.ship_to_cust_country,fsi.sold_to_cust_id,fsi.sold_to_cust_name,"
					+ "fsi.sold_date,fsi.cit,fsi.ta_no,fsi.carton_id,fsi.po_no,fsi.so_no,fsi.fo_sequence,fsi.msn,r.csn,wi.status_code,wi.orig_warr_eff_date,wi.ren_warr_code,"
					+ "r.cancel_code,r.swap_ref_no,r.swap_count,r.delete_flag,r.org_code,r.upd_time,r.dn,r.era,r.rma,r.receive_date,r.delivery_date,r.return_date,r.rma_date,"
					+ "r.scrap_date,r.cust_id,r.warehouse_id,r.shop_id,r.status_id,r.action,r.remarks,dc.handset_type,dc.flex_option,dc.flex_sw,dc.hw,dc.icc_id,ds.so_line_no,"
					+ "ds.ds_region_code,ds.ds_so_no,ds.ds_po_no,ds.ds_cust_id,ds.ds_bill_to_id,ds.ds_ship_to_id,ds.ds_cust_country_code,ds.ds_cust_name,ds.bill_to_id,ds.shipment_no,"
					+ "ds.phone_no,ds.wip_dj,ds.sale_date,ds.last_imei,r.swap_date,dc.wlan,lc.meid_evdo_password,lc.meid_a_key2_type,lc.meid_a_key2,dc.fastt_id,dc.base_processor_id,"
					+ "fsi.location_type,fsi.packing_list,fsi.fab_date,dc.software_version,wi.warr_country_code,wi.warr_region,wi.orig_ship_date,wi.reference_key,dc.wimax,dc.hsn,"
					+ "m.a_key_index,m.cas_no,lc.imc_lock_code,dc.flash_uid,fsi.imc_mfg_location,fsi.guid,fsi.pdb_id,dc.dual_serial_no,dc.dual_serial_no_type,wi.pop_in_sysdate,"
					+ "wi.pop_date,wi.pop_identifier,r.last_repair_date,r.repair_count,fsi.assign_date,fsi.gpp_id,fsi.product_type"
					+ " from upd.upd_factory_shipment_info fsi,upd.upd_repair r,upd.upd_warranty_info wi,upd.upd_device_config dc,upd.upd_direct_shipment ds,upd.upd_lock_code lc,upd.upd_meid m"
					+ " WHERE fsi.serial_no=r.serial_no AND r.serial_no=wi.serial_no AND wi.serial_no=dc.serial_no AND dc.serial_no=ds.serial_no AND ds.serial_no=lc.serial_no AND lc.serial_no=m.serial_no AND m.serial_no=?";

			preparedStmt = con.prepareStatement(mySQLQuery);
			preparedStmt.setString(1,
					pCBASerialNoUPdateQueryInput.getSerialNoIn());
			rs = preparedStmt.executeQuery();

			if (rs.next()) {

				String serialNoStatus=rs.getString("status_code");				
				if(!serialNoStatus.startsWith("ACT")){
					StringBuffer stb = new StringBuffer();
					stb.append("insert into upd.upd_shipment_notavail_sn(SERIAL_NO_IN,SERIAL_NO_OUT,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,STATUS) values(?,?,?,?,?,?,?)");
					prestmt = con.prepareStatement(stb.toString());
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

				}else{				


					String MySql_updFactoryShipmentInfo = "update upd.upd_factory_shipment_info set factory_code,gen_date=?,protocol=?,apc=?,trans_model=?,cust_model=?,mkt_model=?,item_code=?,warr_code=?,"
							+ "ship_date=?,ship_to_cust_id=?,ship_to_cust_addr=?,ship_to_cust_name=?,ship_to_cust_city=?,ship_to_cust_country=?,sold_to_cust_id=?,sold_to_cust_name=?,sold_date=?,"
							+ "cit=?,ta_no=?,carton_id=?,po_no=?,so_no=?,fo_sequence=?,msn=?,assign_date=?,gpp_id=?,product_type=?,location_type=?,packing_list=?,fab_date=?,imc_mfg_location=?,)"
							+ "guid=?,pdb_id=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					con1 = DBUtil.getConnection(ds);
					con1.setAutoCommit(false);

					pstmt1 = con1.prepareStatement(MySql_updFactoryShipmentInfo);

					if(rs.getString("factory_code")!=null && !(rs.getString("factory_code").equals(""))){
						pstmt1.setString(1, rs.getString("factory_code"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getDate("gen_date")!=null && !(rs.getDate("gen_date").equals(""))){
						pstmt1.setDate(2, rs.getDate("gen_date"));
					}else{
						pstmt1.setDate(2,null);
					}

					if(rs.getString("protocol")!=null && !(rs.getString("protocol").equals(""))){
						pstmt1.setString(3, rs.getString("protocol"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("apc")!=null && !(rs.getString("apc").equals(""))){
						pstmt1.setString(4, rs.getString("apc"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("trans_model")!=null && !(rs.getString("trans_model").equals(""))){
						pstmt1.setString(5, rs.getString("trans_model"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("cust_model")!=null && !(rs.getString("cust_model").equals(""))){
						pstmt1.setString(6, rs.getString("cust_model"));
					}else{
						pstmt1.setString(6,null);	
					}

					if(rs.getString("mkt_model")!=null && !(rs.getString("mkt_model").equals(""))){
						pstmt1.setString(7, rs.getString("mkt_model"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("item_code")!=null && !(rs.getString("item_code").equals(""))){
						pstmt1.setString(8, rs.getString("item_code"));
					}else{
						pstmt1.setString(8,null);	
					}

					if(rs.getString("warr_code")!=null && !(rs.getString("warr_code").equals(""))){
						pstmt1.setString(9, rs.getString("warr_code"));
					}else{
						pstmt1.setString(9,null);
					}
					if(rs.getDate("ship_date")!=null && !(rs.getDate("ship_date").equals(""))){
						pstmt1.setDate(10, rs.getDate("ship_date"));
					}else{
						pstmt1.setDate(10,null);	
					}

					if(rs.getString("ship_to_cust_id")!=null && !(rs.getString("ship_to_cust_id").equals(""))){
						pstmt1.setString(11, rs.getString("ship_to_cust_id"));
					}else{
						pstmt1.setString(11,null);
					}

					if(rs.getString("ship_to_cust_addr")!=null && !(rs.getString("ship_to_cust_addr").equals(""))){
						pstmt1.setString(12, rs.getString("ship_to_cust_addr"));
					}else{
						pstmt1.setString(12,null);	
					}

					if(rs.getString("ship_to_cust_name")!=null && !(rs.getString("ship_to_cust_name").equals(""))){
						pstmt1.setString(13, rs.getString("ship_to_cust_name"));
					}else{
						pstmt1.setString(13,null);
					}

					if(rs.getString("ship_to_cust_city")!=null && !(rs.getString("ship_to_cust_city").equals(""))){
						pstmt1.setString(14, rs.getString("ship_to_cust_city"));
					}else{
						pstmt1.setString(14,null);
					}

					if(rs.getString("ship_to_cust_country")!=null && !(rs.getString("ship_to_cust_country").equals(""))){
						pstmt1.setString(15, rs.getString("ship_to_cust_country"));
					}else{
						pstmt1.setString(15,null);
					}

					if(rs.getString("sold_to_cust_id")!=null && !(rs.getString("sold_to_cust_id").equals(""))){
						pstmt1.setString(16, rs.getString("sold_to_cust_id"));
					}else{
						pstmt1.setString(16,null);
					}

					if(rs.getString("sold_to_cust_name")!=null && !(rs.getString("sold_to_cust_name").equals(""))){
						pstmt1.setString(17, rs.getString("sold_to_cust_name"));
					}else{
						pstmt1.setString(17,null);
					}

					if(rs.getDate("sold_date")!=null && !(rs.getDate("sold_date").equals(""))){
						pstmt1.setDate(18, rs.getDate("sold_date"));
					}else{
						pstmt1.setDate(18,null);
					}

					if(rs.getString("cit")!=null && !(rs.getString("cit").equals(""))){
						pstmt1.setString(19, rs.getString("cit"));
					}else{
						pstmt1.setString(19,null);
					}

					if(rs.getString("ta_no")!=null && !(rs.getString("ta_no").equals(""))){
						pstmt1.setString(20, rs.getString("ta_no"));
					}else{
						pstmt1.setString(20,null);	
					}

					if(rs.getString("carton_id")!=null){
						pstmt1.setString(21, rs.getString("carton_id"));
					}else{
						pstmt1.setString(21,null);	
					}

					if(rs.getString("po_no")!=null && !(rs.getString("po_no").equals(""))){
						pstmt1.setString(22, rs.getString("po_no"));
					}else{
						pstmt1.setString(22,null);	
					}

					if(rs.getString("so_no")!=null && !(rs.getString("so_no").equals(""))){
						pstmt1.setString(23, rs.getString("so_no"));
					}else{
						pstmt1.setString(23,null);
					}

					if(rs.getString("fo_sequence")!=null && !(rs.getString("fo_sequence").equals(""))){
						pstmt1.setString(24, rs.getString("fo_sequence"));
					}else{
						pstmt1.setString(24,null);
					}

					if(rs.getString("msn")!=null && !(rs.getString("msn").equals(""))){
						pstmt1.setString(25, rs.getString("msn"));
					}else{
						pstmt1.setString(25,null);	
					}

					if(rs.getDate("assign_date")!=null && !(rs.getDate("assign_date").equals(""))){
						pstmt1.setDate(26, rs.getDate("assign_date"));
					}else{
						pstmt1.setDate(26,null);
					}

					if(rs.getString("gpp_id")!=null && !(rs.getString("gpp_id").equals(""))){
						pstmt1.setString(27, rs.getString("gpp_id"));
					}else{
						pstmt1.setString(27,null);	
					}

					if(rs.getString("product_type")!=null && !(rs.getString("product_type").equals(""))){
						pstmt1.setString(28, rs.getString("product_type"));
					}else{
						pstmt1.setString(28,null);
					}

					if(rs.getString("location_type")!=null && !(rs.getString("location_type").equals(""))){
						pstmt1.setString(29, rs.getString("location_type"));
					}else{
						pstmt1.setString(29,null);
					}

					if(rs.getString("packing_list")!=null && !(rs.getString("packing_list").equals(""))){
						pstmt1.setString(30, rs.getString("packing_list"));
					}else{
						pstmt1.setString(30,null);	
					}

					if(rs.getDate("fab_date")!=null && !(rs.getDate("fab_date").equals(""))){
						pstmt1.setDate(31, rs.getDate("fab_date"));
					}else{
						pstmt1.setDate(31,null);
					}

					if(rs.getString("imc_mfg_location")!=null && !(rs.getString("imc_mfg_location").equals(""))){
						pstmt1.setString(32, rs.getString("imc_mfg_location"));
					}else{
						pstmt1.setString(32,null);
					}

					if(rs.getString("guid")!=null && !(rs.getString("guid").equals(""))){
						pstmt1.setString(33, rs.getString("guid"));
					}else{
						pstmt1.setString(33,null);
					}

					if(rs.getString("pdb_id")!=null && !(rs.getString("pdb_id").equals(""))){
						pstmt1.setString(34, rs.getString("pdb_id"));
					}else{
						pstmt1.setString(34,null);
					}

					pstmt1.setString(35, pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status1 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updRepair = "update upd.upd_repair set cancel_code=?,swap_ref_no=?,swap_count=?,delete_flag=?,org_code=?,upd_time=?,dn=?,era=?,rma=?,receive_date=?,"
							+ "delivery_date=?,return_date=?,rma_date=?,scrap_date=?,cust_id=?,warehouse_id=?,shop_id=?,status_id=?,action=?,remarks=?,csn=?,last_repair_date=?,repair_count=?,swap_date=?,)"
							+ "last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con1.prepareStatement(MySql_updRepair);

					if(rs.getString("cancel_code")!=null && !(rs.getString("cancel_code").equals(""))){
						pstmt1.setString(1, rs.getString("cancel_code"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("swap_ref_no")!=null && !(rs.getString("swap_ref_no").equals(""))){
						pstmt1.setString(2, rs.getString("swap_ref_no"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("swap_count")!=null && !(rs.getString("swap_count").equals(""))){
						pstmt1.setString(3, rs.getString("swap_count"));
					}else{
						pstmt1.setString(3,null);	
					}

					if(rs.getString("delete_flag")!=null && !(rs.getString("delete_flag").equals(""))){
						pstmt1.setString(4, rs.getString("delete_flag"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("org_code")!=null && !(rs.getString("org_code").equals(""))){
						pstmt1.setString(5, rs.getString("org_code"));
					}else{
						pstmt1.setString(5,null);	
					}

					if(rs.getString("upd_time")!=null && !(rs.getString("upd_time").equals(""))){
						pstmt1.setString(6, rs.getString("upd_time"));
					}else{
						pstmt1.setString(6,null);	
					}

					if(rs.getString("dn")!=null && !(rs.getString("dn").equals(""))){
						pstmt1.setString(7, rs.getString("dn"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("era")!=null && !(rs.getString("era").equals(""))){
						pstmt1.setString(8, rs.getString("era"));
					}else{
						pstmt1.setString(8,null);
					}

					if(rs.getString("rma")!=null && !(rs.getString("rma").equals(""))){
						pstmt1.setString(9, rs.getString("rma"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getDate("receive_date")!=null && !(rs.getDate("receive_date").equals(""))){
						pstmt1.setDate(10, rs.getDate("receive_date"));
					}else{
						pstmt1.setDate(10,null);
					}

					if(rs.getDate("delivery_date")!=null && !(rs.getDate("delivery_date").equals(""))){
						pstmt1.setDate(11, rs.getDate("delivery_date"));
					}else{
						pstmt1.setDate(11,null);	
					}

					if(rs.getDate("return_date")!=null && !(rs.getDate("return_date").equals(""))){
						pstmt1.setDate(12, rs.getDate("return_date"));
					}else{
						pstmt1.setDate(12,null);
					}

					if(rs.getDate("rma_date")!=null && !(rs.getDate("rma_date").equals(""))){
						pstmt1.setDate(13, rs.getDate("rma_date"));
					}else{
						pstmt1.setDate(13,null);
					}

					if(rs.getDate("scrap_date")!=null && !(rs.getDate("scrap_date").equals(""))){
						pstmt1.setDate(14, rs.getDate("scrap_date"));
					}else{
						pstmt1.setDate(14,null);	
					}

					if(rs.getString("cust_id")!=null && !(rs.getString("cust_id").equals(""))){
						pstmt1.setString(15, rs.getString("cust_id"));
					}else{
						pstmt1.setString(15,null);
					}

					if(rs.getString("warehouse_id")!=null && !(rs.getString("warehouse_id").equals(""))){
						pstmt1.setString(16, rs.getString("warehouse_id"));
					}else{
						pstmt1.setString(16,null);
					}

					if(rs.getString("shop_id")!=null && !(rs.getString("shop_id").equals(""))){
						pstmt1.setString(17, rs.getString("shop_id"));
					}else{
						pstmt1.setString(17,null);	
					}

					if(rs.getString("status_id")!=null && !(rs.getString("status_id").equals(""))){
						pstmt1.setString(18, rs.getString("status_id"));
					}else{
						pstmt1.setString(18,null);
					}

					if(rs.getString("action")!=null && !(rs.getString("action").equals(""))){
						pstmt1.setString(19, rs.getString("action"));
					}else{
						pstmt1.setString(19,null);
					}

					if(rs.getString("remarks")!=null && !(rs.getString("remarks").equals(""))){
						pstmt1.setString(20, rs.getString("remarks"));
					}else{
						pstmt1.setString(20,null);
					}

					if(rs.getString("csn")!=null && !(rs.getString("csn").equals(""))){
						pstmt1.setString(21, rs.getString("csn"));
					}else{
						pstmt1.setString(21,null);	
					}

					if(rs.getDate("last_repair_date")!=null && !(rs.getDate("last_repair_date").equals(""))){
						pstmt1.setDate(22, rs.getDate("last_repair_date"));
					}else{
						pstmt1.setDate(22,null);
					}

					if(rs.getString("repair_count")!=null && !(rs.getString("repair_count").equals(""))){
						pstmt1.setString(23, rs.getString("repair_count"));
					}else{
						pstmt1.setString(23,null);
					}

					if(rs.getDate("swap_date")!=null && !(rs.getDate("swap_date").equals(""))){
						pstmt1.setDate(24, rs.getDate("swap_date"));
					}else{
						pstmt1.setDate(24,null);
					}

					pstmt1.setString(25,pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status2 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updWarrantyInfo = "update upd.upd_warranty_info set status_code=?,orig_warr_eff_date=?,ren_warr_code=?,warr_country_code=?,warr_region=?,orig_ship_date=?,reference_key=?,"
							+ "pop_in_sysdate=?,pop_date=?,pop_identifier=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con1.prepareStatement(MySql_updWarrantyInfo);

					Date curDate = new Date();
					SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyy");
					String DateToStr = format.format(curDate);


					pstmt1.setString(1, "ACT     " + DateToStr);

					if(rs.getDate("orig_warr_eff_date")!=null && !(rs.getDate("orig_warr_eff_date").equals(""))){
						pstmt1.setDate(2, rs.getDate("orig_warr_eff_date"));
					}else{
						pstmt1.setDate(2,null);
					}

					if(rs.getString("ren_warr_code")!=null && !(rs.getString("ren_warr_code").equals(""))){
						pstmt1.setString(3, rs.getString("ren_warr_code"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("warr_country_code")!=null && !(rs.getString("warr_country_code").equals(""))){
						pstmt1.setString(4, rs.getString("warr_country_code"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("warr_region")!=null && !(rs.getString("warr_region").equals(""))){
						pstmt1.setString(5, rs.getString("warr_region"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("orig_ship_date")!=null && !(rs.getString("orig_ship_date").equals(""))){
						pstmt1.setString(6, rs.getString("orig_ship_date"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("reference_key")!=null && !(rs.getString("reference_key").equals(""))){
						pstmt1.setString(7, rs.getString("reference_key"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getDate("pop_in_sysdate")!=null && !(rs.getDate("pop_in_sysdate").equals(""))){
						pstmt1.setDate(8, rs.getDate("pop_in_sysdate"));
					}else{
						pstmt1.setDate(8, null);	
					}

					if(rs.getDate("pop_date")!=null && !(rs.getDate("pop_date").equals(""))){
						pstmt1.setDate(9, rs.getDate("pop_date"));
					}else{
						pstmt1.setDate(9,null);
					}

					if(rs.getString("pop_identifier")!=null && !(rs.getString("pop_identifier").equals(""))){
						pstmt1.setString(10, rs.getString("pop_identifier"));
					}else{
						pstmt1.setString(10,null);
					}

					pstmt1.setString(11,pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status3 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updDeviceConfig = "update upd.upd_device_config set handset_type=?,flex_option=?,flex_sw=?,hw=?,icc_id=?,software_version=?,wimax=?,hsn=?,flash_uid=?,dual_serial_no=?,"
							+ "dual_serial_no_type=?,fastt_id=?,base_processor_id=?,wlan=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";


					pstmt1 = con1.prepareStatement(MySql_updDeviceConfig);

					if(rs.getString("handset_type")!=null && !(rs.getString("handset_type").equals(""))){
						pstmt1.setString(1, rs.getString("handset_type"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("flex_option")!=null && !(rs.getString("flex_option").equals(""))){
						pstmt1.setString(2, rs.getString("flex_option"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("flex_sw")!=null){
						pstmt1.setString(3, rs.getString("flex_sw"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("hw")!=null && !(rs.getString("hw").equals(""))){
						pstmt1.setString(4, rs.getString("hw"));
					}else{
						pstmt1.setString(4,null);	
					}

					if(rs.getString("icc_id")!=null && !(rs.getString("icc_id").equals(""))){
						pstmt1.setString(5, rs.getString("icc_id"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("software_version")!=null && !(rs.getString("software_version").equals(""))){
						pstmt1.setString(6, rs.getString("software_version"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("wimax")!=null && !(rs.getString("wimax").equals(""))){
						pstmt1.setString(7, rs.getString("wimax"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("hsn")!=null && !(rs.getString("hsn").equals(""))){
						pstmt1.setString(8, rs.getString("hsn"));
					}else{
						pstmt1.setString(8,null);
					}

					if(rs.getString("flash_uid")!=null && !(rs.getString("flash_uid").equals(""))){
						pstmt1.setString(9, rs.getString("flash_uid"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getString("dual_serial_no")!=null && !(rs.getString("dual_serial_no").equals(""))){
						pstmt1.setString(10, rs.getString("dual_serial_no"));
					}else{
						pstmt1.setString(10,null);
					}

					if(rs.getString("dual_serial_no_type")!=null && !(rs.getString("dual_serial_no_type").equals(""))){
						pstmt1.setString(11, rs.getString("dual_serial_no_type"));
					}else{
						pstmt1.setString(11,null);
					}

					if(rs.getString("fastt_id")!=null && !(rs.getString("fastt_id").equals(""))){
						pstmt1.setString(12, rs.getString("fastt_id"));
					}else{
						pstmt1.setString(12,null);
					}

					if(rs.getString("base_processor_id")!=null && !(rs.getString("base_processor_id").equals(""))){
						pstmt1.setString(13, rs.getString("base_processor_id"));
					}else{
						pstmt1.setString(13,null);	
					}

					if(rs.getString("wlan")!=null && !(rs.getString("wlan").equals(""))){
						pstmt1.setString(14, rs.getString("wlan"));
					}else{
						pstmt1.setString(14,null);	
					}

					pstmt1.setString(15,pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status4 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updDirectShipment = "update upd.upd_direct_shipment set so_line_no=?,ds_region_code=?,ds_so_no=?,ds_po_no=?,ds_cust_id=?,ds_bill_to_id=?,ds_ship_to_id=?,ds_cust_country_code=?,"
							+ "ds_cust_name=?,bill_to_id=?,shipment_no=?,phone_no=?,wip_dj=?,sale_date=?,last_imei=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con1.prepareStatement(MySql_updDirectShipment);

					if(rs.getString("so_line_no")!=null && !(rs.getString("so_line_no").equals(""))){
						pstmt1.setString(1, rs.getString("so_line_no"));
					}else{
						pstmt1.setString(1,null);	
					}

					if(rs.getString("ds_region_code")!=null && !(rs.getString("ds_region_code").equals(""))){
						pstmt1.setString(2, rs.getString("ds_region_code"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("ds_so_no")!=null && !(rs.getString("ds_so_no").equals(""))){
						pstmt1.setString(3, rs.getString("ds_so_no"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("ds_po_no")!=null && !(rs.getString("ds_po_no").equals(""))){
						pstmt1.setString(4, rs.getString("ds_po_no"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("ds_cust_id")!=null && !(rs.getString("ds_cust_id").equals(""))){
						pstmt1.setString(5, rs.getString("ds_cust_id"));
					}else{
						pstmt1.setString(5,null);	
					}

					if(rs.getString("ds_bill_to_id")!=null && !(rs.getString("ds_bill_to_id").equals(""))){
						pstmt1.setString(6, rs.getString("ds_bill_to_id"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("ds_ship_to_id")!=null && !(rs.getString("ds_ship_to_id").equals(""))){
						pstmt1.setString(7, rs.getString("ds_ship_to_id"));
					}else{
						pstmt1.setString(7,null);	
					}

					if(rs.getString("ds_cust_country_code")!=null && !(rs.getString("ds_cust_country_code").equals(""))){
						pstmt1.setString(8, rs.getString("ds_cust_country_code"));
					}else{
						pstmt1.setString(8,null);	
					}

					if(rs.getString("ds_cust_name")!=null && !(rs.getString("ds_cust_name").equals(""))){
						pstmt1.setString(9, rs.getString("ds_cust_name"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getString("bill_to_id")!=null && !(rs.getString("bill_to_id").equals(""))){
						pstmt1.setString(10, rs.getString("bill_to_id"));
					}else{
						pstmt1.setString(10,null);
					}

					if(rs.getString("shipment_no")!=null && !(rs.getString("shipment_no").equals(""))){
						pstmt1.setString(11, rs.getString("shipment_no"));
					}else{
						pstmt1.setString(11,null);	
					}

					if(rs.getString("phone_no")!=null && !(rs.getString("phone_no").equals(""))){
						pstmt1.setString(12, rs.getString("phone_no"));
					}else{
						pstmt1.setString(12,null);
					}

					if(rs.getString("wip_dj")!=null && !(rs.getString("wip_dj").equals(""))){
						pstmt1.setString(13, rs.getString("wip_dj"));
					}else{
						pstmt1.setString(13,null);
					}

					if(rs.getDate("sale_date")!=null && !(rs.getDate("sale_date").equals(""))){
						pstmt1.setDate(14, rs.getDate("sale_date"));
					}else{
						pstmt1.setDate(14,null);
					}

					if( rs.getString("last_imei")!=null && !( rs.getString("last_imei").equals(""))){
						pstmt1.setString(15, rs.getString("last_imei"));
					}else{
						pstmt1.setString(15, null);
					}

					pstmt1.setString(16,pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status5 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updLockCode = "update upd.upd_lock_code set meid_evdo_password=?,meid_a_key2_type=?,meid_a_key2=?,imc_lock_code=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con1.prepareStatement(MySql_updLockCode);

					if(rs.getString("meid_evdo_password")!=null && !(rs.getString("meid_evdo_password").equals(""))){
						pstmt1.setString(1, rs.getString("meid_evdo_password"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("meid_a_key2_type")!=null && !(rs.getString("meid_a_key2_type").equals(""))){
						pstmt1.setString(2, rs.getString("meid_a_key2_type"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("meid_a_key2")!=null && !(rs.getString("meid_a_key2").equals(""))){
						pstmt1.setString(3, rs.getString("meid_a_key2"));
					}else{
						pstmt1.setString(3,null);
					}

					if( rs.getString("imc_lock_code")!=null && !( rs.getString("imc_lock_code").equals(""))){
						pstmt1.setString(4, rs.getString("imc_lock_code"));
					}else{
						pstmt1.setString(4,null);
					}

					pstmt1.setString(5,	pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status6 = pstmt1.execute();

					pstmt1 = null;

					String Sql_updMeid = "update upd.upd_meid set a_key_index=?,cas_no=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con1.prepareStatement(Sql_updMeid);

					if(rs.getString("a_key_index")!=null && !(rs.getString("a_key_index").equals(""))){
						pstmt1.setString(1, rs.getString("a_key_index"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("cas_no")!=null && !(rs.getString("cas_no").equals(""))){
						pstmt1.setString(2, rs.getString("cas_no"));
					}else{
						pstmt1.setString(2,null);
					}					

					pstmt1.setString(3,pCBASerialNoUPdateQueryInput.getSerialNoOut());

					boolean status7 = pstmt1.execute();

					pstmt1 = null;

					if (!status1 && !status2 && !status3 && !status4 && !status5
							&& !status6 && !status7) {
						String statusCode = "SCR  " + DateToStr;

						String statusUpdatingOldSerialNo = "update upd.upd_warranty_info set status_code='"
								+ statusCode
								+ "' where serial_no='"
								+ pCBASerialNoUPdateQueryInput.getSerialNoIn()
								+ "'";
						pstmt1 = con1.prepareStatement(statusUpdatingOldSerialNo);
						pstmt1.execute();
						pstmt1=null;

						String statusOfnewSerialNO="update upd.upd_repair set swap_ref_no='"+pCBASerialNoUPdateQueryInput.getSerialNoOut()+"' where serial_no='"+pCBASerialNoUPdateQueryInput.getSerialNoIn()+"'";
						pstmt1 = con1.prepareStatement(statusOfnewSerialNO);
						pstmt1.execute();

					}

					response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
					response.setResponseMessage(ServiceMessageCodes.OPERATION_SUCCESS);
				}

			} else {

				response.setResponseCode(""+ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE);
				response.setResponseMessage(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE_MSG);
				return response;

			}
			// Dual Case

			if (pCBASerialNoUPdateQueryInput.getDualSerialNoIn() != null) {
				if (!(pCBASerialNoUPdateQueryInput.getDualSerialNoIn() == "")) {
					if (!(pCBASerialNoUPdateQueryInput.getDualSerialNoIn().trim() == "")) {
						String DualStatus = getStatus(pCBASerialNoUPdateQueryInput.getDualSerialNoIn());
						if(DualStatus!=null && DualStatus.startsWith("ACT")){
							con1 = updateReferenceTable(
									pCBASerialNoUPdateQueryInput.getSerialNoIn(),
									pCBASerialNoUPdateQueryInput.getSerialNoOut(), con1);
							con1 = updateBasedOnSerial(
									pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),
									pCBASerialNoUPdateQueryInput.getDualSerialNoOut(),
									ds, con1);
							con1 = updateReferenceTable(
									pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),
									pCBASerialNoUPdateQueryInput.getDualSerialNoOut(),
									con1);
						}else{
							con1.rollback();
							sendEmail(pCBASerialNoUPdateQueryInput.getDualSerialNoIn(),pCBASerialNoUPdateQueryInput.getDualSerialNoOut(),con1,prestmt);
							response.setResponseCode(ServiceMessageCodes.EMAIL_MSG_CODE);
							response.setResponseMessage(ServiceMessageCodes.EMAIL_MSG);
							con1.commit();
						}
					}
				}
			}

			// Tri Case
			if (pCBASerialNoUPdateQueryInput.getTriSerialNoIn() != null) {
				if (!(pCBASerialNoUPdateQueryInput.getTriSerialNoIn().trim() == "")) {

					String triStatus = getStatus(pCBASerialNoUPdateQueryInput.getTriSerialNoIn());

					if(triStatus!=null && triStatus.startsWith("ACT")){
					con1 = updateBasedOnSerial(
							pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getTriSerialNoOut(),
							ds, con1);
					con1 = updateReferenceTable(
							pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),
							pCBASerialNoUPdateQueryInput.getTriSerialNoOut(),
							con1);
					}else{
						con1.rollback();
						sendEmail(pCBASerialNoUPdateQueryInput.getTriSerialNoIn(),pCBASerialNoUPdateQueryInput.getTriSerialNoOut(),con1,prestmt);
						con1.commit();
						return response;
						
					}

				}
			}
			con1.commit();

		} catch (Exception e) {
			try {
				con1.rollback();
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
			DBUtil.connectionClosed(conn, prestmt);
			DBUtil.connectionClosed(con1, pstmt1);
		}
		// Dual Case

		return response;
	}

	public Connection updateBasedOnSerial(String serialNoIn,
			String serialNoOut, DataSource ds, Connection con12)
					throws Exception {

		Connection innerselectcon = null;

		PreparedStatement preparedStmt = null;
		PreparedStatement pstmt1 = null;
		try {

			innerselectcon = DBUtil.getConnection(ds);

			String mySQLQuery = "select fsi.factory_code,fsi.gen_date,fsi.protocol,fsi.apc,fsi.trans_model,fsi.cust_model,fsi.mkt_model,fsi.item_code,fsi.warr_code,fsi.ship_date,"
					+ "fsi.ship_to_cust_id,fsi.ship_to_cust_addr,fsi.ship_to_cust_name,fsi.ship_to_cust_city,fsi.ship_to_cust_country,fsi.sold_to_cust_id,fsi.sold_to_cust_name,"
					+ "fsi.sold_date,fsi.cit,fsi.ta_no,fsi.carton_id,fsi.po_no,fsi.so_no,fsi.fo_sequence,fsi.msn,r.csn,wi.status_code,wi.orig_warr_eff_date,wi.ren_warr_code,"
					+ "r.cancel_code,r.swap_ref_no,r.swap_count,r.delete_flag,r.org_code,r.upd_time,r.dn,r.era,r.rma,r.receive_date,r.delivery_date,r.return_date,r.rma_date,"
					+ "r.scrap_date,r.cust_id,r.warehouse_id,r.shop_id,r.status_id,r.action,r.remarks,dc.handset_type,dc.flex_option,dc.flex_sw,dc.hw,dc.icc_id,ds.so_line_no,"
					+ "ds.ds_region_code,ds.ds_so_no,ds.ds_po_no,ds.ds_cust_id,ds.ds_bill_to_id,ds.ds_ship_to_id,ds.ds_cust_country_code,ds.ds_cust_name,ds.bill_to_id,ds.shipment_no,"
					+ "ds.phone_no,ds.wip_dj,ds.sale_date,ds.last_imei,r.swap_date,dc.wlan,lc.meid_evdo_password,lc.meid_a_key2_type,lc.meid_a_key2,dc.fastt_id,dc.base_processor_id,"
					+ "fsi.location_type,fsi.packing_list,fsi.fab_date,dc.software_version,wi.warr_country_code,wi.warr_region,wi.orig_ship_date,wi.reference_key,dc.wimax,dc.hsn,"
					+ "m.a_key_index,m.cas_no,lc.imc_lock_code,dc.flash_uid,fsi.imc_mfg_location,fsi.guid,fsi.pdb_id,dc.dual_serial_no,dc.dual_serial_no_type,wi.pop_in_sysdate,"
					+ "wi.pop_date,wi.pop_identifier,r.last_repair_date,r.repair_count,fsi.assign_date,fsi.gpp_id,fsi.product_type"
					+ " from upd.upd_factory_shipment_info fsi,upd.upd_repair r,upd.upd_warranty_info wi,upd.upd_device_config dc,upd.upd_direct_shipment ds,upd.upd_lock_code lc,upd.upd_meid m"
					+ " WHERE fsi.serial_no=r.serial_no AND r.serial_no=wi.serial_no AND wi.serial_no=dc.serial_no AND dc.serial_no=ds.serial_no AND ds.serial_no=lc.serial_no AND lc.serial_no=m.serial_no AND m.serial_no=?";

			preparedStmt = innerselectcon.prepareStatement(mySQLQuery);
			preparedStmt.setString(1, serialNoIn);
			rs = preparedStmt.executeQuery();

			if (rs.next()) {

				String serialNoStatus=rs.getString("status_code");				
				if(serialNoStatus.startsWith("VOI")){
					StringBuffer stb = new StringBuffer();
					stb.append("insert into upd.upd_shipment_notavail_sn(SERIAL_NO_IN,SERIAL_NO_OUT,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,STATUS) values(?,?,?,?,?,?,?)");
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



					String MySql_updFactoryShipmentInfo = "update upd.upd_factory_shipment_info set factory_code,gen_date=?,protocol=?,apc=?,trans_model=?,cust_model=?,mkt_model=?,item_code=?,warr_code=?,"
							+ "ship_date=?,ship_to_cust_id=?,ship_to_cust_addr=?,ship_to_cust_name=?,ship_to_cust_city=?,ship_to_cust_country=?,sold_to_cust_id=?,sold_to_cust_name=?,sold_date=?,"
							+ "cit=?,ta_no=?,carton_id=?,po_no=?,so_no=?,fo_sequence=?,msn=?,assign_date=?,gpp_id=?,product_type=?,location_type=?,packing_list=?,fab_date=?,imc_mfg_location=?,)"
							+ "guid=?,pdb_id=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(MySql_updFactoryShipmentInfo);

					if(rs.getString("factory_code")!=null && !(rs.getString("factory_code").equals(""))){
						pstmt1.setString(1, rs.getString("factory_code"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getDate("gen_date")!=null && !(rs.getDate("gen_date").equals(""))){
						pstmt1.setDate(2, rs.getDate("gen_date"));
					}else{
						pstmt1.setDate(2,null);
					}

					if(rs.getString("protocol")!=null && !(rs.getString("protocol").equals(""))){
						pstmt1.setString(3, rs.getString("protocol"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("apc")!=null && !(rs.getString("apc").equals(""))){
						pstmt1.setString(4, rs.getString("apc"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("trans_model")!=null && !(rs.getString("trans_model").equals(""))){
						pstmt1.setString(5, rs.getString("trans_model"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("cust_model")!=null && !(rs.getString("cust_model").equals(""))){
						pstmt1.setString(6, rs.getString("cust_model"));
					}else{
						pstmt1.setString(6,null);	
					}

					if(rs.getString("mkt_model")!=null && !(rs.getString("mkt_model").equals(""))){
						pstmt1.setString(7, rs.getString("mkt_model"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("item_code")!=null && !(rs.getString("item_code").equals(""))){
						pstmt1.setString(8, rs.getString("item_code"));
					}else{
						pstmt1.setString(8,null);	
					}

					if(rs.getString("warr_code")!=null && !(rs.getString("warr_code").equals(""))){
						pstmt1.setString(9, rs.getString("warr_code"));
					}else{
						pstmt1.setString(9,null);
					}
					if(rs.getDate("ship_date")!=null && !(rs.getDate("ship_date").equals(""))){
						pstmt1.setDate(10, rs.getDate("ship_date"));
					}else{
						pstmt1.setDate(10,null);	
					}

					if(rs.getString("ship_to_cust_id")!=null && !(rs.getString("ship_to_cust_id").equals(""))){
						pstmt1.setString(11, rs.getString("ship_to_cust_id"));
					}else{
						pstmt1.setString(11,null);
					}

					if(rs.getString("ship_to_cust_addr")!=null && !(rs.getString("ship_to_cust_addr").equals(""))){
						pstmt1.setString(12, rs.getString("ship_to_cust_addr"));
					}else{
						pstmt1.setString(12,null);	
					}

					if(rs.getString("ship_to_cust_name")!=null && !(rs.getString("ship_to_cust_name").equals(""))){
						pstmt1.setString(13, rs.getString("ship_to_cust_name"));
					}else{
						pstmt1.setString(13,null);
					}

					if(rs.getString("ship_to_cust_city")!=null && !(rs.getString("ship_to_cust_city").equals(""))){
						pstmt1.setString(14, rs.getString("ship_to_cust_city"));
					}else{
						pstmt1.setString(14,null);
					}

					if(rs.getString("ship_to_cust_country")!=null && !(rs.getString("ship_to_cust_country").equals(""))){
						pstmt1.setString(15, rs.getString("ship_to_cust_country"));
					}else{
						pstmt1.setString(15,null);
					}

					if(rs.getString("sold_to_cust_id")!=null && !(rs.getString("sold_to_cust_id").equals(""))){
						pstmt1.setString(16, rs.getString("sold_to_cust_id"));
					}else{
						pstmt1.setString(16,null);
					}

					if(rs.getString("sold_to_cust_name")!=null && !(rs.getString("sold_to_cust_name").equals(""))){
						pstmt1.setString(17, rs.getString("sold_to_cust_name"));
					}else{
						pstmt1.setString(17,null);
					}

					if(rs.getDate("sold_date")!=null && !(rs.getDate("sold_date").equals(""))){
						pstmt1.setDate(18, rs.getDate("sold_date"));
					}else{
						pstmt1.setDate(18,null);
					}

					if(rs.getString("cit")!=null && !(rs.getString("cit").equals(""))){
						pstmt1.setString(19, rs.getString("cit"));
					}else{
						pstmt1.setString(19,null);
					}

					if(rs.getString("ta_no")!=null && !(rs.getString("ta_no").equals(""))){
						pstmt1.setString(20, rs.getString("ta_no"));
					}else{
						pstmt1.setString(20,null);	
					}

					if(rs.getString("carton_id")!=null){
						pstmt1.setString(21, rs.getString("carton_id"));
					}else{
						pstmt1.setString(21,null);	
					}

					if(rs.getString("po_no")!=null && !(rs.getString("po_no").equals(""))){
						pstmt1.setString(22, rs.getString("po_no"));
					}else{
						pstmt1.setString(22,null);	
					}

					if(rs.getString("so_no")!=null && !(rs.getString("so_no").equals(""))){
						pstmt1.setString(23, rs.getString("so_no"));
					}else{
						pstmt1.setString(23,null);
					}

					if(rs.getString("fo_sequence")!=null && !(rs.getString("fo_sequence").equals(""))){
						pstmt1.setString(24, rs.getString("fo_sequence"));
					}else{
						pstmt1.setString(24,null);
					}

					if(rs.getString("msn")!=null && !(rs.getString("msn").equals(""))){
						pstmt1.setString(25, rs.getString("msn"));
					}else{
						pstmt1.setString(25,null);	
					}

					if(rs.getDate("assign_date")!=null && !(rs.getDate("assign_date").equals(""))){
						pstmt1.setDate(26, rs.getDate("assign_date"));
					}else{
						pstmt1.setDate(26,null);
					}

					if(rs.getString("gpp_id")!=null && !(rs.getString("gpp_id").equals(""))){
						pstmt1.setString(27, rs.getString("gpp_id"));
					}else{
						pstmt1.setString(27,null);	
					}

					if(rs.getString("product_type")!=null && !(rs.getString("product_type").equals(""))){
						pstmt1.setString(28, rs.getString("product_type"));
					}else{
						pstmt1.setString(28,null);
					}

					if(rs.getString("location_type")!=null && !(rs.getString("location_type").equals(""))){
						pstmt1.setString(29, rs.getString("location_type"));
					}else{
						pstmt1.setString(29,null);
					}

					if(rs.getString("packing_list")!=null && !(rs.getString("packing_list").equals(""))){
						pstmt1.setString(30, rs.getString("packing_list"));
					}else{
						pstmt1.setString(30,null);	
					}

					if(rs.getDate("fab_date")!=null && !(rs.getDate("fab_date").equals(""))){
						pstmt1.setDate(31, rs.getDate("fab_date"));
					}else{
						pstmt1.setDate(31,null);
					}

					if(rs.getString("imc_mfg_location")!=null && !(rs.getString("imc_mfg_location").equals(""))){
						pstmt1.setString(32, rs.getString("imc_mfg_location"));
					}else{
						pstmt1.setString(32,null);
					}

					if(rs.getString("guid")!=null && !(rs.getString("guid").equals(""))){
						pstmt1.setString(33, rs.getString("guid"));
					}else{
						pstmt1.setString(33,null);
					}

					if(rs.getString("pdb_id")!=null && !(rs.getString("pdb_id").equals(""))){
						pstmt1.setString(34, rs.getString("pdb_id"));
					}else{
						pstmt1.setString(34,null);
					}

					pstmt1.setString(35, serialNoOut);

					boolean status1 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updRepair = "update upd.upd_repair set cancel_code=?,swap_ref_no=?,swap_count=?,delete_flag=?,org_code=?,upd_time=?,dn=?,era=?,rma=?,receive_date=?,"
							+ "delivery_date=?,return_date=?,rma_date=?,scrap_date=?,cust_id=?,warehouse_id=?,shop_id=?,status_id=?,action=?,remarks=?,csn=?,last_repair_date=?,repair_count=?,swap_date=?,)"
							+ "last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(MySql_updRepair);

					if(rs.getString("cancel_code")!=null && !(rs.getString("cancel_code").equals(""))){
						pstmt1.setString(1, rs.getString("cancel_code"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("swap_ref_no")!=null && !(rs.getString("swap_ref_no").equals(""))){
						pstmt1.setString(2, rs.getString("swap_ref_no"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("swap_count")!=null && !(rs.getString("swap_count").equals(""))){
						pstmt1.setString(3, rs.getString("swap_count"));
					}else{
						pstmt1.setString(3,null);	
					}

					if(rs.getString("delete_flag")!=null && !(rs.getString("delete_flag").equals(""))){
						pstmt1.setString(4, rs.getString("delete_flag"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("org_code")!=null && !(rs.getString("org_code").equals(""))){
						pstmt1.setString(5, rs.getString("org_code"));
					}else{
						pstmt1.setString(5,null);	
					}

					if(rs.getString("upd_time")!=null && !(rs.getString("upd_time").equals(""))){
						pstmt1.setString(6, rs.getString("upd_time"));
					}else{
						pstmt1.setString(6,null);	
					}

					if(rs.getString("dn")!=null && !(rs.getString("dn").equals(""))){
						pstmt1.setString(7, rs.getString("dn"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("era")!=null && !(rs.getString("era").equals(""))){
						pstmt1.setString(8, rs.getString("era"));
					}else{
						pstmt1.setString(8,null);
					}

					if(rs.getString("rma")!=null && !(rs.getString("rma").equals(""))){
						pstmt1.setString(9, rs.getString("rma"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getDate("receive_date")!=null && !(rs.getDate("receive_date").equals(""))){
						pstmt1.setDate(10, rs.getDate("receive_date"));
					}else{
						pstmt1.setDate(10,null);
					}

					if(rs.getDate("delivery_date")!=null && !(rs.getDate("delivery_date").equals(""))){
						pstmt1.setDate(11, rs.getDate("delivery_date"));
					}else{
						pstmt1.setDate(11,null);	
					}

					if(rs.getDate("return_date")!=null && !(rs.getDate("return_date").equals(""))){
						pstmt1.setDate(12, rs.getDate("return_date"));
					}else{
						pstmt1.setDate(12,null);
					}

					if(rs.getDate("rma_date")!=null && !(rs.getDate("rma_date").equals(""))){
						pstmt1.setDate(13, rs.getDate("rma_date"));
					}else{
						pstmt1.setDate(13,null);
					}

					if(rs.getDate("scrap_date")!=null && !(rs.getDate("scrap_date").equals(""))){
						pstmt1.setDate(14, rs.getDate("scrap_date"));
					}else{
						pstmt1.setDate(14,null);	
					}

					if(rs.getString("cust_id")!=null && !(rs.getString("cust_id").equals(""))){
						pstmt1.setString(15, rs.getString("cust_id"));
					}else{
						pstmt1.setString(15,null);
					}

					if(rs.getString("warehouse_id")!=null && !(rs.getString("warehouse_id").equals(""))){
						pstmt1.setString(16, rs.getString("warehouse_id"));
					}else{
						pstmt1.setString(16,null);
					}

					if(rs.getString("shop_id")!=null && !(rs.getString("shop_id").equals(""))){
						pstmt1.setString(17, rs.getString("shop_id"));
					}else{
						pstmt1.setString(17,null);	
					}

					if(rs.getString("status_id")!=null && !(rs.getString("status_id").equals(""))){
						pstmt1.setString(18, rs.getString("status_id"));
					}else{
						pstmt1.setString(18,null);
					}

					if(rs.getString("action")!=null && !(rs.getString("action").equals(""))){
						pstmt1.setString(19, rs.getString("action"));
					}else{
						pstmt1.setString(19,null);
					}

					if(rs.getString("remarks")!=null && !(rs.getString("remarks").equals(""))){
						pstmt1.setString(20, rs.getString("remarks"));
					}else{
						pstmt1.setString(20,null);
					}

					if(rs.getString("csn")!=null && !(rs.getString("csn").equals(""))){
						pstmt1.setString(21, rs.getString("csn"));
					}else{
						pstmt1.setString(21,null);	
					}

					if(rs.getDate("last_repair_date")!=null && !(rs.getDate("last_repair_date").equals(""))){
						pstmt1.setDate(22, rs.getDate("last_repair_date"));
					}else{
						pstmt1.setDate(22,null);
					}

					if(rs.getString("repair_count")!=null && !(rs.getString("repair_count").equals(""))){
						pstmt1.setString(23, rs.getString("repair_count"));
					}else{
						pstmt1.setString(23,null);
					}

					if(rs.getDate("swap_date")!=null && !(rs.getDate("swap_date").equals(""))){
						pstmt1.setDate(24, rs.getDate("swap_date"));
					}else{
						pstmt1.setDate(24,null);
					}

					pstmt1.setString(25,serialNoOut);

					boolean status2 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updWarrantyInfo = "update upd.upd_warranty_info set status_code=?,orig_warr_eff_date=?,ren_warr_code=?,warr_country_code=?,warr_region=?,orig_ship_date=?,reference_key=?,"
							+ "pop_in_sysdate=?,pop_date=?,pop_identifier=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(MySql_updWarrantyInfo);

					Date curDate = new Date();
					SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyy");
					String DateToStr = format.format(curDate);


					pstmt1.setString(1, "ACT     " + DateToStr);

					if(rs.getDate("orig_warr_eff_date")!=null && !(rs.getDate("orig_warr_eff_date").equals(""))){
						pstmt1.setDate(2, rs.getDate("orig_warr_eff_date"));
					}else{
						pstmt1.setDate(2,null);
					}

					if(rs.getString("ren_warr_code")!=null && !(rs.getString("ren_warr_code").equals(""))){
						pstmt1.setString(3, rs.getString("ren_warr_code"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("warr_country_code")!=null && !(rs.getString("warr_country_code").equals(""))){
						pstmt1.setString(4, rs.getString("warr_country_code"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("warr_region")!=null && !(rs.getString("warr_region").equals(""))){
						pstmt1.setString(5, rs.getString("warr_region"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("orig_ship_date")!=null && !(rs.getString("orig_ship_date").equals(""))){
						pstmt1.setString(6, rs.getString("orig_ship_date"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("reference_key")!=null && !(rs.getString("reference_key").equals(""))){
						pstmt1.setString(7, rs.getString("reference_key"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getDate("pop_in_sysdate")!=null && !(rs.getDate("pop_in_sysdate").equals(""))){
						pstmt1.setDate(8, rs.getDate("pop_in_sysdate"));
					}else{
						pstmt1.setDate(8, null);	
					}

					if(rs.getDate("pop_date")!=null && !(rs.getDate("pop_date").equals(""))){
						pstmt1.setDate(9, rs.getDate("pop_date"));
					}else{
						pstmt1.setDate(9,null);
					}

					if(rs.getString("pop_identifier")!=null && !(rs.getString("pop_identifier").equals(""))){
						pstmt1.setString(10, rs.getString("pop_identifier"));
					}else{
						pstmt1.setString(10,null);
					}

					pstmt1.setString(11,serialNoOut);

					boolean status3 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updDeviceConfig = "update upd.upd_device_config set handset_type=?,flex_option=?,flex_sw=?,hw=?,icc_id=?,software_version=?,wimax=?,hsn=?,flash_uid=?,dual_serial_no=?,"
							+ "dual_serial_no_type=?,fastt_id=?,base_processor_id=?,wlan=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";


					pstmt1 = con12.prepareStatement(MySql_updDeviceConfig);

					if(rs.getString("handset_type")!=null && !(rs.getString("handset_type").equals(""))){
						pstmt1.setString(1, rs.getString("handset_type"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("flex_option")!=null && !(rs.getString("flex_option").equals(""))){
						pstmt1.setString(2, rs.getString("flex_option"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("flex_sw")!=null){
						pstmt1.setString(3, rs.getString("flex_sw"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("hw")!=null && !(rs.getString("hw").equals(""))){
						pstmt1.setString(4, rs.getString("hw"));
					}else{
						pstmt1.setString(4,null);	
					}

					if(rs.getString("icc_id")!=null && !(rs.getString("icc_id").equals(""))){
						pstmt1.setString(5, rs.getString("icc_id"));
					}else{
						pstmt1.setString(5,null);
					}

					if(rs.getString("software_version")!=null && !(rs.getString("software_version").equals(""))){
						pstmt1.setString(6, rs.getString("software_version"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("wimax")!=null && !(rs.getString("wimax").equals(""))){
						pstmt1.setString(7, rs.getString("wimax"));
					}else{
						pstmt1.setString(7,null);
					}

					if(rs.getString("hsn")!=null && !(rs.getString("hsn").equals(""))){
						pstmt1.setString(8, rs.getString("hsn"));
					}else{
						pstmt1.setString(8,null);
					}

					if(rs.getString("flash_uid")!=null && !(rs.getString("flash_uid").equals(""))){
						pstmt1.setString(9, rs.getString("flash_uid"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getString("dual_serial_no")!=null && !(rs.getString("dual_serial_no").equals(""))){
						pstmt1.setString(10, rs.getString("dual_serial_no"));
					}else{
						pstmt1.setString(10,null);
					}

					if(rs.getString("dual_serial_no_type")!=null && !(rs.getString("dual_serial_no_type").equals(""))){
						pstmt1.setString(11, rs.getString("dual_serial_no_type"));
					}else{
						pstmt1.setString(11,null);
					}

					if(rs.getString("fastt_id")!=null && !(rs.getString("fastt_id").equals(""))){
						pstmt1.setString(12, rs.getString("fastt_id"));
					}else{
						pstmt1.setString(12,null);
					}

					if(rs.getString("base_processor_id")!=null && !(rs.getString("base_processor_id").equals(""))){
						pstmt1.setString(13, rs.getString("base_processor_id"));
					}else{
						pstmt1.setString(13,null);	
					}

					if(rs.getString("wlan")!=null && !(rs.getString("wlan").equals(""))){
						pstmt1.setString(14, rs.getString("wlan"));
					}else{
						pstmt1.setString(14,null);	
					}

					pstmt1.setString(15,serialNoOut);

					boolean status4 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updDirectShipment = "update upd.upd_direct_shipment set so_line_no=?,ds_region_code=?,ds_so_no=?,ds_po_no=?,ds_cust_id=?,ds_bill_to_id=?,ds_ship_to_id=?,ds_cust_country_code=?,"
							+ "ds_cust_name=?,bill_to_id=?,shipment_no=?,phone_no=?,wip_dj=?,sale_date=?,last_imei=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(MySql_updDirectShipment);

					if(rs.getString("so_line_no")!=null && !(rs.getString("so_line_no").equals(""))){
						pstmt1.setString(1, rs.getString("so_line_no"));
					}else{
						pstmt1.setString(1,null);	
					}

					if(rs.getString("ds_region_code")!=null && !(rs.getString("ds_region_code").equals(""))){
						pstmt1.setString(2, rs.getString("ds_region_code"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("ds_so_no")!=null && !(rs.getString("ds_so_no").equals(""))){
						pstmt1.setString(3, rs.getString("ds_so_no"));
					}else{
						pstmt1.setString(3,null);
					}

					if(rs.getString("ds_po_no")!=null && !(rs.getString("ds_po_no").equals(""))){
						pstmt1.setString(4, rs.getString("ds_po_no"));
					}else{
						pstmt1.setString(4,null);
					}

					if(rs.getString("ds_cust_id")!=null && !(rs.getString("ds_cust_id").equals(""))){
						pstmt1.setString(5, rs.getString("ds_cust_id"));
					}else{
						pstmt1.setString(5,null);	
					}

					if(rs.getString("ds_bill_to_id")!=null && !(rs.getString("ds_bill_to_id").equals(""))){
						pstmt1.setString(6, rs.getString("ds_bill_to_id"));
					}else{
						pstmt1.setString(6,null);
					}

					if(rs.getString("ds_ship_to_id")!=null && !(rs.getString("ds_ship_to_id").equals(""))){
						pstmt1.setString(7, rs.getString("ds_ship_to_id"));
					}else{
						pstmt1.setString(7,null);	
					}

					if(rs.getString("ds_cust_country_code")!=null && !(rs.getString("ds_cust_country_code").equals(""))){
						pstmt1.setString(8, rs.getString("ds_cust_country_code"));
					}else{
						pstmt1.setString(8,null);	
					}

					if(rs.getString("ds_cust_name")!=null && !(rs.getString("ds_cust_name").equals(""))){
						pstmt1.setString(9, rs.getString("ds_cust_name"));
					}else{
						pstmt1.setString(9,null);
					}

					if(rs.getString("bill_to_id")!=null && !(rs.getString("bill_to_id").equals(""))){
						pstmt1.setString(10, rs.getString("bill_to_id"));
					}else{
						pstmt1.setString(10,null);
					}

					if(rs.getString("shipment_no")!=null && !(rs.getString("shipment_no").equals(""))){
						pstmt1.setString(11, rs.getString("shipment_no"));
					}else{
						pstmt1.setString(11,null);	
					}

					if(rs.getString("phone_no")!=null && !(rs.getString("phone_no").equals(""))){
						pstmt1.setString(12, rs.getString("phone_no"));
					}else{
						pstmt1.setString(12,null);
					}

					if(rs.getString("wip_dj")!=null && !(rs.getString("wip_dj").equals(""))){
						pstmt1.setString(13, rs.getString("wip_dj"));
					}else{
						pstmt1.setString(13,null);
					}

					if(rs.getDate("sale_date")!=null && !(rs.getDate("sale_date").equals(""))){
						pstmt1.setDate(14, rs.getDate("sale_date"));
					}else{
						pstmt1.setDate(14,null);
					}

					if( rs.getString("last_imei")!=null && !( rs.getString("last_imei").equals(""))){
						pstmt1.setString(15, rs.getString("last_imei"));
					}else{
						pstmt1.setString(15, null);
					}

					pstmt1.setString(16,serialNoOut);

					boolean status5 = pstmt1.execute();

					pstmt1 = null;

					String MySql_updLockCode = "update upd.upd_lock_code set meid_evdo_password=?,meid_a_key2_type=?,meid_a_key2=?,imc_lock_code=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(MySql_updLockCode);

					if(rs.getString("meid_evdo_password")!=null && !(rs.getString("meid_evdo_password").equals(""))){
						pstmt1.setString(1, rs.getString("meid_evdo_password"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("meid_a_key2_type")!=null && !(rs.getString("meid_a_key2_type").equals(""))){
						pstmt1.setString(2, rs.getString("meid_a_key2_type"));
					}else{
						pstmt1.setString(2,null);
					}

					if(rs.getString("meid_a_key2")!=null && !(rs.getString("meid_a_key2").equals(""))){
						pstmt1.setString(3, rs.getString("meid_a_key2"));
					}else{
						pstmt1.setString(3,null);
					}

					if( rs.getString("imc_lock_code")!=null && !( rs.getString("imc_lock_code").equals(""))){
						pstmt1.setString(4, rs.getString("imc_lock_code"));
					}else{
						pstmt1.setString(4,null);
					}

					pstmt1.setString(5,	serialNoOut);

					boolean status6 = pstmt1.execute();

					pstmt1 = null;

					String Sql_updMeid = "update upd.upd_meid set a_key_index=?,cas_no=?,last_mod_datetime=now(),last_mod_by='pcba_pgm_success' where serial_no=?";

					pstmt1 = con12.prepareStatement(Sql_updMeid);

					if(rs.getString("a_key_index")!=null && !(rs.getString("a_key_index").equals(""))){
						pstmt1.setString(1, rs.getString("a_key_index"));
					}else{
						pstmt1.setString(1,null);
					}

					if(rs.getString("cas_no")!=null && !(rs.getString("cas_no").equals(""))){
						pstmt1.setString(2, rs.getString("cas_no"));
					}else{
						pstmt1.setString(2,null);
					}					

					pstmt1.setString(3,serialNoOut);

					boolean status7 = pstmt1.execute();

					pstmt1 = null;

					if (!status1 && !status2 && !status3 && !status4 && !status5
							&& !status6 && !status7) {
						String statusCode = "SCR  " + DateToStr;

						String statusUpdatingOldSerialNo = "update upd.upd_warranty_info set status_code='"+ statusCode+ "' where serial_no='"	+ serialNoIn + "'";
						pstmt1 = con1.prepareStatement(statusUpdatingOldSerialNo);
						pstmt1.execute();
						pstmt1=null;

						String statusOfnewSerialNO="update upd.upd_repair set swap_ref_no='"+serialNoOut+"' where serial_no='"+serialNoIn+"'";
						pstmt1 = con1.prepareStatement(statusOfnewSerialNO);
						pstmt1.execute();

					}

					response.setResponseCode(ServiceMessageCodes.OLD_SN_SUCCESS);
					response.setResponseMessage(ServiceMessageCodes.OPERATION_SUCCESS);

}

			} else {

				response.setResponseCode(""+ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE);
				response.setResponseMessage(ServiceMessageCodes.OLD_SERIAL_NO_NOT_FOUND_IN_WARRANTY_INFO_TABLE_MSG);
				
			}

		} catch (Exception e) {
			con12.rollback();
			e.printStackTrace();
			throw e;
		}

		return con12;

	}

	public Connection updateReferenceTable(String serialNoIn,
			String serialNoOut, Connection con) throws SQLException, Exception {
		Connection innerselectcon = null;
		PreparedStatement pst = null;
		PreparedStatement pstUpdate = null;
		ResultSet rs = null;

		StringBuffer sbuffer = new StringBuffer();
		StringBuffer SQLInnerQuery = new StringBuffer();
		sbuffer.append("SELECT SERIAL_NO,REFERENCE_KEY,STATUS FROM upd.upd_sn_repos_ref WHERE SERIAL_NO=? AND STATUS IS NULL");
		try {
			innerselectcon = DBUtil.getConnection(ds);
			pst = innerselectcon.prepareStatement(sbuffer.toString());
			pst.setString(1, serialNoIn);
			rs = pst.executeQuery();
			if (rs.next()) {

				SQLInnerQuery
				.append("INSERT INTO upd.upd_sn_repos_ref (SERIAL_NO,REFERENCE_KEY,CREATED_BY,CREATED_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME) VALUES (?,?,'pcba_pgm',now(),'pcba_pgm',now())");
				pstUpdate = con.prepareStatement(SQLInnerQuery.toString());
				pstUpdate.setString(1, serialNoOut);
				pstUpdate.setString(2, rs.getString("REFERENCE_KEY"));
				boolean status = pstUpdate.execute();

				if (!status) {

					String updateOldserialNOStatus = "update upd.upd_sn_repos_ref set STATUS='SCR'  where serial_no='"
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
	
	// need to check onces for mySql

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

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in MEID:" + e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG+e.getMessage());

		}

		try {
			// get database connection
			conn1 = DBUtil.getConnection(ds);
			String query="select reference_key from upd.upd_warranty_info where serial_no=?";
			pstmt1 = conn1.prepareStatement(query);
			pstmt1.setString(1,SerialNoIn);
			rs1 = pstmt1.executeQuery();

			logger.info("Status of Serial No:"+query);

			String referenceKey = null;
			String referenceKeyQuery="select count(*) from upd.upd_sn_repos_ref where status is null and reference_key=?";

			if (rs1.next()) {
				referenceKey=rs1.getString("reference_key");
				if(referenceKey!=null && !(referenceKey.equals(""))){

					conn2 = DBUtil.getConnection(ds);
					pstmt2 = conn2.prepareStatement(referenceKeyQuery);
					pstmt2.setString(1, referenceKey);
					rs2 = pstmt2.executeQuery();

					logger.info("Reading Count of Serial No:"+referenceKeyQuery);

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

	
	public String getStatus(String serialNoIn){

		Connection conn1=null;
		PreparedStatement pstmt1=null;
		ResultSet rs1=null;
		String status=null;
		// TODO Auto-generated method stub
		try {

			ds = DBUtil.getMySqlDataSource();
		} catch (NamingException e) {
			logger.info("Data source not found in MEID:" + e);
			response.setResponseCode(""+ServiceMessageCodes.NO_DATASOURCE_FOUND);
			response.setResponseMessage(ServiceMessageCodes.NO_DATASOURCE_FOUND_FOR_SERIAL_NO_MSG+e.getMessage());

		}

		try {
			// get database connection
			conn1 = DBUtil.getConnection(ds);
			String query="select status_code from upd.upd_warranty_info where serial_no=?";
			pstmt1 = conn1.prepareStatement(query);
			pstmt1.setString(1,serialNoIn);
			rs1 = pstmt1.executeQuery();
			if(rs1.next()){
				status =rs.getString("status_code");
			}

			logger.info("Status of Serial No in Shipment:"+query);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DBUtil.closeConnection(conn1, pstmt1, rs1);
		}
		return status;
	}
	public Connection sendEmail(String serialNoIn,String serialNoOut,Connection innerselectcon,PreparedStatement prestmt){
		try{

			StringBuffer stb = new StringBuffer();
			stb.append("insert into upd.shipment_notavail_sn(SERIAL_NO_IN,SERIAL_NO_OUT,CREATED_BY,CREATION_DATETIME,LAST_MOD_BY,LAST_MOD_DATETIME,STATUS) values(?,?,?,?,?,?,?)");
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

			logger.info(" SQL Query:"+stb);

			MailUtil.sendEmail(serialNoIn,serialNoOut);


		}catch(Exception e){
			e.printStackTrace();
		}	
		return innerselectcon;
	}


}
