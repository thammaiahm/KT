package com.mot.upd.pcba.utils;

import com.mot.upd.pcba.utils.MEIDException;

/**
 * @author JKXH84-Poonam Mishra
 *
 */
public class MeidUtils {

	 public MeidUtils()
	   {
	   }

	   /**
	    * Returns 18 character decimal representation of the MEID
	    * <p>
	    * The first 8 hex digits are converted to decimal and left padded with zeros
	    * to a length of 10 characters.
	    * The 9-14 hex digits are converted to dicimal and left padded with zeros.
	    * to a length of 8 characters.
	    * The strings are concatenated to return the 18 digit decimal value.
	    * The checksum digit is not converted to decimal.<br>
	    * The validateMEID method is used to validate the input MEID.
	    * @param meid String hexidecimal MEID with or without checksum digit.
	    * @throws MEIDException if input MEID is not valid.
	    * @return String decimal MEID (18 digits)
	    */
	   public static String getDecimalMEID(String meid) throws MEIDException
	   {
	      validateMEID(meid);
	      return hex2dec(meid.substring(0, 8), 10) + hex2dec(meid.substring(8, 14), 8);
	   }

	   /**
	    * Input 18 digit decimal MEID and return the 15 character hexidecimal MEID.
	    * <p>
	    * The validateMEID method is used to validate the input MEID.
	    * @param dmeid String decimal MEID
	    * @throws MEIDException if input decimal MEID is not valid.
	    * @return String hexidecimal MEID including checksum (15 characters)
	    */
	   public static String getHexMEID(String dmeid) throws MEIDException
	   {
	      if (dmeid.length() != 18)
	      {
	         throw new MEIDException("Invalid decimal MEID length: " + dmeid);
	      }

	      return validateMEID(dmeid);
	   }

	   /**
	    * Convert decimal MEID to hexidecimal MEID.  Check that length is 14 chars.
	    * @param dmeid String decimal MEID
	    * @throws MEIDException
	    * @return String hexidecimal MEID
	    */
	   private static String convertDMEIDtoHex(String dmeid) throws MEIDException
	   {
	      String meidA = dec2hex(dmeid.substring(0, 10), 8);
	      String meidB = dec2hex(dmeid.substring(10), 6);
	      if (meidA.length() > 8 || meidB.length() > 6)
	      {
	         throw new MEIDException("Invalid decimal MEID: " + dmeid);
	      }
	      return meidA + meidB;
	   }

	   /**
	    * Validate input hexidecimal or decimal MEID and return the hexidecimal MEID including checksum digit.<p>
	    * Validate MEID is 14 or 15 characters in length, that it starts with at least one of the first two
	    * characters in the range A-F, that it is hexidecimal and the checksum is valid.
	    * <p>
	    * If the input is 14 characters, the checksum is appended.
	    * <p>
	    * Input of 18 digit decimal MEID is also allowed.  The decimal value is
	    * converted to hexidecimal and validated.
	    * <p>
	    * Output hexidecimal value is always returned in uppercase.
	    *
	    * @param meidIn String Hexidecimal MEID with or without checksum or decimal MEID
	    * @throws MEIDException
	    * @return String MEID in hexidecimal format including the checksum digit.
	    */
	   public static String validateMEID(String meidIn) throws MEIDException
	   {
	      if (meidIn == null)
	      {
	         throw new MEIDException("validateMEID: Input MEID is null.");
	      }

	      String meid = meidIn.toUpperCase();
	      try
	      {
	         if (meid.length() == 18)
	         {
	            meid = convertDMEIDtoHex(meid);
	         }

	         if (meid.length() == 15 || meid.length() == 14)
	         {
	 //Modified By Mohd Zafar (TCS) as part of decimal MEID Changes -Start        	 
	        	 if(!isDecMeid(meid))
	        	 {
	        		 int check0 = Integer.parseInt(meid.substring(0, 1), 16);
	        		 int check1 = Integer.parseInt(meid.substring(1, 2), 16);
	        		 if (check0 < 10 && check1 < 10)
	                 {
	                    throw new MEIDException("MEID not within valid prefix range: " +
	                       meid);
	                 }
	             }
//	        	 Modified By Mohd Zafar (TCS) as part of decimal MEID Changes -End            

	            // Calculate checksum
	            String checksum = getChecksum(meid);

	            // If input MEID value did not include checksum, then append and return
	            if (meid.length() == 14)
	            {
	               return meid + checksum;
	            }

	            // Check calculated checksum with input value
	            if (!checksum.equals(meid.substring(14)))
	            {
	               throw new MEIDException("Invalid checksum value for MEID: " + meid +
	                  " Correct checksum: " + checksum);
	            }

	            return meid;
	         }
	         else
	         {
	            throw new MEIDException("Invalid MEID length for " + meid);
	         }
	      }
	      catch (java.lang.NumberFormatException nfe)
	      {
	         throw new MEIDException("Invalid character found in MEID: " + meid);
	      }
	   }

	   /**
	    * Returns a decimal version of the input 'hex' that adds leading zeros to 'len'
	    * @param hex String
	    * @param len int
	    * @return String
	    */
	   private static String hex2dec(String hex, int len)
	   {
	      return zeroPad(Long.toString(Long.parseLong(hex, 16)), len);
	   }

	   /**
	    * Returns a hexidecimal version of the input 'dec' that has leading zeros to 'len'
	    * @param dec String
	    * @param len int
	    * @return String
	    */
	   private static String dec2hex(String dec, int len)
	   {
	      return zeroPad(Long.toHexString(Long.parseLong(dec)).toUpperCase(), len);
	   }

	   /**
	    * Add leading zeros to input 'in' to length 'len'
	    * @param in String
	    * @param len int
	    * @return String
	    */
	   private static String zeroPad(String in, int len)
	   {
	      String prefix = "";
	      for (int i = 0; i < len - in.length(); i++)
	      {
	         prefix += "0";
	      }
	      return prefix + in;
	   }

	   /**
	    * Returns checksum digit for an MEID.
	    * @param meid String hexidecimal MEID
	    * @throws MEIDException if length of input MEID is not 14 or 15.
	    * @return String checksum digit
	    */
	   public static String getChecksum(String meid) throws MEIDException
	   {
		   
		// Modified by Mohd Zafar as part of serial type changes -67423 -START   
	     /* if (meid.length() < 14 || meid.length() > 15)
	      {
	         throw new MEIDException("Invalid MEID length for " + meid);
	      }*/
		   
	      int total = 0;
	      int valAtIdx = -1;
	      int doubleVal = -1;
	      int lenthSerialNo=-1;
	      lenthSerialNo=meid.length();
	// Modified by Mohd Zafar (TCS)  as part of the decimal MEID Changes --start
	      int baseLenChk=-1;
	      int checkSumExpected=-1;
	      if (lenthSerialNo==14)
	    	  baseLenChk=14;
	      else 
	    	  baseLenChk=lenthSerialNo-1;
	     // System.out.println("baseLenChk:"+baseLenChk+ "Serial length : "+lenthSerialNo);
	      if (isDecMeid(meid)&& baseLenChk==14)
	      {
	    	  //System.out.println("In Side Decimal Check :"+baseLenChk+ "Serial length : "+lenthSerialNo);
	    	// Modified by Mohd Zafar as part of serial type changes -67423 -END
	    	  for (int i = 0; i < baseLenChk; i++)
	    	  {
	    		  valAtIdx = Integer.parseInt(meid.substring(i, i + 1), 10);
	    		  if ( (i % 2) != 0)
	    		  {
	    			  doubleVal = valAtIdx * 2;
	    			  total += doubleVal / 10;
	    			  total += doubleVal % 10;
		          }
	    		  else
		            total += valAtIdx;
		      }

	      checkSumExpected = 10 - (total % 10);
	      if (checkSumExpected == 10)
	         checkSumExpected = 0;
	    	  
	      }
	      else   	  
	      {
	    	// Modified by Mohd Zafar (TCS)  as part of the decimal MEID Changes --End   
	    	//  System.out.println(" Inside Else of decimal  baseLenChk:"+baseLenChk+ "Serial length : "+lenthSerialNo);
	    	  for (int i = 0; i < baseLenChk; i++)
	    	  {
	    		  valAtIdx = Integer.parseInt(meid.substring(i, i + 1), 16);
	    		  if ( (i % 2) != 0)
	    		  {
	    			  doubleVal = valAtIdx * 2;
	    			  total += doubleVal / 16;
	    			  total += doubleVal % 16;
		          }
	    		  else
		            total += valAtIdx;
		      }

	          checkSumExpected = 16 - (total % 16);
	      if (checkSumExpected == 16)
	         checkSumExpected = 0;
	      }
	      return Integer.toHexString(checkSumExpected).toUpperCase();
	   }

	   /**
	    * Returns full 15 character hexidecimal MEID, appending the checksum digit if needed.
	    * @param meid String
	    * @throws MEIDException if length of input MEID is not 14 or 15.
	    * @return String
	    */
	   private static String getMeidChecksum(String meid) throws MEIDException
	   {
	      if (meid.length() == 15)
	      {
	         return meid;
	      }
	      return meid + getChecksum(meid);
	   }

	   /**
	    * For testing purposes only.
	    * @param args String[]
	    */
	   
	   //Modified by Mohd Zafar(TCS) as part of Decimal MEid pahse 2 project 66972- START
	   public static boolean isDecMeid(String meid)
		{
			boolean isDec=true;
			
			for(int i=0;i<meid.length();i++)
			{
				char valAtIndex=meid.charAt(i);
				if((valAtIndex>=65&&valAtIndex<=70)||(valAtIndex>=97&&valAtIndex<=102))
				{
					//System.out.println("Its a HEX value");
					isDec=false;
					break;
				}
			}
			return isDec;
		}
	   //Modified by Mohd Zafar(TCS) as part of Decimal MEid pahse 2 project 66972- END
	   
	   public static void main(String[] args)
	   {
	      try
	      {
	         System.out.println("********************************");
	         System.out.println("\nInvalid MEID length test >>");
	         try
	         {
	            MeidUtils.validateMEID("12345");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage());
	         }

	         System.out.println("\n********************************");
	         System.out.println("\nInvalid MEID prefix test >>");
	         try
	         {
	            MeidUtils.validateMEID("12345678901234");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }

	         System.out.println("\n********************************");
	         System.out.println("\nInvalid character test >>");
	         try
	         {
	            MeidUtils.validateMEID("A012345678901G");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }

	         System.out.println("\n********************************");
	         System.out.println("\nInvalid checksum test >>");
	         try
	         {
	            MeidUtils.validateMEID("A01234567890120");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");
	         System.out.println("\nInvalid length of decimal MEID");
	         try
	         {
	            MeidUtils.validateMEID("2684354559999999991");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");
	         System.out.println("\nDecimal MEID value too low");
	         try
	         {
	            MeidUtils.validateMEID("268435455999999999");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");
	         System.out.println("\nDecimal MEID value too high");
	         try
	         {
	            MeidUtils.validateMEID("429496729516777216");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");
	         System.out.println("\nDecimal MEID value too high");
	         try
	         {
	            MeidUtils.validateMEID("429496729616777215");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");
	         System.out.println("\nDecimal MEID value too low");
	         try
	         {
	            MeidUtils.validateMEID("000000000016777215");
	            System.err.println("Should not get here!");
	         }
	         catch (MEIDException me)
	         {
	            System.out.println("<Expected error> Message: " + me.getMessage()); ;
	         }
	         System.out.println("\n********************************");

	         String[] meidList =
	            {"A0123456123450", "A0123456123451", "A0123456123452", "A0123456123453",
	            "A0123456123454", "A0123456123455", "A0123456123456", "A0123456123457",
	            "A0123456123458", "A0123456123459", "A012345612345A", "A012345612345B",
	            "A012345612345C", "A012345612345D", "A012345612345E", "A012345612345F"};
	         /*   meidList = new String[]
	               {"A0000000000000", "a0ffffff000000", "a0ffffffffffff", "A23456789ABCDE2",
	               "FF000000FFFFFF", "FFFFFFFFFFFFFF"};
	//*/
	         System.out.println("");
	         for (int i = 0; i < meidList.length; i++)
	         {
	            System.out.println("********************************");
	            System.out.println("MEID: " + meidList[i] + "(" +
	               MeidUtils.validateMEID(meidList[i]) + ")");
	            System.out.println("Checksum:    " + MeidUtils.getChecksum(meidList[i]));
	            System.out.println("Decimal MEID: " + MeidUtils.getDecimalMEID(meidList[i]));
	            System.out.println("Dec to Hex: " +
	               MeidUtils.validateMEID(MeidUtils.getDecimalMEID(meidList[i])));
	         }

	         System.out.println("\n<MEID>,<Decimal MEID>");
	         for (int i = 0; i < meidList.length; i++)
	         {
	            System.out.println(
	               meidList[i].substring(0,
	               14).toUpperCase() + MeidUtils.getChecksum(meidList[i]) + "," +
	               MeidUtils.getDecimalMEID(meidList[i]));
	         }
	      }
	      catch (MEIDException me)
	      {
	         me.printStackTrace();
	      }
	   }
	
}
