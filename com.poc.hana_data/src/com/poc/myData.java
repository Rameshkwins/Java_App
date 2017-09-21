/**
 * 
 */
package com.poc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * @author RAMESH
 * 
 */
public class myData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String Hana_Query = "Select top 20 distinct "
				+ "VBELN as OrderNumber, "
				+ "EXTDESC as Status,"
				+ " CA_ORDER_DATE as Carrier_Date, "
				+ "ZZ_CARRIER as CARRIER  "
				+ "FROM \"_SYS_BIC\".\"ztamko.ecc.portal/ZAN_SALESORDER_DETAILS\"";

		System.out.println("Function_invoked");
		get_data(Hana_Query);

	}

	public static void close_connection(Connection con) {
		if (con != null) {
			try {
				con.close();
				System.out.println("Connection closed!");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Connection closed Exception!");
			}
		}
	}

	public static Connection get_connection() {

		Connection connection = null;
		String hanaUserid = "rtamila";
		String hana_Pwd = "passwd";
		String hanaSys = "sapsbxhana.xxxx.com";
		String hanaPort = ":30015";
		String hanaSID = "HX1";
		try {
			connection = DriverManager.getConnection("jdbc:sap://" + hanaSys
					+ hanaPort + "/?databaseName=" + hanaSID, hanaUserid,
					hana_Pwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}

	public static String convertResultSetToJson(ResultSet resultSet)
			throws SQLException {
		JSONArray json = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();

		while (resultSet.next()) // iterate rows
		{
			JSONObject obj = new JSONObject(); // extends HashMap
			for (int i = 1; i <= numColumns; ++i) // iterate columns
			{
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));
			}
			json.add(obj);
		}
		return json.toString();
	}

	public static void get_data(String Hana_Query) {
		ResultSet i_resultSet = null;
		Statement i_statement = null;
		Connection i_connection = null;

		try {
			i_connection = get_connection();
			i_statement = i_connection.createStatement();
//			String SearchbySOldto = "0001001630";
			i_resultSet = i_statement.executeQuery(Hana_Query);
			System.out.print(convertResultSetToJson(i_resultSet));
			i_resultSet.close();
			i_statement.close();

			/**********************************/
		} catch (SQLException e) {
			System.err.println("\nConnection Failed. User/Passwd Error?");
			try {
				i_resultSet.close();
				i_statement.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			return;
		}
		if (i_connection != null) {

			System.out.println("Connection to HANA successful!");
			close_connection(i_connection);
		}

	}

}
