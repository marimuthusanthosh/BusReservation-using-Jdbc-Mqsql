package JavaJdbcConnectPackage;

import java.math.BigDecimal;
import java.sql.*;

public class Jdbc {
   public static void  main(String[] args) throws Exception
   {  
//     readRecords();
//     insertRecords();
//     readRecords();
//        insertUsingPreparedStatement(50, "hello", "Doe", "Engineering",new BigDecimal("75000.00"), Date.valueOf("2023-11-10"));
//        readRecords();
//        delete(50);
//	     update();
//	   sp();
//        spin();
//	   spout();
//	   commit();
	   batch();
    } 
   public static void readRecords() throws Exception{
	   String url="jdbc:mysql://localhost:3306/jdbcconnection";
	   String userName ="root"; 
	   String passWord ="rockmadhav123";
	   String query ="select * from employees";
	   Connection con =DriverManager.getConnection(url,userName,passWord);
	   Statement st =con.createStatement();
	   ResultSet rs= st.executeQuery(query);
	   while(rs.next())
	   {
	   System.out.println("employee_id is "+rs.getInt(1));
	   System.out.println("first_name "+rs.getString(2));
	   System.out.println("last_name is "+rs.getString(3));
	   System.out.println("department is "+rs.getString(4));
	   System.out.println("salary is "+rs.getBigDecimal(5));
	   System.out.println("employee_id is "+rs.getDate(6));  
	   System.out.println("nextrecord");
	   }
	   con.close();
   }
   public static void insertRecords() throws Exception{
	   String url="jdbc:mysql://localhost:3306/jdbcconnection";
	   String userName ="root"; 
	   String passWord ="rockmadhav123";
	   String query ="INSERT INTO employees VALUES (26,'Santhosh', 'marimuthu', 'Engineering', 1000000.00, '2021-06-15')";
	   Connection con =DriverManager.getConnection(url,userName,passWord);
	   Statement st =con.createStatement();
	   int rows = st.executeUpdate(query);
	   System.out.print(rows);
	   con.close();
   }
   public static void insertUsingPreparedStatement(int employee_id, String first_name, String last_name, 
           String department, BigDecimal salary, Date hire_date) throws Exception {
String url = "jdbc:mysql://localhost:3306/jdbcconnection";
String userName = "root"; 
String passWord = "rockmadhav123";
String query = "INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?)";

// Establish the connection
Connection con = DriverManager.getConnection(url, userName, passWord);
PreparedStatement pst = con.prepareStatement(query);

// Set the values in the prepared statement
pst.setInt(1, employee_id);
pst.setString(2, first_name);
pst.setString(3, last_name);
pst.setString(4, department);
pst.setBigDecimal(5, salary);
pst.setDate(6, hire_date);

// Execute the update
int rowsAffected = pst.executeUpdate();
System.out.println(rowsAffected + " row(s) inserted.");

// Close the resources
pst.close();
con.close();
   }
   public static void delete(int employee_id) throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";
	    String query = "DELETE FROM employees WHERE employee_id = ?";

	    // Establish the connection
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	    PreparedStatement pst = con.prepareStatement(query);

	    // Set the values in the prepared statement
	    pst.setInt(1, employee_id);

	    // Execute the update
	    int rowsAffected = pst.executeUpdate();
	    System.out.println(rowsAffected + " row(s) deleted.");

	    // Close the resources
	    pst.close();
	    con.close();
}
   public static void update() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";  
	    String query = "update employees set first_name ='kinguuu'  WHERE employee_id = 6";

	    // Establish the connection
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	    Statement st = con.createStatement();
	    int rows=st.executeUpdate(query);
        System.out.print("no of updated rows"+rows);
	    // Set the values in the prepared statement
	    
	    con.close();
}
   public static void sp() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";  

	    // Establish the connection
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	     CallableStatement cst = con.prepareCall("{call GetEmp()}");
	    ResultSet rs= cst.executeQuery();
	
	    while(rs.next())
		   {
		   System.out.println("employee_id is "+rs.getInt(1));
		   System.out.println("first_name "+rs.getString(2));
		   System.out.println("last_name is "+rs.getString(3));
		   System.out.println("department is "+rs.getString(4));
		   System.out.println("salary is "+rs.getBigDecimal(5));
		   System.out.println("employee_id is "+rs.getDate(6));  
		   System.out.println("nextrecord");
		   }
	    rs.close();
	    cst.close();
	    con.close();
}
   public static void spin() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";  

	    // Establish the connection
	    int id =3; 
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	     CallableStatement cst = con.prepareCall("{call GetEmpById(?)}");
	      cst.setInt(1,id);
	    ResultSet rs= cst.executeQuery();
	    while(rs.next())
		   {
		   System.out.println("employee_id is "+rs.getInt(1));
		   System.out.println("first_name "+rs.getString(2));
		   System.out.println("last_name is "+rs.getString(3));
		   System.out.println("department is "+rs.getString(4));
		   System.out.println("salary is "+rs.getBigDecimal(5));
		   System.out.println("Date is "+rs.getDate(6));  
		   System.out.println("nextrecord");
		   }
	    rs.close();
	    cst.close();
	    con.close();
}
   public static void spout() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";  

	    // Establish the connection
	    int id = 3; 
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	    
	    // Prepare the callable statement with the correct parameter
	    CallableStatement cst = con.prepareCall("{call GetEmpNameById(?, ?)}"); // Call the procedure with IN and OUT params
	    
	    // Set the IN parameter
	    cst.setInt(1, id); // Set the employee_id (IN parameter)
	    
	    // Register the OUT parameter
	    cst.registerOutParameter(2, Types.VARCHAR); // Register the OUT parameter
	    
	    // Execute the stored procedure
	    cst.executeUpdate();
	    
	    // Get and print the OUT parameter value
	    String employeeName = cst.getString(2); // Get the result of the OUT parameter
	    System.out.println("Employee Name: " + employeeName); // Print the result
	    
	    // Close resources
	    cst.close();
	    con.close();
	}
   public static void commit() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";
	    String query1 = "UPDATE employees SET salary = 500000 WHERE employee_id = 1";
	    String query2 = "UPDATE employees SET salary = 500001 WHERE employee_id = 2";

	    // Establish the connection
	    Connection con = DriverManager.getConnection(url, userName, passWord);

	    // Disable auto-commit to begin the transaction
	    con.setAutoCommit(false);

	    Statement st = con.createStatement();

	    // Execute both updates
	    int rows1 = st.executeUpdate(query1);
	    System.out.println("Rows affected by query1: " + rows1);

	    int rows2 = st.executeUpdate(query2);
	    System.out.println("Rows affected by query2: " + rows2);

	    // to find wheather it is updated 
	    if(rows1>0&&rows2>0)
	      con.commit();
	      System.out.println("Transaction committed successfully.");

	    // Close the connection
	    con.close();
	}
   public static void batch() throws Exception {
	    String url = "jdbc:mysql://localhost:3306/jdbcconnection";
	    String userName = "root"; 
	    String passWord = "rockmadhav123";
	    String query1 = "UPDATE employees SET salary = 500000 WHERE employee_id = 1";
	    String query2 = "UPDATE employees SET salary = 500001 WHERE employee_id = 2";
	    String query3 = "UPDATE employees SET salary = 500002 WHERE employee_id = 3";
	    String query4 = "UPDATE employees SET salary = 500003 WHERE employee_id = 4";

	    // Establish the connection
	    Connection con = DriverManager.getConnection(url, userName, passWord);
	      con.setAutoCommit(false);
	    Statement st = con.createStatement();
         st.addBatch(query1);  
         st.addBatch(query2);
         st.addBatch(query3);
         st.addBatch(query4);
         
         int array[]=st.executeBatch();
         for(int i:array)
         {
        	 if(i>0)
               continue;
        	 else
        	  con.rollback();
         }  
         con.commit();
         con.close(); 
         System.out.print("successfully updated");
	}

   

}  
//Types of statement 
//normal statement--->used to call the create 
//prepared statement-->used to called the prepared statement
//callable statement-->used to call the procedure
