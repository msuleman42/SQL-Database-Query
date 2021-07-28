// Munir Suleman

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;

public class JabberServer {
	
	private static String dbcommand = "jdbc:postgresql://127.0.0.1:5432/postgres";
	private static String db = "postgres";
	private static String pw = "password"; //password

	private static Connection conn;
	
	public static Connection getConnection() {
		return conn;
	}

	public static void main(String[] args) {
				
		JabberServer jabber = new JabberServer();
		JabberServer.connectToDatabase();
		jabber.dropTables(); 	
		
		//ArrayList<String> result = new ArrayList<String>();
		//ArrayList<ArrayList<String>> result1 = new ArrayList<ArrayList<String>>();
		
		//result =  jabber.getFollowerUserIDs(0);
		//result = jabber.getFollowingUserIDs(0);
		//result1 = jabber.getLikesOfUser(0);
		//result1 = jabber.getTimelineOfUser(1);
		//result1 = jabber.getMutualFollowUserIDs();
		//result = jabber.getUsersWithMostFollowers();
		//jabber.addUser("therock", "something@something.com");
		//jabber.addJab("ellie", "oh hi there");
		//jabber.addFollower(12, 1);
		//jabber.addLike(7, 1);
		
		//System.out.println(jabber.nextUserID());
		//System.out.println(jabber.nextJabID());
		//print2(result1);
		//print1(result);
	}
	
	public ArrayList<String> getFollowerUserIDs(int userid) {
		// returns a list of the userids (as Strings) of the jabberusers that follow the user with the userid 'userid'

		ArrayList<String> followerUserIDs = new ArrayList<String>();
		
		String query = "SELECT userida FROM follows WHERE useridb = " + userid + ";";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while (rset.next()) {
				followerUserIDs.add(rset.getString("userida"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return followerUserIDs;
	}

	public ArrayList<String> getFollowingUserIDs(int userid) {
		// returns a list of the userids of the jabberusers that the user with the userid 'userid' is following
		
		ArrayList<String> followingUserIDs = new ArrayList<String>();
		
		String query = "SELECT useridb FROM follows WHERE userida = " + userid + ";";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while (rset.next()) {
				followingUserIDs.add(rset.getString("useridb"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		return followingUserIDs;
	}
	
	public ArrayList<ArrayList<String>> getLikesOfUser(int userid) {
		// returns a list of ArrayLists with the username and jabtext of all jabs liked by the user 'userid'. 
		// The username is the username of the user who posted the jab.
		
		ArrayList<ArrayList<String>> likesOfUser = new ArrayList<ArrayList<String>>();
		
		String query = "SELECT username, jabtext FROM likes NATURAL JOIN (SELECT jabid, username, jabtext FROM jab"
						+ " NATURAL JOIN jabberuser) AS a1 WHERE userid = " + userid + ";";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while(rset.next()) {
				ArrayList<String> oneLine = new ArrayList<String>();
				oneLine.add(rset.getString("username"));
				oneLine.add(rset.getString("jabtext"));
				likesOfUser.add(oneLine);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return likesOfUser;
	}
	
	public ArrayList<ArrayList<String>> getTimelineOfUser(int userid) {
		// returns the timeline of a user. A user's timeline is all of the jabs posted by users they follow.
		// Each row of the result should be the username of the user who posted the jab and the jab text.
		
		ArrayList<ArrayList<String>> timelineOfUser = new ArrayList<ArrayList<String>>();
		
		String query = "SELECT username, jabtext FROM (SELECT * FROM jab NATURAL JOIN jabberuser) AS a1 JOIN "
				       + "(SELECT * FROM follows where userida = " + userid + ") AS a2 on a1.userid = a2.useridb;";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while(rset.next()) {
				ArrayList<String> oneLine = new ArrayList<String>();
				oneLine.add(rset.getString("username"));
				oneLine.add(rset.getString("jabtext"));
				timelineOfUser.add(oneLine);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return timelineOfUser;
	}
	
	public ArrayList<ArrayList<String>> getMutualFollowUserIDs() {
		// returns a list of ArrayLists, each containing a pair of mutual follows
		
		ArrayList<ArrayList<String>> mutualFollowerUserIDs = new ArrayList<ArrayList<String>>();
		
		String query = "SELECT a1.userida, a1.useridb FROM follows AS a1, (SELECT * FROM follows) AS a2 WHERE a1.userida = a2.useridb AND "
						+ "a1.useridb = a2.userida AND a1.userida < a1.useridb ORDER BY userida;";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while(rset.next()) {
				ArrayList<String> oneLine = new ArrayList<String>();
				oneLine.add(rset.getString("userida"));
				oneLine.add(rset.getString("useridb"));
				mutualFollowerUserIDs.add(oneLine);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return mutualFollowerUserIDs;
	}
	
	public void addUser(String username, String emailadd) {
		// adds a new user to the jabberuser table with username 'username' and email address 'emailadd'
		
		int newUserID = nextUserID();
		
		Statement stmt;
		
		String query = "INSERT INTO jabberuser VALUES(" + newUserID + ", '" + username + "', '" + emailadd + "');";
		
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void addJab(String username, String jabtext) {
		// adds a new jab to the jab table. The user is represented by 'username' and the jab by 'jabtext'
		
		int userID = -1;
		String query = "SELECT userid FROM jabberuser WHERE username = '" + username + "';";
		
		ResultSet rset = executeThisQuery(query);
		
		try {
			while(rset.next()) {
				userID = rset.getInt("userid");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int newJabID = nextJabID();
		Statement stmt;
		
		String query1 = "INSERT INTO jab VALUES(" + newJabID + ", " + userID + ", '" + jabtext + "');";
		
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void addFollower(int userida, int useridb) {
		// adds a new follows relationship: userida follows useridb
		Statement stmt;
		
		String query = "INSERT INTO follows VALUES(" + userida + ", " + useridb + ");";
		
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void addLike(int userid, int jabid) {
		// adds a new like: user 'userid' likes jab 'jabid'.
		
		Statement stmt;
		
		String query = "INSERT INTO likes VALUES(" + userid + ", " + jabid + ");";
		
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public ArrayList<String> getUsersWithMostFollowers() {
		// returns the userids of the user(s) with the most followers
		
		ArrayList<String> usersWithMostFollowers = new ArrayList<String>();
		
		String query = "SELECT useridb FROM follows GROUP BY useridb HAVING count(useridb) >= ALL (SELECT count(useridb) FROM follows "
				        + "GROUP BY useridb ORDER BY useridb DESC);";
		ResultSet rset = executeThisQuery(query);
		
		try {
			while (rset.next()) {
				usersWithMostFollowers.add(rset.getString("useridb"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return usersWithMostFollowers;
	}
	
	//------------------------------------------------------------------------------------------
	
	//This method return executes a query and returns a ResultSet
	public static ResultSet executeThisQuery(String query) {
		ResultSet rset = null;
		try {
			Statement stmt = conn.createStatement();
			rset = stmt.executeQuery(query);

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rset;
	}
	
	public int nextUserID() {
		// method returns the next usid to be used
		int maxID = -1;
		String query = "SELECT MAX(userid) FROM jabberuser;";
		
		ResultSet rset = executeThisQuery(query);
		
		try {
			while (rset.next()) {
				maxID = rset.getInt("max");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(maxID.intValue());
		int newUserID = maxID + 1;
		return newUserID;
	}
	
	public int nextJabID() {
		// method returns the next usid to be used
		int maxID = -1;
		String query = "SELECT MAX(jabid) FROM jab;";
		
		ResultSet rset = executeThisQuery(query);
		
		try {
			while (rset.next()) {
				maxID = rset.getInt("max");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(maxID.intValue());
		int newJabID = maxID + 1;
		return newJabID;
	}
	
	//  CODE BEYOND THIS POINT IS NOT WRITTEN BY ME------------------------------------------------------------------------------------
	public JabberServer() {}
	
	public static void connectToDatabase() {

		try {
			conn = DriverManager.getConnection(dbcommand,db,pw);

		}catch(Exception e) {		
			e.printStackTrace();
		}
	}

	/*
	 * Utility method to print an ArrayList of ArrayList<String>s to the console.
	 */
	private static void print2(ArrayList<ArrayList<String>> list) {
		
		for (ArrayList<String> s: list) {
			print1(s);
			System.out.println();
		}
	}
		
	/*
	 * Utility method to print an ArrayList to the console.
	 */
	private static void print1(ArrayList<String> list) {
		
		for (String s: list) {
			System.out.print(s + " ");
		}
	}

	public void resetDatabase() {
		
		dropTables();
		
		ArrayList<String> defs = loadSQL("jabberdef");
	
		ArrayList<String> data =  loadSQL("jabberdata");
		
		executeSQLUpdates(defs);
		executeSQLUpdates(data);
	}
	
	private void executeSQLUpdates(ArrayList<String> commands) {
	
		for (String query: commands) {
			
			try (PreparedStatement stmt = conn.prepareStatement(query)) {
				stmt.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private ArrayList<String> loadSQL(String sqlfile) {
		
		ArrayList<String> commands = new ArrayList<String>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(sqlfile + ".sql"));
			
			String command = "";
			
			String line = "";
			
			while ((line = reader.readLine())!= null) {
				
				if (line.contains(";")) {
					command += line;
					command = command.trim();
					commands.add(command);
					command = "";
				}
				
				else {
					line = line.trim();
					command += line + " ";
				}
			}
			
			reader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return commands;
		
	}

	private void dropTables() {
		
		String[] commands = {
				"drop table jabberuser cascade;",
				"drop table jab cascade;",
				"drop table follows cascade;",
				"drop table likes cascade;"};
		
		for (String query: commands) {
			
			try (PreparedStatement stmt = conn.prepareStatement(query)) {
				stmt.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
