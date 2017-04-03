package logdb;

import general.ExitCode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import sampler.Parameter;
import sampler.StringParameter;
import sampler.AnnealingFunction;

public class LogDB {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static final int NUMBEROFRETRIES = 50;
	private static final int RETRYTIMEMS = 1000; //wait time in ms between sql retries
	private static final String TABLEconfig = "_config";
	private static final String TABLEresults = "_results";
	private static final String TABLEanno = "_annotated_results";
	private static final String TABLEgold = "_goldstandard";
	private Connection c = null;
	private String dbname;
	private String searchType = "MAX";

	//variables used for caching inside one run
	private ArrayList<Long> configCache;
	private double scoreMin = -1.0;
	private double scoreMax = -1.0;


	public LogDB(){
		this("log.db");
	}

	public LogDB(String databasename){
		this.dbname = databasename;
		connect();
		configCache = new ArrayList<>();
	}
	
	private void crash(Exception e){
		logger.log(Level.SEVERE, "Crash in LogDB", e);
		this.disconnect();
	    System.exit(ExitCode.UNKNOWNERROR);
	}
	
	private void connect(){
		if(c != null){
			//do nothing
			return;
		}
		try {
		      Class.forName("org.sqlite.JDBC");
		      c = DriverManager.getConnection("jdbc:sqlite:"+dbname);
		      logger.fine("Opened a new connection to the database.");
		}catch(Exception e){
			crash(e);
		}
	}
	
	private void disconnect(){
		if(c == null){
			//do nothing
			return;
		}
		try {
		      c.close();
		}catch(Exception e){
			logger.log(Level.SEVERE, "Crash in LogDB", e);
			System.exit(ExitCode.UNKNOWNERROR);
		}
	}
	
	/**
	 * Uses a simple statement to execute the given sql update and retries
	 * if an exception occurs
	 * @param sql The sql query that will be executed
	 * @return Returns the number of rows affected by the update
	 */
	private int executeUpdate(String sql){	
		Exception ex = null;
		int affected = -1;
		for(int i=0; i<NUMBEROFRETRIES; i++){
			try{
				connect();
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				affected = stmt.executeUpdate(sql);
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				return affected;
			}catch(Exception e){
				logger.log(Level.WARNING, "SQL String: "+sql, e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (i+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql update query.");
				}
			}
		}
		crash(ex);
		return affected;
	}

	/**
	 * @param sql The sql select query that will be executed
	 * @return An integer value matching the request
	 */
	private int selectInteger(String sql){
		int result = -1337; //dummy value
		Exception ex = null;
		for(int i=0; i<NUMBEROFRETRIES; i++){
			try{
				connect();
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					result = rs.getInt(1);
				}
				rs.close();
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				return result;
			}catch(Exception e){
				logger.log(Level.WARNING, "SQL String: "+sql, e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (i+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql select query.");
				}
			}
		}
		crash(ex);
		return result;
	}

	/**
	 * @param sql The sql select query that will be executed
	 * @return An double value matching the request
	 */
	private double selectDouble(String sql){
		double result = -13.37; //dummy value
		Exception ex = null;
		for(int i=0; i<NUMBEROFRETRIES; i++){
			try{
				connect();
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					result = rs.getDouble(1);
				}
				rs.close();
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				return result;
			}catch(Exception e){
				logger.log(Level.WARNING, "SQL String: "+sql, e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (i+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql select query.");
				}
			}
		}
		crash(ex);
		return result;
	}

	/**
	 * @param sql The sql select query that will be executed
	 * @return An string value matching the request
	 */
	private String selectString(String sql){
		String result = ""; //dummy value
		Exception ex = null;
		for(int i=0; i<NUMBEROFRETRIES; i++){
			try{
				connect();
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					result = rs.getString(1);
				}
				rs.close();
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				return result;
			}catch(Exception e){
				logger.log(Level.WARNING, "SQL String: "+sql, e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (i+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql select query.");
				}
			}
		}
		crash(ex);
		return result;
	}
	
	/**
	 * 
	 * @param stepNumber The number of steps in the path
	 * @param runName The name of the current sampler run
	 * @param includeGold Indicates if a gold table must be created
	 */
	public void prepareRun(int stepNumber, String runName, boolean includeGold){
		// create a new table for the run if none exists
		// user has to keep track of the naming by himself, using the same name for different
		// runs will merge the runs and make the data confuse
		StringBuilder createPlan = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		createPlan.append(runName);
		createPlan.append(TABLEconfig);
		createPlan.append(" (");
		createPlan.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,");
		for(int i=0; i<stepNumber; i++){
			//Stepname will be the name of the edgegrup chosen for this step
			createPlan.append("step");
			createPlan.append(i);
			createPlan.append("_name TEXT NOT NULL, step");
			createPlan.append(i);
			createPlan.append("_id INTEGER NOT NULL,");
		}
		createPlan.append("score REAL DEFAULT 0, failed INTEGER DEFAULT -1);");
		
		StringBuilder createResults = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		createResults.append(runName);
		createResults.append(TABLEresults);
		createResults.append(" (");
		createResults.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,");
		createResults.append("`configId` INTEGER NOT NULL,");
		createResults.append("`chrom` TEXT NOT NULL, ");
		createResults.append("`pos` TEXT NOT NULL, ");
		createResults.append("`content` TEXT ");
		createResults.append(");");

		String createSample = "CREATE TABLE IF NOT EXISTS ";
		createSample += runName;
		createSample += "_sample ( `id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, `configId` INTEGER NOT NULL, score String NOT NULL);";
		
		StringBuilder createAnnotated = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		createAnnotated.append(runName);
		createAnnotated.append(TABLEanno);
		createAnnotated.append(" (");
		createAnnotated.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,");
		createAnnotated.append("`configId` INTEGER NOT NULL,");
		createAnnotated.append("`chrom` TEXT NOT NULL, ");
		createAnnotated.append("`pPos` TEXT NOT NULL, ");
		createAnnotated.append("`type` TEXT NOT NULL, ");
		createAnnotated.append("`trans` TEXT NOT NULL, ");
		createAnnotated.append("`code` TEXT NOT NULL, ");
		createAnnotated.append("`quality` TEXT NOT NULL ");
		createAnnotated.append(");");

		this.executeUpdate(createPlan.toString());
		this.executeUpdate(createResults.toString());
		this.executeUpdate(createAnnotated.toString());
		this.executeUpdate(createSample);
		logger.fine("Tables created.");
				
		
		if(includeGold) createGoldTable(runName);
	}
	
	private void createGoldTable(String runName){
		StringBuilder createResults = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		createResults.append(runName);
		createResults.append(TABLEgold);
		createResults.append(" (");
		createResults.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,");
		createResults.append("`chrom` TEXT NOT NULL, ");
		createResults.append("`pos` TEXT NOT NULL, ");
		createResults.append("`content` TEXT NOT NULL");
		createResults.append(")");
		
		this.executeUpdate(createResults.toString());
	}
	
	/**
	 * Creates a table for the given edgegroup, if none exists yet
	 * @param runName The name of the current sampler run
	 * @param edgeName The name of the edgeGroup
	 * @param paramNames String array containing the parameter names of the edge group
	 */
	public void createEdgeGroup(String runName, String edgeName, String[] paramNames){
		// create a new table for the run if none exists
		StringBuilder createPlan = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		createPlan.append(runName);
		createPlan.append("_step_");
		createPlan.append(edgeName);
		createPlan.append(" (");
		createPlan.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL");
		for(String p : paramNames){
			//Stepname will be the name of the edgegrup chosen for this step
			createPlan.append(", ");
			createPlan.append(p);
			createPlan.append(" TEXT NOT NULL");
		}
		createPlan.append(")");
		
		this.executeUpdate(createPlan.toString());
		logger.fine("Table for edge group "+edgeName+" created.");

	}

	public void addSample(String runName, long configId, double score){
		connect();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(runName);
		sql.append("_sample ( configId, score ) VALUES ");
		sql.append("(");
		sql.append(configId);
		sql.append(", ");
		sql.append(score);
		sql.append(")");

		this.executeUpdate(sql.toString());
	}


	/**
	 * Add the given edge to it's edge group chronic. If the edge has no parameters, no 
	 * entry will be created and 0 is returned. In this context an "edge" is considered
	 * to be all parameter values for a single edge group
	 * @param runName The name of the current sampler run
	 * @param edgeGroupName Name of the current edge group to operate upon
	 * @param values The values of the parameters
	 * @param params Array of parameters to get the column names from
	 * @return Returns the id of the row containing the edge
	 */
	public long addEdge(String runName, String edgeGroupName, String[] values, Parameter[] params){
		// assumes that the user gives the correct number of values in the string
		long id = -1;
		try {
			if (values.length == 0 && params.length == 0) {
				//some edges don't have parameters, return 0 for these
				return 0;
			}
		}catch (NullPointerException npe){
			return 0;
		}
		connect();
		
		//step1: check if the edge already exists
		StringBuilder sql = new StringBuilder("SELECT id FROM ");
		sql.append(runName);
		sql.append("_step_");
		sql.append(edgeGroupName);
		sql.append(" WHERE ");
		String delimit = "";
		for(Parameter p : params){
			sql.append(delimit);
			sql.append(p.getName());
			sql.append("= ?");
			delimit = " AND ";
		}
		sql.append(" LIMIT 1;");
		
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString());
				for(int i=0; i<values.length; i++){
					pstmt.setString(i+1, values[i]);
				}
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()){
					id = rs.getLong(1);
					logger.finer("Found that the edge already exists, returning it's row id ("+id+").");
				}
				rs.close();
				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;
				
			}catch(Exception e){
				logger.log(Level.WARNING, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql select query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		if(id > 0){
			return id;
		}
		
		sql = new StringBuilder("INSERT INTO ");
		StringBuilder val = new StringBuilder(") VALUES (");
		sql.append(runName);
		sql.append("_step_");
		sql.append(edgeGroupName);
		sql.append(" (");
		delimit = "";
		for(Parameter p : params){
			sql.append(delimit);
			sql.append(p.getName());
			val.append(delimit);
			val.append("?");
			delimit = ", ";
		}
		sql.append(val.toString());
		sql.append(")");
		
		ex = null;
		finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
				for(int i=0; i<values.length; i++){
					pstmt.setString(i+1, values[i]);
				}
				int affectedRows = pstmt.executeUpdate();
		
				try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
		            if (generatedKeys.next()) {
		                id = generatedKeys.getLong(1);
		            } else {
		                throw new SQLException("Creating egde entry failed, no ID obtained.");
		            }
		        }
				logger.fine("Added edge "+id+": '"+sql.toString()+"', affected "+affectedRows+" rows.");
				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;
			}catch(Exception e){
				logger.log(Level.SEVERE, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		
		return id;
	}
	
	/**
	 * @param paramValues A pair array of the used parameters with elements {edgeGroupName, edgeId}
	 * @param runName The name of the current sampling process
	 * @return The id of the configuration inside the table
	 */
	public long addConfiguration(ValuePair[] paramValues, String runName){
		long rowId = 0; //dummyValue
		// assumes that the user gives the correct number of values in the string
		connect();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		StringBuilder val = new StringBuilder(") VALUES (");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" (");
		String delimit = "";
		for(int i=0; i<paramValues.length; i++){
			sql.append(delimit);
			sql.append("step");
			sql.append(i);
			sql.append("_name, step");
			sql.append(i);
			sql.append("_id");

			val.append(delimit);
			val.append("?, ?");

			delimit = ", ";
		}
		sql.append(val.toString());
		sql.append(")");

		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
				for(int i=1; i<=paramValues.length; i++){
					pstmt.setString((i*2)-1, paramValues[i-1].getName());
					pstmt.setInt(i*2, Integer.parseInt(paramValues[i-1].getValue()));
				}
				pstmt.executeUpdate();

				ResultSet generatedKeys = pstmt.getGeneratedKeys();
				if (generatedKeys.next()) {
					rowId = generatedKeys.getLong(1);
				} else {
					throw new SQLException("Creating configuration entry failed, no ID obtained.");
				}

				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;

			}catch(Exception e){
				ex = e;
				logger.warning("Adding a new configuration failed at attempt "+(k+1)+"with error: "+e.getMessage());
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		logger.finer("Created new configuration with id "+rowId);

		//add to cache
		configCache.add(rowId);

		return rowId;
	}

	/**
	 * Adds the given list of variants as gold standard to the database
	 * @param runName  The name of the current sampler run
	 * @param vlist List of variants from the gold standard
	 */
	public void addGoldVariant(String runName, ArrayList<Variant> vlist){
		logger.fine("Adding batch variants to the gold standard table, this set contains "+vlist.size()+" variants.");
		connect();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(runName);
		sql.append(TABLEgold);
		sql.append(" ( chrom, pos, content ) VALUES ");
		String seperator = "";
		for(Variant v : vlist){
			sql.append(seperator);
			sql.append("('");
			sql.append(v.getChrom());
			sql.append("', ");
			sql.append(v.getPos());
			sql.append(", '");
			sql.append(v.getContent());
			sql.append("')");
			seperator = ", ";
		}

		int affectedRows = this.executeUpdate(sql.toString());
		logger.fine("Added gold variants, affected "+affectedRows+" rows.");
		
	}
	
	/**
	 * Same as addResultVariant, but adds all variants given in the list
	 * @param runName The name of the current sampler run
	 * @param configId The id of the config that created the result set
	 * @param vlist The list of variants to be added
	 */
	public void addResultVariant(String runName, long configId, ArrayList<Variant> vlist){
		logger.fine("Adding batch variants, the set contains "+vlist.size()+" variants.");
		connect();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(runName);
		sql.append(TABLEresults);
		sql.append(" ( configId, chrom, pos, content ) VALUES ");
		String seperator = "";
		for(Variant v : vlist){
			sql.append(seperator);
			sql.append("(");
			sql.append(configId);
			sql.append(", '");
			sql.append(v.getChrom());
			sql.append("', ");
			sql.append(v.getPos());
			sql.append(", '");
			sql.append(v.getContent());
			sql.append("')");
			seperator = ", ";
		}
		

		int affectedRows = this.executeUpdate(sql.toString());
		logger.fine("Added result variants, affected "+affectedRows+" rows.");
	}
	
	/**
	 * Same as addResultVariant, but adds all variants given in the list
	 * @param runName The name of the current sampler run
	 * @param configId The id of the config that created the result set
	 * @param vlist The list of variants to be added
	 */
	public void addAnnotatedResultVariant(String runName, long configId, ArrayList<String[]> vlist){
		logger.fine("Adding batch variants, the set contains "+vlist.size()+" variants.");
		connect();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(runName);
		sql.append(TABLEanno);
		sql.append(" ( configId, chrom, pPos, type, trans, code, quality ) VALUES ");
		String seperator = "";
		for(String[] v : vlist){
			sql.append(seperator);
			sql.append("(");
			sql.append(configId);
			sql.append(", '");
			sql.append(v[0]);
			sql.append("', '");
			sql.append(v[1]);
			sql.append("', '");
			sql.append(v[2]);
			sql.append("', '");
			sql.append(v[3]);
			sql.append("', '");
			sql.append(v[4]);
			sql.append("', '");
			sql.append(v[5]);
			sql.append("')");
			seperator = ", ";
		}

		int affectedRows = this.executeUpdate(sql.toString());
		logger.fine("Added annotated variants, affected "+affectedRows+" rows.");
	}
	
	/**
	 * Set the fail flag for the given configuration, indicating that
	 * it did not complete the whole pipeline
	 * @param id The id of the configuration that failed
	 * @param runName The name of the current sampler run
	 * @param reason An error code that could be helpful for debugging the workflow (for example the id of the step that failed)
	 */
	public void failConfiguration(long id, String runName, int reason){
		// assumes that the user gives the correct number of values in the string
		connect();
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" SET failed=");
		sql.append(reason);
		sql.append(" WHERE id=");
		sql.append(id);
		logger.info("Failed configuration: "+sql.toString());

		configCache.remove(id);
		
		this.executeUpdate(sql.toString());
	}
	
	/**
	 * Set the result for a configuration that completed the pipeline
	 * @param id Identifier of the configuration in the table
	 * @param score The new score for the configuration
	 */
	public void updateConfiguration(long id, double score, String runName){
		// assumes that the user gives the correct number of values in the string
		connect();
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" SET score='");
		sql.append(score);
		sql.append("', failed=0 WHERE id=");
		sql.append(id);
		logger.info("Update configuration: "+sql.toString());

		//set new min and max scores
		if(score > this.scoreMax){
			this.scoreMax = score;
		}
		if(score < this.scoreMin){
			this.scoreMin = score;
		}
		
		this.executeUpdate(sql.toString());
	}

	/**
	 * Get the score for a given configuration
	 * @param runName The name of the current sampler run
	 * @return The score of the configuration or '-1' if something went wrong(i.e. the configuration failed and has no score)
	 */
	public double getScoreForConfig(String runName, long configId){
		String sql = "SELECT score FROM " + runName + TABLEconfig + " WHERE failed=0 AND id = "+configId;
		double result = this.selectDouble(sql);
		if(result < 0){
			//can happen if configuration does not exist or failed and has no score
			result = -1.0;
		}
		logger.finer("Run '"+runName+"', config "+ configId +" has score "+result);
		return result;
	}

	/**
	 * Search for a configuration that has the same parameter values as a given subset
	 * @param subset A set of {edgeGroupName, edgeId in edgegroup}
	 * @param runName The name of the current sampler run
	 * @return The first configuration in the result set that has a matching subset or -1 if none was found
	 */
	public long containsSubset(ValuePair[] subset, String runName) {
		return containsSubset(subset, runName, false);
	}

	/**
	 * Search for a configuration that has the same parameter values as a given subset
	 * @param subset A set of {edgeGroupName, edgeId in edgegroup}
	 * @param runName The name of the current sampler run
	 * @param includeFailed If true, the query will include failed configurations
	 * @return The first configuration in the result set that has a matching subset or -1 if none was found
	 */
	public long containsSubset(ValuePair[] subset, String runName, boolean includeFailed) {
		long result = -1;
		connect();
		StringBuilder sql = new StringBuilder("SELECT id FROM ");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" WHERE ");
		//select only the subset of parameters
		//stop one element early to deal with the AND-Problem
		String concat = "";
		for(int i=0; i<subset.length; i++){
			sql.append(concat);
			sql.append("step");
			sql.append(i);
			sql.append("_name=? AND step");
			sql.append(i);
			sql.append("_id=?");
			concat = " AND ";
		}
		if(!includeFailed){
			sql.append(" AND failed = 0");
		}
		sql.append(" ORDER BY score DESC LIMIT 1;");
		logger.finest(sql.toString());
		
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString());
				int a = 1; // to count the position for the prepared statement
				for(ValuePair vp : subset){
					pstmt.setString((a++), vp.getName());
					pstmt.setString((a++), vp.getValue());
				}
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()){
					result = rs.getLong(1);
					logger.finest("Found a matching configuration ("+result+") for cache call.");
				}else{
					logger.finest("Empty result for cache call, no previous configs with same subset.");
				}
				rs.close();
				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;

			}catch(Exception e){
				logger.severe("Error while checking database for a subset of configurations.");
				logger.log(Level.WARNING, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		
		return result;
	}
	
	/**
	 * Get the total number of completed configurations for the given run
	 * @param runName The name of the current sampler run
	 * @return The number of (not failed) configurations, or -1 if an error occurred
	 */
	public int getTotalNumberOfConfigurations(String runName){
		if(configCache.size() > 0){
			return configCache.size();
		}
		String sql = "SELECT COUNT(*) FROM " + runName + TABLEconfig + " WHERE failed=0";
		int result = this.selectInteger(sql);
		logger.finer("Run "+runName+" has "+result+" completed configurations.");
		return result;
	}
	
	/**
	 * 
	 * @param runName The name of the current sampler run
	 * @return An list containing the ids of all configurations (that are not marked as failed)
	 */
	public ArrayList<Long> getConfigurations(String runName){
		if(configCache.size() > 0){
			return configCache;
		}
		String sql = "SELECT id FROM " + runName + TABLEconfig + " WHERE failed=0";
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while(rs.next()){
					configCache.add(rs.getLong(1));
				}
				rs.close();
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;

			}catch(Exception e){
				logger.severe("Error while checking database for a list of configurations.");
				logger.log(Level.WARNING, sql, e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}

		logger.finer("Run "+runName+" has "+configCache.size()+" completed configurations.");
		return configCache;
	}
	
	/**
	 * @param runName The name of the current sampler run
	 * @param filter List of variants. The result will only contain only count values for these variants
	 * @return Returns an array list containing the occurrences of variants in the database,
	 * filtered to match the given variants. If the filter is empty or null, all occurrences
	 * for all variants are returned.
	 */
	public ArrayList<Integer> getCommonVariantOccurrences(String runName, ArrayList<Variant> filter){
		ArrayList<Integer> result = new ArrayList<>();
		/* Using one sql query, get the total list of variants that are common
		 * between the database and the variant list, as well as the score for each
		 * unique variant
		 */
		StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ");
		sql.append(runName);
		sql.append(TABLEresults);
		sql.append(" result");

		boolean dropTemp = false;
		String tempName = runName+"_resulttemp";
		if(filter != null && !filter.isEmpty()){
			storeVariantsInTemp(tempName, filter);
			sql.append(", ");
			sql.append(tempName);
			sql.append(" temp WHERE result.chrom = temp.chrom AND result.pos = temp.pos ");
			dropTemp = true;
		}
		sql.append(" GROUP BY result.chrom, result.pos");
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery(sql.toString());
				while(rs.next()){
					result.add(rs.getInt(1));
				}
				rs.close();
				stmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;
			}catch(Exception e){
				logger.severe("Error while checking database for filtered variants.");
				logger.log(Level.WARNING, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		if(filter != null && !filter.isEmpty()) {
			logger.finer("Found " + result.size() + " variants matching the given filter with size "+filter.size());
		}else{
			logger.finer("Found "+result.size()+" variants in total (no filter used).");
		}
		if(dropTemp){
			this.dropTemp(tempName);
		}

		return result;
	}

	/**
	 * @param runName The name of the current sampler run
	 * @param configId
	 * @return
	 */
	public int getCommonVariantSumForConfig(String runName, long configId){
		/* Calculate the sum of values of variant occurences
		 * This means for each variant found by the config, get the number of
		 * occurences of this variant in the result database
		 */
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT COUNT(*) FROM ");
		sql.append(runName);
		sql.append(TABLEresults);
		sql.append(" results, (SELECT chrom, pos FROM ");
		sql.append(runName);
		sql.append(TABLEresults);
		sql.append(" WHERE configId = ");
		sql.append(configId);
		sql.append(") temp WHERE results.chrom = temp.chrom AND results.pos = temp.pos ");
		return this.selectInteger(sql.toString());
	}

	/**
	 * @param runName The name of the current sampler run
	 * @param configId
	 * @return
	 */
	public int getNumberOfVariantsForConfig(String runName, long configId){
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT COUNT(*) FROM ");
		sql.append(runName);
		sql.append(TABLEresults);
		sql.append(" WHERE configId = ");
		sql.append(configId);
		return this.selectInteger(sql.toString());
	}

	
	/**
	 * Get the amount of hits in the gold standard. A hit is of the given variants include one variant with chrom and pos that have
	 * a match in the gold standard variants
	 * @param runName The name of the current sampler run
	 * @param filter List of variants. The result will only contain only count values for these variants
	 * @return Returns an array list containing the occurrences of variants in the database,
	 * filtered to match the given variants. If the filter is empty or null, all occurrences
	 * for all variants are returned.
	 */
	public int getGoldstandardHits(String runName, ArrayList<Variant> filter){
		StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ");
		sql.append(runName);
		sql.append(TABLEgold);
		sql.append(" gold");
		boolean dropTemp = false;
		String tempName = runName+"_goldtemp";
		if(filter != null && !filter.isEmpty()){
			storeVariantsInTemp(tempName, filter);
			sql.append(", ");
			sql.append(tempName);
			sql.append(" temp WHERE gold.chrom = temp.chrom AND gold.pos = temp.pos");
			dropTemp = true;
		}
		int result = selectInteger(sql.toString());
		logger.finer("Found "+result+" variants in the set (no filter).");
		if(dropTemp){
			this.dropTemp(tempName);
		}
		return result;	
	}
	
	
	private void dropTemp(String tempName){
		String sql = "DROP TABLE IF EXISTS "+tempName;
		this.executeUpdate(sql);
		logger.fine("Table "+tempName+" was dropped..");
	}
	
	/**
	 * Creates a temporary table with the given name and the given values for chrom and pos
	 * @param tempName The name of the table
	 * @param variants List of variants that will be inserted into the table
	 */
	private void storeVariantsInTemp(String tempName, ArrayList<Variant> variants){
		logger.fine("Storing variants to a temporary table");
		connect();
		//remove any previous temp tables with the same name that might exist after a crash
		dropTemp(tempName);
		StringBuilder tempTable = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		tempTable.append(tempName);
		tempTable.append(" (");
		tempTable.append("`id` INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,");
		tempTable.append("`name` TEXT NOT NULL UNIQUE, "); //to prevent double variants
		tempTable.append("`chrom` TEXT NOT NULL, ");
		tempTable.append("`pos` TEXT NOT NULL ");
		tempTable.append(")");
		
		this.executeUpdate(tempTable.toString());
		logger.fine("Table "+tempName+" created.");

		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tempName);
		sql.append(" ( name, chrom, pos ) VALUES (?, ?, ?)");

		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				for ( Variant v : variants ) {
					PreparedStatement pstmt = c.prepareStatement(sql.toString());
					String chrom = v.getChrom();
					int pos = v.getPos();
					pstmt.setString(1, chrom + "_" + pos);
					pstmt.setString(2, chrom);
					pstmt.setInt(3, pos);

					try {
						pstmt.executeUpdate();
					}catch(SQLException constraint) {
						/* error code 19 is failed constraint
						 * this will probably be because the name is unique, meaning we tried
						 * to add the same variant again. We ignore this */
						if (constraint.getErrorCode() == 19 || constraint.getErrorCode() == 23) {
							logger.fine("Constraint failed: " + constraint.getMessage());
						} else {
							throw new Exception(constraint.getMessage());
						}
					}
					pstmt.close();
				}
				c.commit();
				c.setAutoCommit(true);
				finished = true;
			}catch (Exception se){
				logger.log(Level.WARNING, sql.toString(), se);
				ex = se;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
	}
	
	/**
	 * Checks if at least one variant is in the result set.
	 * @param runName The name of the current sampler run
	 * @param gold If true, the method will look for variants in the goldset, not in the resultset
	 * @return True if a entry is found, else false
	 */
	public boolean hasVariants(String runName, boolean gold){
		boolean result = false;
		StringBuilder sql = new StringBuilder("SELECT COUNT() FROM ");
		sql.append(runName);
		if(gold){
			sql.append(TABLEgold);
		}else{
			sql.append(TABLEresults);
		}
		sql.append(" LIMIT 1;");
		logger.fine(sql.toString());
		
		int count = this.selectInteger(sql.toString());
		if(count > 0){
			result = true;
		}
		return result;
	}
	
	/**
	 * Deletes all entrys in the result table
	 * @param runName The name of the current sampler run
	 * @param gold If true, the table to delete from is the gold table
	 */
	public void deleteVariants(String runName, boolean gold){
		StringBuilder sql = new StringBuilder("DELETE FROM ");
		sql.append(runName);
		if(gold){
			sql.append(TABLEgold);
		}else{
			sql.append(TABLEresults);
		}
		logger.fine(sql.toString());
		
		int i = this.executeUpdate(sql.toString());
		logger.finer("Query successful, deleted "+i+" rows.");

	}
	
	/**
	 * Retrieve a list of scores for all edge groups available for this step
	 * from the database
	 * @param runName The name of the current sampler run
	 * @param step The id of the step in the workflow
	 * @param previous A list of value pairs containing all decisions previously made for the current path {edgeGroup, edgeId}
	 * @return Array list containing tuples <EdgeGroupName, Score>, ordered by their score from lowest to highest
	 */
	public ArrayList<ValuePair> getEdgeGroupScores(String runName, int step, ArrayList<ValuePair> previous){
		ArrayList<ValuePair> result = new ArrayList<>();
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(searchType);
		sql.append("(score) as ms, step");
		sql.append(step);
		sql.append("_name FROM ");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" WHERE failed=0 ");
		if(previous != null && previous.size() > 0 && step > 1){
			for(int i=0; i<step; i++){
				sql.append(" AND step");
				sql.append(i);
				sql.append("_name = ? AND step");
				sql.append(i);
				sql.append("_id = ? ");
			}
		}
		sql.append(" GROUP BY step");
		sql.append(step);
		sql.append("_name ORDER BY ms ASC");
		
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString());
				if(previous != null && previous.size() > 0 && step > 1){
					for(int i=0; i<step; i++){
						pstmt.setString((2*i)+1, previous.get(i).getName());
						pstmt.setString((2*i)+2, previous.get(i).getValue());
					}
				}
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()){
					ValuePair vp = new ValuePair(rs.getString(2), rs.getString(1));
					try{
						if(Double.parseDouble(vp.getValue()) > 0){
							result.add(vp);
						}
					}catch(Exception e){
						logger.log(Level.WARNING, "Error while creating value pair, resuming action", e);
					}

				}
				rs.close();
				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;
			}catch(Exception e){
				logger.severe("Error while checking database for edgegroup scores");
				logger.log(Level.WARNING, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		return result;
	}

	/**
	 * @param runName The name of the current sampler run
	 * @param step The id of the step
	 * @param previous A list of value pairs containing all decisions previously made for the current path {edgeGroup, edgeId}
	 * @param edgeName The name of the edgegroup
	 * @param p The parameter object to run the query for
	 * @param maxValueId The id of the parameter value up to which the edges are considered
	 * @param temperature
	 * @param currentScore
	 * @return Returns a value pair, containing the number of elements as key and the max scoresum for these
	 * elements as value {#Elements, Max Score}
	 */
	public ValuePair getScoreSumForParamRange(String runName, int step, ArrayList<ValuePair> previous, String edgeName, Parameter p, long maxValueId, double temperature, double currentScore){
		// SELECT the avg scores for each edge for this step matching the filter criteria
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(searchType);
		sql.append("(config.score) AS m FROM ");
		sql.append(runName);
		sql.append(TABLEconfig);
		sql.append(" config LEFT JOIN ");
		sql.append(runName);
		sql.append("_step_");
		sql.append(edgeName);
		sql.append(" step  ON config.step");
		sql.append(step);
		sql.append("_name = ? AND config.step");
		sql.append(step);
		sql.append("_id = step.id WHERE failed=0 AND ");
		if(!(p instanceof StringParameter)){
			sql.append(" CAST( ");
		}
		sql.append("step.");
		sql.append(p.getName());
		if(!(p instanceof StringParameter)){
			sql.append(" as REAL) ");
		}
		sql.append(" >= ? AND ");
		if(!(p instanceof StringParameter)){
			sql.append(" CAST( ");
		}
		sql.append("step.");
		sql.append(p.getName());
		if(!(p instanceof StringParameter)){
			sql.append(" as REAL) ");
		}
		sql.append(" <= ?");
		if(previous != null && previous.size() > 0 && step > 1){
			for(int i=0; i<step; i++){
				sql.append(" AND step");
				sql.append(i);
				sql.append("_name = ? AND step");
				sql.append(i);
				sql.append("_id = ? ");
			}
		}
		sql.append(" GROUP BY step.");
		sql.append(p.getName());
		sql.append(" ORDER BY m ASC");

		int hits = 0;
		double sum = 0.0;
		
		Exception ex = null;
		boolean finished = false;
		for(int k=0; k<NUMBEROFRETRIES && !finished; k++){
			try{
				c.setAutoCommit(false);
				PreparedStatement pstmt = c.prepareStatement(sql.toString());
				pstmt.setString(1, p.getName());
				pstmt.setString(2, p.getMinValue());
				pstmt.setString(3, p.getValue(maxValueId));
				if(previous != null && previous.size() > 0 && step > 1){
					for(int i=0; i<step; i++){
						pstmt.setString(((2*i)+1)+3, previous.get(i).getName());
						pstmt.setString(((2*i)+2)+3, previous.get(i).getValue());
					}
				}

				/* ATTENTION:
				 * The current implementation of the sum calculation requires a "clean" database,
				 * meaning results from previous runs are allowed, but the settings for a parameter
				 * may not change in a way that the new settings have less possibilites than the new
				 * or other parametervalues. This means a change for an imaginary parameter a with the
				 * values {1,2,3} to {1,2,3,4,5,6} is allowed, whereas {1,2} order {1.5,2.5,3.5} is not
				 * allowed
				 */

				ResultSet rs = pstmt.executeQuery();
				int position = 0;
				while(rs.next()) {
					hits += rs.getInt(2);
					ValuePair vp = new ValuePair("null", rs.getString(1));
					sum += AnnealingFunction.getRelativeScoreForElement(vp, temperature, this.getScoreRange(runName), currentScore);
					position++;
				}
				logger.finest("Hits: "+hits+" and sum: "+sum);

				pstmt.close();
				c.commit();
				c.setAutoCommit(true);
				finished = true;
			}catch(SQLException e){
				logger.log(Level.WARNING, sql.toString(), e);
				ex = e;
				try{
					Thread.sleep(RETRYTIMEMS * (k+1));
				}catch(InterruptedException iex){
					logger.finest("Thread was interrupted while waiting for a retry for sql insert query.");
				}
			}
		}
		if(!finished){
			crash(ex);
		}
		
		return new ValuePair(""+hits, sum+"");
		
	}

	public double getScoreRange(String runName){
		double range = 13.37;
		if(this.scoreMax < 0 || this.scoreMin < 0){
			//not loaded yet
			String min = "SELECT MIN(score) FROM "+runName+"_config WHERE failed=0;";
			String max = "SELECT MAX(score) FROM "+runName+"_config WHERE failed=0;";
			this.scoreMin = this.selectDouble(min);
			if(this.scoreMin < 0){
				logger.finest("No scores yet, unable to load min score. Using default range as return value.");
				return range;
			}
			this.scoreMax = this.selectDouble(max);
		}
		return (this.scoreMax - this.scoreMin);
	}
	
}