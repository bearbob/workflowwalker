package sampler;

import java.util.logging.Logger;

public class Edge {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private String groupName;
	private String[] paramValues;
	private long edgeId;
	private String command;
	private String[] inputFiles;
	private String[] outputFiles;
	
	/**
	 * Creates a new edge object
	 * @param name The name of the edge group this edge belongs to
	 * @param command The executable command with parameter values
	 * @param inputs Array of file names of files that are needed before execution
	 * @param outputs
	 */
	public Edge(String name, String[] paramValues, String command, String[] inputs, String[] outputs){
		this.setGroupName(name);
		this.setCommand(command);
		this.setInputFiles(inputs);
		this.setOutputFiles(outputs);
		this.setParamValues(paramValues);
		logger.finest("Created edge for "+name+", internal values are "+this.getGroupName()+"("+this.edgeId+")");		
	}
	
	/**
	 * ID of the edge inside the database
	 * @return
	 */
	public long getId(){
		return this.edgeId;
	}
	
	public void setId(long id){
		this.edgeId = id;
	}
	
	public String getIdAsString(){
		return ""+this.edgeId;
	}

	public String[] getInputFiles() {
		return inputFiles;
	}

	private void setInputFiles(String[] inputFiles) {
		this.inputFiles = inputFiles;
	}

	/**
	 * @return The name of the edge group this edge belongs to
	 */
	public String getGroupName() {
		return groupName;
	}

	private void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	
	/** Returns the executable command for this edge */
	public String getCommand() {
		return command;
	}

	private void setCommand(String command) {
		this.command = command;
	}

	public String[] getOutputFiles() {
		return outputFiles;
	}

	private void setOutputFiles(String[] outputFiles) {
		this.outputFiles = outputFiles;
	}

	public String[] getParamValues() {
		return paramValues;
	}

	private void setParamValues(String[] paramValues) {
		this.paramValues = paramValues;
	}
	
}
