package sampler;

/*
		% WorkflowWalker: A Workflow Parameter Optimizer
		%
		% Copyright 2017 Björn Groß
		%
		% Licensed under the Apache License, Version 2.0 (the "License");
		% you may not use this file except in compliance with the License.
		% You may obtain a copy of the License at
		%
		%    http://www.apache.org/licenses/LICENSE-2.0
		%
		% Unless required by applicable law or agreed to in writing, software
		% distributed under the License is distributed on an "AS IS" BASIS,
		% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
		% See the License for the specific language governing permissions and
		% limitations under the License.
*/

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import logdb.ValuePair;

public class EdgeGroup {
	protected static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private Parameter[] parameterList;
	private String groupName;
	private String[] inputFiles;
	private String[] outputFiles;
	private String rawCommand;

	/**
	 * 
	 * @param groupName Arbitrary but unique name (handle as identifier)
	 * @param paramList An array of parameters used for this edge group (can be null)
	 * @param inputFiles An array of strings with file names of the input files (can be null)
	 * @param outputFiles An array of strings with file names of the ouput files (can be null)
	 * @param rawCommand The raw command where the parameter values that are samples are replaced with placeholders. The placeholders have to use the same names as the parameter that replaces them. For example, the parameter "seeds" has the placeholder "$#seeds#$"
	 */
	public EdgeGroup(String groupName, Parameter[] paramList, String[] inputFiles, String[] outputFiles, String rawCommand){
		this.setParameterList(paramList);
		if(groupName == null){
			String placeholder = "placeholdername_";
			Random generator = new Random();
			int i = generator.nextInt(255) + 1;
			logger.log(Level.SEVERE, "The group name cannot be empty for an edge group. Critical error. Using random name: " + placeholder);
			groupName = placeholder;
		}
		this.groupName = groupName;
		this.setInputFiles(inputFiles);
		this.setOutputFiles(outputFiles);
		this.setRawCommandDefinition(rawCommand);
		
	}

	private void setParameterList(Parameter[] list) {
		if(list == null){
			//use an empty list
			this.parameterList = new Parameter[]{};
			return;
		}
		this.parameterList = list;
	}

	private void setInputFiles(String[] inputFiles) {
		if(inputFiles == null){
			//use an empty list
			this.inputFiles = new String[]{};
			return;
		}
		this.inputFiles = inputFiles;
	}

	private void setOutputFiles(String[] outputFiles) {
		if(outputFiles == null){
			//use an empty list
			this.outputFiles = new String[]{};
			return;
		}
		this.outputFiles = outputFiles;
	}

	/** Returns the name of the group, which is also
	 * the identifier for the group in the database
	 */
	public String getGroupName(){
		return this.groupName;
	}
	
	/** Array containing file names of all files that are needed
	 * for this edge group to execute the command
	 */
	public String[] getInputFiles(){
		return this.inputFiles;
	}
	
	/** Array containing file names of all files that are created
	 * by this edge group after executing the command
	 */
	public String[] getOutputFiles(){
		return this.outputFiles;
	}

	private void setRawCommandDefinition(String rawCommand){
		if(rawCommand == null){
			//use an empty list
			this.rawCommand = "";
			return;
		}
		this.rawCommand = rawCommand;
	}

	/** Returns the command definition for this group of edges.
	 * With this definition and the parameter settings of an edge
	 * a process can be build and run y replacing the parameter placeholders
	 * @return
	 */
	private String getRawCommandDefinition(){
		// process command should be the same for all edges in this group,
		// the only difference is the parameter setting
		return this.rawCommand;
	}
	
	/**
	 * @return Returns a processed command that can be run at the command line.
	 * Also adds information about the selected edge to the database.
	 */
	private String getUsableCommand(ValuePair[] paramValues){
		String command = getRawCommandDefinition();
		//replace the parameter tokens in the task definition
		if(paramValues == null || paramValues.length == 0){
			return command;
		}
		for(ValuePair vp : paramValues){
			String target = "$#"+vp.getName()+"#$";
			command = command.replace(target, vp.getValue());
		}
		return command;
	}
	
	/**
	 * Creates a edge for the given parameter values with a usable script command
	 * @param values A value pair array containing the values for the parameters
	 * @return Edge object
	 */
	public Edge getEdgeById(ValuePair[] values){
		if(values.length < this.parameterList.length){
			logger.warning("Given value array size does not match the parameter list size for this edge group");
			return null;
		}
		String command = this.getUsableCommand(values);
		String[] valueStrings = new String[values.length];
		for(int i=0; i<values.length; i++){
			valueStrings[i] = values[i].getValue();
		}
		return new Edge(this.getGroupName(), valueStrings, command, this.getInputFiles(), this.getOutputFiles());
	}
	
	public Parameter[] getParameterList(){
		return this.parameterList;
	}
}
