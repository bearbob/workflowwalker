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

import java.util.logging.Logger;

public class IntegerParameter extends Parameter {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static final String type = "integer";
	private int lowVal;
	private int highVal;
	private int stepSize;
	
	/**
	 * 
	 * @param name The name of the parameter in the configuration
	 * @param lowest Lowest value possible for this parameter
	 * @param highest Highest value possible for this parameter
	 * @param stepSize The distance between two values
	 */
	public IntegerParameter(String name, int lowest, int highest, int stepSize){
		this.name = name;
		this.lowVal = lowest;
		this.highVal = highest;
		this.stepSize = stepSize;
		logger.fine("Created "+type+" parameter "+this.getName());
	}

	@Override
	public String getValue(long id) {
		long val = this.lowVal + (id*stepSize);
		if(val > highVal) val = highVal;
		logger.finer("Parameter "+this.getName()+" return value for ID "+id+": "+val);
		return ""+val;
	}

	@Override
	public int getNumberOfPossibilities() {
		int range = highVal - lowVal;
		int steps = (int)(range/stepSize);
		steps += 1; //first possibility is making no step at all, stay at lowVal
		logger.finer("Number of possibilities for "+type+" parameter "+this.getName()+": "+steps);
		return steps;
	}

	@Override
	public String getMinValue() {
		return this.lowVal + "";
	}

	@Override
	public String getMaxValue() {
		return this.highVal + "";
	}

	@Override
	public int getMaxId() {
		return this.getNumberOfPossibilities() - 1;
	}

	

}
