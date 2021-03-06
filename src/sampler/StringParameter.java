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

public class StringParameter extends Parameter {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static final String type = "string";
	private String[] domain;
	
	/**
	 * String parameters have no stepsize defined because every value is a possible value if it is included in the domain
	 * @param name The name of the parameter in the configuration
	 * @param domain The possible values to pick from
	 */
	public StringParameter(String name, String[] domain){
		this.name=name;
		//FIXME Order the elements in the domain by alphabetical order, starting with a
		this.domain=domain;
		logger.fine("Created "+type+" parameter "+this.getName());
	}

	@Override
	public String getValue(long id) {
		int i = (int) ((id>=domain.length)? (domain.length-1):id);
		String val = domain[i];
		logger.finer("Parameter "+this.getName()+" return value for ID "+id+": "+val);
		return val;
	}

	@Override
	public int getNumberOfPossibilities() {
		int steps = domain.length;
		logger.finer("Number of possibilities for "+type+" parameter "+this.getName()+": "+steps);
		return steps;
	}

	@Override
	public String getMinValue() {
		return domain[0];
	}

	@Override
	public String getMaxValue() {
		return domain[domain.length-1];
	}

	@Override
	public int getMaxId() {
		return domain.length-1;
	}

}
