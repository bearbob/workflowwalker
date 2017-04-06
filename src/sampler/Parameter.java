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

public abstract class Parameter {
	/** The name of the parameter */
	protected String name;

	/**
	 * @return The name of this parameter as string value
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * @param id
	 * @return Returns a string representation of the value with the given id
	 */
	abstract public String getValue(long id);
	
	/**
	 * @return Returns the string representation of the lowest possible value for this parameter
	 */
	abstract public String getMinValue();
	
	/**
	 * @return Returns the string representation of the highest possible value for this parameter
	 */
	abstract public String getMaxValue();

	/**
	 * @return Returns the id representation of the highest possible value for this parameter
	 */
	abstract public int getMaxId();
	
	/**
	 * @return Returns the number of possible values for this parameter.
	 * Therefore the number of possible values must be finite.
	 */
	abstract public int getNumberOfPossibilities();

}
