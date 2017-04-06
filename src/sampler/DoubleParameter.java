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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.logging.Logger;

public class DoubleParameter extends Parameter {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private double lowVal;
	private double highVal;
	private double stepSize;
	private int decimalDigits = 0;
	private static final String type = "double";
	
	/**
	 * 
	 * @param name The name of the parameter in the configuration
	 * @param lowest Lowest value possible for this parameter
	 * @param highest Highest value possible for this parameter
	 * @param stepSize The distance between two values
	 */
	public DoubleParameter(String name, double lowest, double highest, double stepSize){
		this.name = name;
		this.lowVal = lowest;
		this.highVal = highest;
		this.stepSize = stepSize;
		logger.fine("Created "+type+" parameter "+this.getName());
		
		/* check what has more decimal digits - the base value or the step size
		 * and use the larger one as reference when generating values later on
		 */
		String text = Double.toString(Math.abs(stepSize));
		int decimalsStep = text.length() - (text.indexOf('.') + 1);
		text = Double.toString(Math.abs(lowVal));
		int decimalsLow = text.length() - (text.indexOf('.') + 1);
		this.decimalDigits = (decimalsLow > decimalsStep)? decimalsLow : decimalsStep;
	}
	
	/**
	 * rounds a double up to the given number of decimal places
	 * @param value The full double value
	 * @param places How many places after the delimiter?
	 * @return Returns a double with limited places after the delimiter
	 */
	private static double round(double value, int places) {
	    if (places < 0) return value;

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}

	@Override
	public String getValue(long id) {
		double value = this.lowVal + (id*stepSize);
		//there can be an error with the decimal digits, fix this here
		//this is based on the assumption that we cannot have more decimal digits than the stepsize or the base value,
		// if we just use subtraction and addition
		double val = round(value, this.decimalDigits);
		
		if(val > highVal) val = highVal;
		logger.finer("Parameter "+this.getName()+" return value for ID "+id+": "+val);
		return ""+val;
	}

	@Override
	public int getNumberOfPossibilities() {
		double range = highVal - lowVal;
		int steps = (int)(range/stepSize);
		steps += 1; //first possibility is no step at all, stay at lowVal
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
