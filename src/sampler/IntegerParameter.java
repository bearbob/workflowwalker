package sampler;

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

	

}
