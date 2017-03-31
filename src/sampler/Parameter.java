package sampler;

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
