package sampler;

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

import logdb.LogDB;
import logdb.ValuePair;


public class Step {
	protected static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private ArrayList<EdgeGroup> groupList = new ArrayList<>();
	private int ID = -1;
	private LogDB logdb;
	private String runName;

	/**
	 * @param logdb Running instance of logdb
	 * @param runName The name of the current sampling process
	 */
	public Step(LogDB logdb, String runName){
		this.logdb = logdb;
		this.runName = runName;
	}

	public void setId(int value) {
		if(ID < 0){
			ID = value;
		}
	}

	public int getId(){
		return this.ID;
	}
	
	/**
	 * Add a new edge group to this steps list of groups
	 * @param eg The edge group object that will be added
	 */
	public void addEdgeGroup(EdgeGroup eg){
		if(this.groupList.contains(eg)){
			logger.warning("Tried adding edgeGroup "+eg.getGroupName()+" to the step list, but it already exists there.");
		}else{
			//Add edge group to the database for logging purpose
			String[] paramName = new String[eg.getParameterList().length];
			for(int i=0; i<paramName.length; i++){
				paramName[i] =  eg.getParameterList()[i].getName();
			}
			logdb.createEdgeGroup(runName, eg.getGroupName(), paramName);
			
			this.groupList.add(eg);
			logger.finer("Added EdgeGroup "+eg.getGroupName()+" to the step list");
		}
	}

	/**
	 *
	 * @return The number of possible alternatives for the whole step
	 */
	public long getPossibilities() {
		long poss = 0;
		for(EdgeGroup eg : this.groupList){
			poss += eg.getPossibilities();
		}
		return poss;
	}

	/**
	 *
	 * @param target The id of the selected configuration
	 * @param lowerBound lowest id within range
	 * @param upperBound Highest id within range
	 * @return
	 */
	protected Edge chooseNewEdge(long target, long lowerBound, long upperBound){

		long followingOptions = (upperBound - lowerBound +1)/this.getPossibilities();
		logger.finer("Target = "+target+", Options following step "+this.ID+": "+followingOptions);
		long templower = lowerBound;
		long tempupper = upperBound;

		//pick the matching edge group
		int chosenGroup = 0; //default = first element
		if(this.groupList.size() > 1){
			//only choose if there is an actual choice

			for(int i=0; i<groupList.size(); i++){
				EdgeGroup eg = groupList.get(i);
				//search for the group containing the target path
				tempupper = eg.getPossibilities()*followingOptions + templower - 1;
				logger.finer("Current lower: "+templower + ", current upper: "+tempupper+" for edge group "+i);
				if(tempupper >= target){
					chosenGroup = i;
					logger.finer("Chosen group: group "+i);
					break;
				}
				templower = tempupper + 1;
			}
			//edge group has been picked
		}
		
		//for each parameter in the edge group pick the value separately
		Parameter[] plist = groupList.get(chosenGroup).getParameterList();
		ValuePair[] valueList = new ValuePair[plist.length];
		long edgeGroupFollowing = groupList.get(chosenGroup).getPossibilities();
		
		for(int i=0; i<plist.length; i++){
			int edgesAll = plist[i].getNumberOfPossibilities();
			edgeGroupFollowing = edgeGroupFollowing/edgesAll;

			long parameterFollowing = edgeGroupFollowing * followingOptions;
			//using that integer division uses the lower int:
			long m = (target - templower)/parameterFollowing;
			logger.fine("m = (target - templower)/parameterFollowing;\n" + m + " = ("+ target + " - " + templower+")/"+parameterFollowing);

			templower = templower + (m * followingOptions);
			tempupper = templower + followingOptions;

			valueList[i] = new ValuePair(plist[i].getName(), plist[i].getValue(m));
			logger.finest("Choose value "+valueList[i].getValue()+" for "+valueList[i].getName());

		}//end of loop over all parameters
		logger.fine("Choose edge for value list with size "+valueList.length+" (parameter: "+plist.length+")");
		Edge chosenEdge = groupList.get(chosenGroup).getEdgeById(valueList);

		chosenEdge.setLowerBound(templower);
		chosenEdge.setUpperBound(tempupper);
		
		long id = logdb.addEdge(runName, chosenEdge.getGroupName(), chosenEdge.getParamValues(), plist);
		chosenEdge.setId(id);
		return chosenEdge;
	}
	
}
