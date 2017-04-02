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
	 * Chooses a new edge randomly from all edges in this step
	 * @param previous A list of edges containing all decisions previously made for the current path
	 * @param temperature The current value of the temperature variable. The higher the temperature, the more the probabilities will drift from each other
	 * @return The chosen edge
	 */
	protected Edge chooseNewEdge(ArrayList<Edge> previous, double temperature){
		// get the information about previous decisions and save it in value pairs
		ArrayList<ValuePair> history;
		if(previous != null && previous.size() > 0){
			history = new ArrayList<>();
			for( Edge e : previous ){
				history.add(new ValuePair(e.getGroupName(), String.valueOf(e.getId())));
			}
		}else {
			history = null;
		}

		/*
		 * Construct the new edge for this step. This edge is created from many smaller edges, one for each parameter.
		 * But first: pick one of the available edge groups randomly, weighted by their best score
		 */
		int chosenGroup = 0; //default = first element
		Random r = new Random();
		double randomValue = r.nextDouble();
		if(this.groupList.size() > 1){
			//only choose if there is an actual choice
			double pointer = 0.0;
			ArrayList<ValuePair> groupScores = logdb.getEdgeGroupScores(runName, ID, history);
			//get the total sum of scores for all edge groups that have been visited
			double groupSum = Temperature.getRelativeScoreSum(groupScores, temperature);

			//create temporary list to remove elements from, to make the selection faster for many groups
			ArrayList<ValuePair> tempScores = new ArrayList<>();
			tempScores.addAll(groupScores);
			groups:for(int i=0; i<groupList.size(); i++){
				//search for the group score of group i
				boolean visited = false;
				int j;
				for(j=0; j<tempScores.size(); j++){
					ValuePair vp = tempScores.get(j);
					if(vp.getName().equals(groupList.get(i).getGroupName())){
						//P = ownScore/AllVisitedScore * numVisited/all
						double score = Temperature.getRelativeScoreForElement(vp, temperature, j, groupScores.size());
						pointer += (score/groupSum) * ((double)groupScores.size()/groupList.size());
						visited = true;
						//dont search further trough the group scores
						break;
					}
				}
				if(!visited) {
					// add the probability if edge has not been visited
					pointer += 1.0 / groupList.size();
				}else{
					tempScores.remove(j);
				}
				if(pointer > randomValue){
					chosenGroup = i;
					break groups;
				}
			}
			//edge group has been picked
		}
		
		//for each parameter in the edge group pick the value separately
		Parameter[] plist = groupList.get(chosenGroup).getParameterList();
		ValuePair[] valueList = new ValuePair[plist.length];
		
		paramLoop:for(int i=0; i<plist.length; i++){
			//start binary search for this parameter
			randomValue = r.nextDouble();
			// amount of all edges for this decision
			int edgesAll = plist[i].getNumberOfPossibilities();
			if(edgesAll < 2){
				//if not even two edges exist, there is no need to randomly choose
				valueList[i] = new ValuePair(plist[i].getName(), plist[i].getValue(0));
				continue paramLoop;
			}
			// value pair <Amount of visited edges, maximum score sum for these edges>
			ValuePair vpAll = logdb.getScoreSumForParamRange(runName, ID, history,
					groupList.get(chosenGroup).getGroupName(), plist[i], plist[i].getMaxId(), temperature);
			
			int visitedAll = Integer.parseInt(vpAll.getName());
			double scoreAll = Double.parseDouble(vpAll.getValue());
			double visitedMultiply = ((edgesAll * scoreAll)>0) ? (double)visitedAll/(edgesAll * scoreAll) : 0.0;
			long left = 0;
			long right = edgesAll -1;
			long m;
			double compare;
			logger.finest("Started binary search");
			binarySearch:while(left <= right){
				//find new middle
				m = (left + right)/2;
				logger.finest("New target m="+m+" (left: "+left+"; right: "+right+")");
				ValuePair vp = logdb.getScoreSumForParamRange(runName, ID, history, groupList.get(chosenGroup).getGroupName(), plist[i], m, temperature);
				logger.finest( "compare = (("+(m+1)+" - "+Integer.parseInt(vp.getName())+"))/"+(double)edgesAll+") + ("+visitedMultiply+" * "+Double.parseDouble(vp.getValue())+")");
				// m is not the number of edges, but m=(edges -1)
				compare = (((m+1) - Integer.parseInt(vp.getName()))/(double)edgesAll) + (visitedMultiply * Double.parseDouble(vp.getValue()));
				logger.finest("Compare: "+compare+" to target-random="+randomValue);
				if(compare < randomValue){
					left = m+1;
				}else if(compare > randomValue){
					// check if the last element has a smaller compare value
					if(m>0){
						vp = logdb.getScoreSumForParamRange(runName, ID, history, groupList.get(chosenGroup).getGroupName(), plist[i], (m-1), temperature);
						compare = ((m - Integer.parseInt(vp.getName()))/(double)edgesAll) + (visitedMultiply * Double.parseDouble(vp.getValue()));
					}else{
						//no left element exists, if m is already the leftmost
						compare = 0.0;
					}
					if(compare < randomValue){
						//the last compare is smaller, therefore the target points to the element we found!
						valueList[i] = new ValuePair(plist[i].getName(), plist[i].getValue(m));
						logger.finest("Choose value "+valueList[i].getValue()+" for "+valueList[i].getName());
						break binarySearch;
					}
					//keep searching
					right = m-1;
				}
			}

		}//end of loop over all parameters
		logger.finest("Choose edge for value list with size "+valueList.length+" (parameter: "+plist.length+")");
		Edge chosenEdge = groupList.get(chosenGroup).getEdgeById(valueList);
		
		if(chosenEdge == null){
			logger.severe("Did not select a new edge. Critical error. Fallback running: select maximum value edge.");
			//fallback: select max values
			for(int i=0; i<plist.length; i++){
				valueList[i] = new ValuePair(plist[i].getName(), plist[i].getMaxValue());
			}
			chosenEdge = groupList.get(chosenGroup).getEdgeById(valueList);
		}
		
		long id = logdb.addEdge(runName, chosenEdge.getGroupName(), chosenEdge.getParamValues(), plist);
		chosenEdge.setId(id);
		return chosenEdge;
	}
	
}
