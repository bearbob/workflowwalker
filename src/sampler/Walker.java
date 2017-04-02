package sampler;

import general.ExitCode;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import logdb.LogDB;
import logdb.ValuePair;

// Ye Old Texas Ranger

abstract public class Walker {
	protected static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	protected boolean USECACHE = true;
	protected LogDB logdb;
	protected String runName;
	
	public Walker(LogDB logdb, String runName){
		this.logdb = logdb;
		this.runName = runName;
	}
	
	/**
	 * @return A workflow of steps that are used to generate
	 * the workflow that will be run
	 */
	abstract protected Workflow getSteps();
		
	public void useCache(boolean trigger){
		USECACHE = trigger;
	}
	
	/**
	 * Start simulated annealing on the given workflow with the designated number of samples.
	 * @param samples The number of samples that are run.
	 */
	public void sample(int samples){
		Step[] steps = this.getSteps().asArray();
		
		if(steps.length <= 0){
			logger.severe("No workflow steps are given. At least one step must be defined.");
			System.exit(ExitCode.WORKFLOWERROR);
		}

		long time = 0;

		sampling:for(int run=1; run<=samples; run++){
			logger.info("Started sample run "+run);
			long starttime = System.currentTimeMillis();
			//create the new workflow
			ArrayList<Edge> workflow = new ArrayList<>();
			ArrayList<ValuePair> config = new ArrayList<>();
			for(Step step : steps){
				logger.finest("Choose new Edge for step "+step.getId());
				double temperature = ((double)run)/ samples;
				Edge e = step.chooseNewEdge(workflow, temperature);
				workflow.add(e);
				logger.finest("Adding edge "+e.getGroupName()+" with ID "+e.getIdAsString()+" to workflow.");
				config.add(new ValuePair(e.getGroupName(), e.getIdAsString()));
			}
			long previousId = logdb.containsSubset(config.toArray(new ValuePair[config.size()]), runName, true);
			if( previousId > 0){
				//has been computed before, no need to do it again
				logger.info("This configuration has been computed before with id: "+previousId);
				//log the score for optional result evaluation
				logdb.addSample(runName, previousId, logdb.getScoreForConfig(runName, previousId));
				continue sampling;
			}
			logger.finest("Configuration is not known yet. ");
			//save new config to db
			long rootId = logdb.addConfiguration(config.toArray(new ValuePair[config.size()]), runName);

			// compute the new config, but first check if caching is active and a subset has
			// been computed before
			long cachedId = -1; // default aka "none found"
			// The last step both the cache and the new config have in common
			int lastCommonStep = -1;
			if(USECACHE){
				filter:for(int i=0; i<config.size(); i++){
					//create and fill subset
					ValuePair[] subset = new ValuePair[i+1];
					for(int j=0; j<=i; j++){
						subset[j] = config.get(j);
					}
					long tempId = logdb.containsSubset(subset, runName);
					if(tempId > 0){
						logger.fine("Cached config found. Was "+cachedId+", is now "+tempId+" (Step ID: "+i+").");
						cachedId = tempId;
						lastCommonStep = i;
					}else{
						//no need to look any further, subset filter will only be more specified
						logger.fine("Stop looking for a suitable cache config, current cache config: "+cachedId+ " with starting step "+lastCommonStep);
						break filter;
					}
				}
			}//eoif USECACHE


			//stop the time
			long start = System.currentTimeMillis();
			// fetch from cache if possible and first run
			int result = walk(rootId, workflow.toArray(new Edge[workflow.size()]), cachedId, lastCommonStep);
			if(result != 0){
				logdb.failConfiguration(rootId, runName, result);
			}else{
				logdb.addSample(runName, rootId, logdb.getScoreForConfig(runName, rootId));
			}
			long resultTime = System.currentTimeMillis() - start;
			String timer = String.format("%d min, %d sec", 
						    TimeUnit.MILLISECONDS.toMinutes(resultTime),
						    TimeUnit.MILLISECONDS.toSeconds(resultTime) - 
						    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(resultTime))
						);
			long endtime = System.currentTimeMillis() - starttime;
			logger.info("Counter: Executing workflow "+rootId+" took "+timer + " ("+endtime+"ms)");
			time += endtime;
						
		}
		logger.info("Finished sampling after "+samples+" rounds. Took an average time of "+ (time/samples)+"ms.");
		
	}
	
	/**
	 * Executes the given workflow, creates all needed files and does the necessary checks
	 * @param configId  The id of the targeted configuration
	 * @param workflow Array of edges that form the workflow with a specific configuration
	 * @param cacheId ID of the config that will be used as cache
	 * @param lastCommonStep The last common step between the current config (configId) and the cached config (cacheId)
	 * @return 0 if everything worked out, or the step that failed the workflow. This is not the ID of the step, but it's order. This means even though the first step has ID 0, if it fails 1 will be returned. This means the result is either 0 or (stepID +1)
	 */
	private int walk(long configId, Edge[] workflow, long cacheId, int lastCommonStep){
		//step variable is used to show which step failed in case of an error
		int step = 0;
		try{
			createExecutionEnv(configId);
			handleInputFiles(configId);
			if(USECACHE) {
				boolean cache = handleCacheFiles(configId, workflow, cacheId, lastCommonStep);
				if (!cache || lastCommonStep < 0) {
					lastCommonStep = 0;
				}
			}
			logger.finest("Starting with last common step: "+lastCommonStep);
			for(int e=lastCommonStep; e<workflow.length; e++){
				step = e;
				traverseEdge(workflow[e], configId);
			}
			submitResult(configId);
			
			// no error so far? Nice. Then we are done.
			if(!USECACHE){
				//no caching, erase the files after we are done to save space
				deleteWorkfiles(configId);
			}
			return 0;
		}catch(Exception e){
			logger.warning("Error:\nWalking failed at step "+(step+1)+" with message "+e.getMessage()+"\n");
			//while the step ids start with 0, we count the fail number without 0 (starting at 1)
			return (step+1);
		}
	}
	
	/** 
	 * Creates the execution enviroment: path, directories and initial file links
	 */
	abstract protected void createExecutionEnv(long configId);
	
	/**
	 * Prepares all needed input files in the new execution enviroment
	 * @param configId The id of the targeted configuration
	 */
	abstract protected void handleInputFiles(long configId);
	
	/**
	 * Prepares the cached files for the new execution enviroment
	 * @param configId The id of the targeted configuration
	 * @param workflow Array of edges that form the workflow with a specific configuration
	 * @param cacheId ID of the config that will be used as cache
	 * @param lastCommonStep The last common step between the current config (configId) and the cached config (cacheId)
	 * @return True if no error occurred and caching can be used. Returns also false, if caching is deactivated
	 */
	abstract protected boolean handleCacheFiles(long configId, Edge[] workflow, long cacheId, int lastCommonStep);
	
	/**
	 * Execute the script given by the edge after checking the existence of all needed inputs
	 * and check the existens of all outputs afterwards
	 * @param e Edge object that will be executed
	 */
	abstract protected void traverseEdge(Edge e, long configId) throws ExitCodeException ;
	
	/**
	 * Saves the results of the workflow and updates the configuration score in the database
	 */
	abstract protected void submitResult(long configId);
	
	/**
	 * Deletes the working folder for this configuration
	 * @param configId The id of the targeted configuration
	 */
	abstract protected void deleteWorkfiles(long configId);
	
}
