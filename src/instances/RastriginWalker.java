package instances;

import java.util.ArrayList;

import logdb.LogDB;
import sampler.*;

/**
 * This walker is used to test the function of the algorithm by finding the minima 
 * for a simple multi dimensional function
 * @author vadril
 *
 */
public class RastriginWalker extends Walker {
	private double fx = 0;
	private Workflow workflow;
	/**
	 * 
	 * @param logdb An instance of the LogDB
	 * @param runName The name of the current run
	 */
	public RastriginWalker(LogDB logdb, String runName) {
		super(logdb, runName);
		logger.fine("Created new simple Function Walker instance");
		this.logdb.prepareRun(this.getSteps().size(), runName, false);
		
	}

	@Override
	protected void traverseEdge(Edge e, long configId) throws ExitCodeException {		
		/*
		 * The Rastrigin Function is a fairly complicated mathematical function often used as a 
		 * test problem for numerical optimization algorithms. 
		 * There are actually several variations of the function. 
		 * A common version is:
		 * 	f(x,y) = 20 + (x^2 – 10*cos(2*pi*x)) + (y^2 – 10*cos(2*pi*y))
		 * where x and y can be between -5.12 and +5.12.
		 *
		 * The function has a minimum value of 0.0 when x = 0.0 and y = 0.0.
		 */

		double A = 10;
		//to keep the 1/x function between 0 and 1, aim for (number of variables *10)+1
		String[] vals = e.getCommand().split(";");
		for(String v : vals){
			double x = Double.parseDouble(v);
			fx += (Math.pow(x,2) - A*Math.cos(2*Math.PI*x) + A);
		}
	}
	
	@Override
	protected Workflow getSteps() {
		/* Workflow size:
		 * Three steps with (20 + 20 + 20) edges each
		 * 60^3 = 216000
		 */
		if(workflow == null){
			workflow = new Workflow();

			Step s = new Step(this.logdb, this.runName);
			ArrayList<Parameter> paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xl", -3, -1, 0.01));
			EdgeGroup eg = new EdgeGroup("lowVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xm", -1, 1, 0.01));
			eg = new EdgeGroup("midVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xm#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xh", 1, 3, 0.01));
			eg = new EdgeGroup("highVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xh#$");
			s.addEdgeGroup(eg);
			workflow.add(s);

			s = new Step(this.logdb, this.runName);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yl", -3, -1, 0.01));
			eg = new EdgeGroup("lowVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("ym", -1, 1, 0.01));
			eg = new EdgeGroup("midVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#ym#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yh", 1, 3, 0.01));
			eg = new EdgeGroup("highVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yh#$");
			s.addEdgeGroup(eg);
			workflow.add(s);

			logger.fine("Created new workflow with "+workflow.size()+" steps.");
		}
		return workflow;
	}

	@Override
	protected void submitResult(long configId) {
		/* To convert the minima to a maxima, the function is lifted by 1 (to avoid dividing by zero)
		 */
		double score = 1.0/(fx + 1);
		fx = 0;
		logdb.updateConfiguration(configId, ""+score, runName);
	}
	
	@Override
	protected void createExecutionEnv(long configId) {
		// ignore for this walker
	}

	@Override
	protected void handleInputFiles(long configId) {
		// ignore for this walker
	}

	@Override
	protected boolean handleCacheFiles(long configId, Edge[] workflow,
			long cacheId, int lastCommonStep) {
		// ignore for this walker
		return false;
	}

	@Override
	protected void deleteWorkfiles(long configId) {
		// ignore for this walker
	}
	
}
