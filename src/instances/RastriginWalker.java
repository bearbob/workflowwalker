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
	protected Workflow getSteps() {
		/* Workflow size:
		 * Three steps with (20 + 20 + 20) edges each
		 * 60^3 = 216000
		 */
		if(workflow == null){
			workflow = new Workflow();

			double stepSize = 0.01;

			Step s = new Step(this.logdb, this.runName);
			ArrayList<Parameter> paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xll", -5, (-3 - stepSize), stepSize));
			EdgeGroup eg = new EdgeGroup("verylowVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xll#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xl", -3, (-1 - stepSize), stepSize));
			eg = new EdgeGroup("lowVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xm", -1, 1, stepSize));
			eg = new EdgeGroup("midVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xm#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xh", (1 + stepSize), 3, stepSize));
			eg = new EdgeGroup("highVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xh#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xhh", (3 + stepSize), 5, stepSize));
			eg = new EdgeGroup("veryhighVars1", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#xhh#$");
			s.addEdgeGroup(eg);
			workflow.add(s);

			s = new Step(this.logdb, this.runName);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yll", -5, (-3 - stepSize), stepSize));
			eg = new EdgeGroup("verylowVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yll#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yl", -3, (-1 - stepSize), stepSize));
			eg = new EdgeGroup("lowVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("ym", -1, 1, stepSize));
			eg = new EdgeGroup("midVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#ym#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yh", (1 + stepSize), 3, stepSize));
			eg = new EdgeGroup("highVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yh#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yhh", (3 + stepSize), 5, stepSize));
			eg = new EdgeGroup("veryhighVars2", paramList.toArray(new Parameter[paramList.size()]), null, null, "$#yhh#$");
			s.addEdgeGroup(eg);
			workflow.add(s);

			logger.fine("Created new workflow with "+workflow.size()+" steps.");
		}
		return workflow;
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
		String[] vals = e.getCommand().split(";");
		for(String v : vals){
			double x = Double.parseDouble(v);
			fx += 35;
			fx -= (Math.pow(x,2) - A*Math.cos(2*Math.PI*x));
		}
	}

	@Override
	protected void submitResult(long configId) {
		logdb.updateConfiguration(configId, ""+fx, runName);
		fx = 0;
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

	@Override
	protected boolean searchMaxima(){
		return true;
	}
	
}
