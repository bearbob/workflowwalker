package instances;

import java.util.ArrayList;

import logdb.LogDB;
import sampler.*;

/*
		% WorkflowWalker: A Workflow Parameter Optimizer
		%
		% Copyright 2017 Björn Groß and Raik Otto
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
		this.logdb.prepareRun(this.getSteps().size(), runName);

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
			EdgeGroup eg = new EdgeGroup("verylowVars1", paramList.toArray(new Parameter[paramList.size()]), "$#xll#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xl", -3, (-1 - stepSize), stepSize));
			eg = new EdgeGroup("lowVars1", paramList.toArray(new Parameter[paramList.size()]), "$#xl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xm", -1, 1, stepSize));
			eg = new EdgeGroup("midVars1", paramList.toArray(new Parameter[paramList.size()]), "$#xm#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xh", (1 + stepSize), 3, stepSize));
			eg = new EdgeGroup("highVars1", paramList.toArray(new Parameter[paramList.size()]), "$#xh#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("xhh", (3 + stepSize), 5, stepSize));
			eg = new EdgeGroup("veryhighVars1", paramList.toArray(new Parameter[paramList.size()]), "$#xhh#$");
			s.addEdgeGroup(eg);
			workflow.add(s);

			s = new Step(this.logdb, this.runName);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yll", -5, (-3 - stepSize), stepSize));
			eg = new EdgeGroup("verylowVars2", paramList.toArray(new Parameter[paramList.size()]), "$#yll#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yl", -3, (-1 - stepSize), stepSize));
			eg = new EdgeGroup("lowVars2", paramList.toArray(new Parameter[paramList.size()]), "$#yl#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("ym", -1, 1, stepSize));
			eg = new EdgeGroup("midVars2", paramList.toArray(new Parameter[paramList.size()]), "$#ym#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yh", (1 + stepSize), 3, stepSize));
			eg = new EdgeGroup("highVars2", paramList.toArray(new Parameter[paramList.size()]), "$#yh#$");
			s.addEdgeGroup(eg);
			paramList = new ArrayList<>();
			paramList.add(new DoubleParameter("yhh", (3 + stepSize), 5, stepSize));
			eg = new EdgeGroup("veryhighVars2", paramList.toArray(new Parameter[paramList.size()]), "$#yhh#$");
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
			fx += 30;
			fx -= (Math.pow(x,2) - A*Math.cos(2*Math.PI*x));
		}
	}

	@Override
	protected void submitResult(long configId) {
		logdb.updateConfiguration(configId, fx, runName);
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


}
