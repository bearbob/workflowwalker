package instances;

import logdb.LogDB;
import sampler.*;

import java.util.ArrayList;

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

// TODO: This class is not ready to use but a collection of ideas how one might implement the walker with Cuneiform

public class CuneiformWalker extends Walker {

    private Workflow workflow;
    /**
     *
     * @param logdb An instance of the LogDB
     * @param runName The name of the current run
     */
    public CuneiformWalker(LogDB logdb, String runName) {
        super(logdb, runName);
        logger.fine("Created new Cuneiform walker instance");
        this.logdb.prepareRun(this.getSteps().size(), runName, true, false);

    }

    @Override
    protected void traverseEdge(Edge e, long configId) throws ExitCodeException {
        /* TODO
         * Execute the
         *
         */

    }

    @Override
    protected Workflow getSteps() {
        if(workflow == null){
            workflow = new Workflow();

            /* TODO
             * Create a workflow with a single step, containing all parameters if you wish to use a single workflow.
             * The workflow could be stored in a file (using the $#x#$ notation for variable value placeholders).
             */

            /* TODO
             * To use workflows with different algorithms, create different edge groups for this single step
             */

            logger.fine("Created new workflow with "+workflow.size()+" steps.");
        }
        return workflow;
    }

    @Override
    protected void submitResult(long configId) {
        // TODO Retrieve the output file of the workflow and use it here to generate a score


        //double score = ???
        //logdb.updateConfiguration(configId, ""+score, runName);
    }

    @Override
    protected void createExecutionEnv(long configId) {
        // TODO
    }

    @Override
    protected void handleInputFiles(long configId) {
        // TODO
    }

    @Override
    protected boolean handleCacheFiles(long configId, Edge[] workflow,
                                       long cacheId, int lastCommonStep) {
        // TODO
        return false;
    }

    @Override
    protected void deleteWorkfiles(long configId) {
        // TODO
    }

}
