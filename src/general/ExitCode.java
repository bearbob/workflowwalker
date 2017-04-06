package general;

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

public class ExitCode {
	public static final int NOERROR = 0;
	public static final int MISUSEERROR = 2;
	public static final int UNKNOWNERROR = 1;
	public static final int EXECUTEERROR = 3;
	public static final int INPUTERROR = 4;
	public static final int PATHERROR = 5;
	public static final int ARGUMENTERROR = 6;
	public static final int WORKFLOWERROR = 7;
	public static final int INVOKEERROR = 126;
	public static final int COMMANDNOTFOUNDERROR = 127;
	
	public static String getExitCode(int code){
		switch(code){
		case NOERROR: return "No error";
		case MISUSEERROR: return "Misuse of shell builtins (according to Bash documentation)";
		case PATHERROR: return "Path error";
		case EXECUTEERROR: return "Execute error";
		case INPUTERROR: return "Input error";
		case ARGUMENTERROR: return "Argument error";
		case WORKFLOWERROR: return "Error in the workflow definition";
		case INVOKEERROR: return "Command invoked cannot execute";
		case COMMANDNOTFOUNDERROR: return "Command not found";
		
		default: return "Unknown error: "+code;
		}
	}
}
