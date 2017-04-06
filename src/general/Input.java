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

public class Input {
	private String originalInput;
	private String fullInput;
	private String fileOnly;
	private boolean IsVariable = false;
	
	/**
	 * @param input The path for the file (either relative to the base directory or the absolute path)
	 */
	public Input(String input){
		this.setFullInput(input);
		this.originalInput = input;
	}

	/**
	 * @return Returns only the filename with extension, without any other path information
	 */
	public String getFileOnly() {
		if(this.IsVariable){
			return this.originalInput;
		}
		return fileOnly;
	}

	public void setFileOnly(String fileOnly) {
		this.fileOnly = fileOnly;
	}

	public String getFullInput() {
		return fullInput;
	}

	private void setFullInput(String fullInput) {
		this.fullInput = fullInput;
	}

	/**
	 *
	 * @return True if the input is handled as a string variable
	 */
	public boolean isVariable() {
		return IsVariable;
	}

	/**
	 * Change the input to be a variable. Variables have the same values for FileOnly and FullInput
	 */
	public void setVariable() {
		IsVariable = true;
		this.setFullInput(originalInput);
		this.setFileOnly(originalInput);
	}
	
}
