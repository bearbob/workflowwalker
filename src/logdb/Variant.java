package logdb;

/*
		% WorkflowWalker: A Workflow Parameter Optimizer
		%
		% Copyright 2017 Björn Groß
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

public class Variant {
	private int pos;
	private String chrom;
	private String content;
	
	public Variant(String chrom, int pos, String content){
		this.setChrom(chrom);
		this.setPos(pos);
		this.setContent(content);
	}

	public String getChrom() {
		return chrom;
	}

	private void setChrom(String chrom) {
		this.chrom = chrom;
	}

	public int getPos() {
		return pos;
	}

	private void setPos(int pos) {
		this.pos = pos;
	}

	public String getContent() {
		return content;
	}

	private void setContent(String content) {
		this.content = content;
	}
	
}
