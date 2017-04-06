package instances;

import java.util.ArrayList;

import logdb.LogDB;
import sampler.DoubleParameter;
import sampler.EdgeGroup;
import sampler.IntegerParameter;
import sampler.Parameter;
import sampler.Step;

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

public class PloidyRnaWalker extends RnaWalker {
	
	public PloidyRnaWalker(LogDB logdb, String runName, String basedir, String inputFile, int threads, TargetFunction target) {
		super(logdb, runName, basedir, inputFile, threads, target);
	}

	@Override
	protected Step getRawVariants(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new DoubleParameter("contamination", 0.0, 0.1, 0.05));
		paramList.add(new DoubleParameter("hets", 0.001, 0.003, 0.001));
		paramList.add(new IntegerParameter("maxReadsInRegionPerSample", 7500, 12500, 2500));
		paramList.add(new IntegerParameter("mbq", 5, 15, 5));
		paramList.add(new IntegerParameter("minReadsPerAlignStart", 5, 15, 5));
		paramList.add(new DoubleParameter("stand_call_conf", 27.0, 33.0, 1.0));
		paramList.add(new DoubleParameter("stand_emit_conf", 27.0, 33.0, 1.0));
		paramList.add(new IntegerParameter("ploidly", 1, 4, 1));

		StringBuilder script = new StringBuilder();
		script.append("gatk -T HaplotypeCaller -R ");
		script.append(getFile("fa"));
		script.append(" -I recal_reads.bam -o raw_variants.vcf");
		script.append(" -dontUseSoftClippedBases ");
		script.append(" --contamination_fraction_to_filter $#contamination#$ --heterozygosity $#hets#$ ");
		script.append("	--maxReadsInRegionPerSample $#maxReadsInRegionPerSample#$ --min_base_quality_score $#mbq#$ --minReadsPerAlignmentStart $#minReadsPerAlignStart#$ ");
		script.append("	--standard_min_confidence_threshold_for_calling $#stand_call_conf#$ --standard_min_confidence_threshold_for_emitting $#stand_emit_conf#$");
		script.append(" --sample_ploidy $#ploidly#$");
		String[] out = new String[]{"raw_variants.vcf"};
		String[] in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("annodb")};
		}
		EdgeGroup eg = new EdgeGroup("Haplotype_GATK",
				paramList.toArray(new Parameter[paramList.size()]),
				in,
				out,
				script.toString()
				);

		step.addEdgeGroup(eg);
		return step;
	}

}
