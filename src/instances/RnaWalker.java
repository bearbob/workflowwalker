package instances;

import java.util.ArrayList;
import java.util.logging.Level;

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

public class RnaWalker extends BashWalker {
	private int THREADS = 1; //Default
	private Workflow workflow = null;

	public RnaWalker(LogDB logdb, String runName, String basedir, String inputFile, int threads, TargetFunction target) {
		super(logdb, runName, basedir, inputFile, target, threads);
	}

	@Override
	protected String[] getInputKeys() {
		logger.finest("Calling for input key names for RNAseq");
		
		ArrayList<String> result = new ArrayList<>();
		result.add("fq1");
		result.add("fq2");
		result.add("dict");
		result.add("fa");
		result.add("fai");
		result.add("mills");
		result.add("mills_tbi");
		result.add("genomeDir");
		result.add("sequencename");
		if(tf.getTarget() == TargetFunction.SIMILARITYgoldstandard){
			result.add("gold");
		}
		if(tf.isAnnotate()){
			result.add("annodb");
		}
		
		return result.toArray(new String[result.size()]);
	}

	@Override
	protected Workflow getSteps() {
		if(workflow == null){
			logger.fine("First call to getSteps methode, create new workflow and add steps.");
			workflow = new Workflow();
			workflow.add(getAlign());
			workflow.add(getSort());
			workflow.add(getSplit());
			workflow.add(getRecal());
			workflow.add(getPrintReads());
			workflow.add(getRawVariants());
			logger.fine("Created new workflow with "+workflow.size()+" steps.");
		}
		return workflow;
	}

	@Override
	protected void submitResult(long configId) {
		String vcf = getExecDir(configId)+"/raw_variants.vcf";
		if(tf.isAnnotate()){
			vcf = getExecDir(configId)+"/raw_variants.jv.vcf";
		}
		tf.submitVcf(runName, vcf, configId);
		
	}

	private Step getAlign(){
		Step align = new Step(this.logdb, this.runName);
		//add the edgegroups
		ArrayList<Parameter> paramList = new ArrayList<>();
		//add the parameters
		//STAR aligner: scoring parameters
		paramList.add(new IntegerParameter("scoreGap", 15, 35, 5));
		paramList.add(new IntegerParameter("scoreGapNoncan", -10, -6, 2));
		paramList.add(new IntegerParameter("scoreGapGCAG", -8, -1, 2));
		paramList.add(new IntegerParameter("scoreGapATAC", -10, -6, 2));
		paramList.add(new IntegerParameter("scoreDelOpen", -3, -1, 1));
		paramList.add(new IntegerParameter("scoreDelBase", -3, -1, 1));
		paramList.add(new IntegerParameter("scoreInsOpen", -3, -1, 1));
		paramList.add(new IntegerParameter("scoreInsBase", -3, -1, 1));
		//STAR aligner: alignments and seeding
		paramList.add(new IntegerParameter("seedSearchStartLmax", 40, 60, 10));
		paramList.add(new IntegerParameter("seedPerReadNmax", 800, 1200, 200));
		
		//command definitions, inputs, outputs
		ArrayList<String> starOneIn = new ArrayList<>();
		starOneIn.add(getFile("fa"));
		starOneIn.add(getFile("genomeDir"));
		starOneIn.add(getFile("fq1"));
		StringBuilder starOne = new StringBuilder("mkdir pass1");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("cd pass1");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("STAR --genomeDir ../");
		starOne.append(getFile("genomeDir"));
		starOne.append(" --readFilesIn ../"); //up one folder for files
		starOne.append(getFile("fq1"));
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			starOne.append(" ../");
			starOne.append(getFile("fq2"));
			starOneIn.add(getFile("fq2"));
		}
		starOne.append("  --runThreadN ");
		starOne.append(THREADS);
		starOne.append(" --scoreGap $#scoreGap#$ --scoreGapNoncan $#scoreGapNoncan#$ --scoreGapGCAG $#scoreGapGCAG#$ --scoreGapATAC $#scoreGapATAC#$ --scoreDelOpen $#scoreDelOpen#$ ");
		starOne.append(" --scoreDelBase $#scoreDelBase#$ --scoreInsOpen $#scoreInsOpen#$ --scoreInsBase $#scoreInsBase#$ --seedSearchStartLmax $#seedSearchStartLmax#$ --seedPerReadNmax $#seedPerReadNmax#$");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("samtools flagstat Aligned.out.sam");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("cd .."); //go back to the parent folder
		EdgeGroup starAlign = new EdgeGroup("star", 
				paramList.toArray(new Parameter[paramList.size()]), 
				starOneIn.toArray(new String[starOneIn.size()]), 
				new String[]{"pass1/SJ.out.tab", "pass1/Aligned.out.sam"}, 
				starOne.toString());
		
		align.addEdgeGroup(starAlign);
		
		//TODO: add bwa align?
		return align;
	}
	
	private Step getSort(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		ArrayList<Parameter> paramList = new ArrayList<>();
		//add the parameters
		//paramList.add(new StringParameter("sortorder", new String[]{"coordinate", "unsorted", "queryname", "duplicate"}));
		paramList.add(new StringParameter("sortorder", new String[]{"coordinate"}));
		
		StringBuilder script = new StringBuilder();
		script.append("picard AddOrReplaceReadGroups I=pass1/Aligned.out.sam O=rg_added_sorted.bam SO=$#sortorder#$ RGID=1 RGLB=library RGPL=ILLUMINA RGPU=machine RGSM=");
		script.append(getFile("sequencename"));
		script.append(System.getProperty("line.separator"));
		script.append("picard MarkDuplicates I=rg_added_sorted.bam O=dedup.bam METRICS_FILE=dedupmetrics.txt ASSUME_SORT_ORDER=$#sortorder#$ CREATE_INDEX=true VALIDATION_STRINGENCY=SILENT");
		
		EdgeGroup eg = new EdgeGroup("picardSort", 
				paramList.toArray(new Parameter[paramList.size()]), 
				new String[]{"pass1/Aligned.out.sam", getFile("sequencename")}, 
				new String[]{"rg_added_sorted.bam", "dedup.bam", "dedup.bai"},
				script.toString()
				);

		step.addEdgeGroup(eg);
		return step;
		
	}
	
	private Step getSplit(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder cigar = new StringBuilder();
		cigar.append("gatk -T SplitNCigarReads -R ");
		cigar.append(getFile("fa"));
		cigar.append(" -I dedup.bam -o split.bam");
		cigar.append(" -rf ReassignOneMappingQuality -RMQF 255 -RMQT 60 -U ALLOW_N_CIGAR_READS");
		EdgeGroup eg = new EdgeGroup("SplitNCigar_GATK", 
				null, 
				new String[]{"dedup.bam", getFile("fa"), getFile("fai"), getFile("dict")}, 
				new String[]{"split.bam", "split.bai"}, 
				cigar.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	private Step getRecal(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("ics", 3, 8, 1));
		paramList.add(new IntegerParameter("mcs", 3, 7, 1));
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T BaseRecalibrator -R ");
		script.append(getFile("fa"));
		script.append(" -I split.bam -o recal_data.grp -knownSites ");
		script.append(getFile("mills"));
		script.append(" --indels_context_size $#ics#$ --mismatches_context_size $#mcs#$");
		EdgeGroup eg = new EdgeGroup("Baserecal_GATK", 
				paramList.toArray(new Parameter[paramList.size()]), 
				new String[]{"split.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi")}, 
				new String[]{"recal_data.grp"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	private Step getPrintReads(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T PrintReads -R ");
		script.append(getFile("fa"));
		script.append(" -I split.bam -BQSR recal_data.grp -o recal_reads.bam");
		EdgeGroup eg = new EdgeGroup("PrintReads_GATK", 
				null, 
				new String[]{"split.bam", "recal_data.grp", getFile("fa"), getFile("fai"), getFile("dict")}, 
				new String[]{"recal_reads.bam", "recal_reads.bai"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
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
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T HaplotypeCaller -R ");
		script.append(getFile("fa"));
		script.append(" -I recal_reads.bam -o raw_variants.vcf");
		script.append(" -dontUseSoftClippedBases ");
		script.append(" --contamination_fraction_to_filter $#contamination#$ --heterozygosity $#hets#$ ");
		script.append("	--maxReadsInRegionPerSample $#maxReadsInRegionPerSample#$ --min_base_quality_score $#mbq#$ --minReadsPerAlignmentStart $#minReadsPerAlignStart#$ ");
		script.append("	--standard_min_confidence_threshold_for_calling $#stand_call_conf#$ --standard_min_confidence_threshold_for_emitting $#stand_emit_conf#$");
		
		try{
			//if the variable ploidity exists in the config file, use the given ploidity instead of the default value
			//exception could have two reasons: the integer-string-conversion failed or the variable does not exist
			int ploidy = Integer.parseInt(getFile("ploidity"));
			script.append(" --sample_ploidy ");
			script.append(ploidy);
		}catch(NumberFormatException nfe){
			logger.log(Level.WARNING, "A value for ploidity exists but could not be converted to integer.", nfe);
		}catch(Exception e){
			logger.finest("No value for ploidity found, skipping and using default value");
		}
		
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
