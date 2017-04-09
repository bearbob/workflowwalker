package instances;

import logdb.LogDB;
import sampler.*;

import java.util.ArrayList;
import java.util.logging.Level;

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

public class LargeDnaWalker extends BashWalker {
	protected Workflow workflow = null;

	public LargeDnaWalker(LogDB logdb, String runName, String basedir, String inputFile, int threads, TargetFunction target) {
		super(logdb, runName, basedir, inputFile, target, threads);
	}

	@Override
	protected String[] getInputKeys() {
		logger.finest("Calling for input key names for DNAseq");
		ArrayList<String> result = new ArrayList<>();
		result.add("fq1");
		result.add("fq2");
		result.add("bwaidx");
		result.add("bowtie2idx");
		result.add("genomeDir");
		result.add("dict");
		result.add("fa");
		result.add("fai");
		result.add("mills");
		result.add("mills_tbi");
		result.add("phase1");
		result.add("phase1_tbi");
		result.add("dbsnp138");
		result.add("dbsnp138_tbi");
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
			workflow.add(getTrim()); //0
			workflow.add(getAlign()); //1
			workflow.add(getSort()); //2
			workflow.add(getCreateTarget()); //3
			workflow.add(getRealign()); //4
			workflow.add(getRecal()); //5
			workflow.add(getPrintReads()); //6
			workflow.add(getRawVariants()); //7
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

	private Step getTrim(){
		Step step = new Step(this.logdb, this.runName);

		/* the first edgegroup is empty - nothing happens here.
		 * this is because the choice is to either use trimming or not. this edge group
		 * represents the "not"-part
		 *  */
		EdgeGroup eg = new EdgeGroup("noTrim", null, null, null, null);
		step.addEdgeGroup(eg);


		/* The trim step will use trim_galore. Since the input name must be the same as if this step never
		 * happened (see "not"-group) some renaming will occur. The first rename is to make sure the output naming
		 * done by trim_galore can be handled easily
		 */

		// ArrayList<Parameter> paramList = new ArrayList<>();
		// paramList.add(new IntegerParameter("quality", 15, 25, 5));
		ArrayList<String> in = new ArrayList<>();
		in.add(getFile("fq1"));
		StringBuilder prefix= new StringBuilder("mv ");
		prefix.append(getFile("fq1"));
		prefix.append(" fastq1.read");
		prefix.append(System.getProperty("line.separator"));
		StringBuilder script= new StringBuilder("trim_galore ");
		// script.append(" -q $#quality#$ ");
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			prefix.append("mv ");
			prefix.append(getFile("fq2"));
			prefix.append(" fastq2.read");
			prefix.append(System.getProperty("line.separator"));
			script.append(" --paired fastq1.read fastq2.read"); //1 and 2 have to be in order
			in.add(getFile("fq2"));
		}else {
			script.append(" fastq1.read");
		}

		prefix.append(script.toString());
		//now rename again
		if(!this.isSingle()) {
			prefix.append(System.getProperty("line.separator"));
			prefix.append("mv fastq1.read_val_1.fq ");
			prefix.append(getFile("fq1"));
			prefix.append(System.getProperty("line.separator"));
			prefix.append("mv fastq2.read_val_2.fq ");
			prefix.append(getFile("fq2"));
		}else{
			prefix.append(System.getProperty("line.separator"));
			prefix.append("mv fastq1.read_trimmed.fq ");
			prefix.append(getFile("fq1"));
		}

		EdgeGroup trim = new EdgeGroup("trim_galore",
				null,
				in.toArray(new String[in.size()]),
				in.toArray(new String[in.size()]),
				prefix.toString());

		step.addEdgeGroup(trim);

		return step;

	}
	
	private Step getAlign(){
		Step align = new Step(this.logdb, this.runName);
		String alignResult ="aligned_reads.sam";
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("seeds", 10, 30, 5));
		paramList.add(new IntegerParameter("bandwidth", 60, 140, 10));
		//command definitions, inputs, outputs
		ArrayList<String> bwaIn = new ArrayList<>();
		bwaIn.add(getFile("bwaidx"));
		bwaIn.add(getFile("fq1"));
		StringBuilder bwaScript= new StringBuilder("tar xf ");
		bwaScript.append(getFile("bwaidx"));
		bwaScript.append(System.getProperty("line.separator"));
		bwaScript.append("bwa mem -R \"@RG\\tID:group1\\tSM:sample1\\tPL:illumina\\tLB:lib1\\tPU:unit1\" ");
		bwaScript.append("  -t ");
		bwaScript.append(THREADS);
		bwaScript.append(" -k $#seeds#$ -w $#bandwidth#$");
		// bwaScript.append(" -k $#seeds#$ -w $#bandwidth#$ -d $#zdropoff#$ -c $#discard#$ -A $#matchscore#$ -B $#mismatchPenality#$ ");
		bwaScript.append(" bwaidx ");
		bwaScript.append(getFile("fq1"));
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			bwaScript.append(" "); //to seperate fq1 and fq2
			bwaScript.append(getFile("fq2"));
			bwaIn.add(getFile("fq2"));
		}
		bwaScript.append(" > ");
		bwaScript.append(alignResult);
		bwaScript.append(System.getProperty("line.separator"));
		bwaScript.append("samtools flagstat "); //additional information
		bwaScript.append(alignResult);
		EdgeGroup bwa = new EdgeGroup("bwa",
				paramList.toArray(new Parameter[paramList.size()]),
				bwaIn.toArray(new String[bwaIn.size()]), 
				new String[]{alignResult}, 
				bwaScript.toString());
		
		align.addEdgeGroup(bwa);

		paramList = new ArrayList<>();
		//add the parameters
		//STAR aligner: alignments and seeding
		paramList.add(new IntegerParameter("seedSearchStartLmax", 40, 60, 10));
		paramList.add(new IntegerParameter("seedPerReadNmax", 800, 1200, 100));

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
		starOne.append(" --seedSearchStartLmax $#seedSearchStartLmax#$ --seedPerReadNmax $#seedPerReadNmax#$");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("samtools flagstat Aligned.out.sam");
		starOne.append(System.getProperty("line.separator"));
		starOne.append("mv Aligned.out.sam ../");
		starOne.append(alignResult);
		starOne.append(System.getProperty("line.separator"));
		starOne.append("cd .."); //go back to the parent folder
		EdgeGroup starAlign = new EdgeGroup("star",
				paramList.toArray(new Parameter[paramList.size()]),
				starOneIn.toArray(new String[starOneIn.size()]),
				new String[]{alignResult},
				starOne.toString());

		align.addEdgeGroup(starAlign);


		paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("seedlength", 10, 30, 5));
		paramList.add(new IntegerParameter("mismatch", 0, 1, 1));
		//command definitions, inputs, outputs
		ArrayList<String> bowtie2In = new ArrayList<>();
		bowtie2In.add(getFile("bowtie2idx"));
		bowtie2In.add(getFile("fq1"));
		StringBuilder bowtiwScript= new StringBuilder("tar xf ");
		bowtiwScript.append(getFile("bowtie2idx"));
		bowtiwScript.append(System.getProperty("line.separator"));
		bowtiwScript.append("bowtie2 ");
		bowtiwScript.append("--threads ");
		bowtiwScript.append(THREADS);
		//params
		bowtiwScript.append(" -L $#seedlength#$ -N $#mismatch#$ ");
		// bowtiwScript.append(" -L $#seedlength#$ --mp $#mismatchMax#$,$#mismatchMin#$ ");
		bowtiwScript.append(" -x bowtie2idx ");
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			bowtiwScript.append(" -1 ");
			bowtiwScript.append(getFile("fq1"));
			bowtiwScript.append(" -2 "); //to seperate fq1 and fq2
			bowtiwScript.append(getFile("fq2"));
			bowtie2In.add(getFile("fq2"));
		}else{
			//unpaired reads
			bowtiwScript.append(" -U ");
			bowtiwScript.append(getFile("fq1"));
		}
		//define output
		bowtiwScript.append(" -S ");
		bowtiwScript.append(alignResult);
		bowtiwScript.append(System.getProperty("line.separator"));
		bowtiwScript.append("samtools flagstat ");
		bowtiwScript.append(alignResult);

		EdgeGroup bowtie = new EdgeGroup("bowtie2",
				paramList.toArray(new Parameter[paramList.size()]),
				bowtie2In.toArray(new String[bowtie2In.size()]), 
				new String[]{alignResult}, 
				bowtiwScript.toString());
		
		align.addEdgeGroup(bowtie);

		//NextGenMap https://github.com/Cibiv/NextGenMap/wiki
		paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("seeds", 10, 15, 1));
		// ngm -1 NA12878_downsample0.0025_1.fastq -2 NA12878_downsample0.0025_2.fastq -r human_g1k_v37.clean.fasta -o results.sam -t 4
		ArrayList<String> ngmIn = new ArrayList<>();
		ngmIn.add(getFile("fq1"));
		ngmIn.add(getFile("fa"));
		StringBuilder ngmScript= new StringBuilder("ngm -t ");
		ngmScript.append(THREADS);
		ngmScript.append(" -k $#seeds#$ -r ");
		ngmScript.append(getFile("fa"));
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			ngmScript.append(" -1 ");
			ngmScript.append(getFile("fq1"));
			ngmScript.append(" -2 ");
			ngmScript.append(getFile("fq2"));
			ngmIn.add(getFile("fq2"));
		}else{
			ngmScript.append(" -q ");
			ngmScript.append(getFile("fq1"));
		}
		ngmScript.append(" -o ");
		ngmScript.append(alignResult);
		EdgeGroup ngm = new EdgeGroup("ngm",
				paramList.toArray(new Parameter[paramList.size()]),
				ngmIn.toArray(new String[ngmIn.size()]),
				new String[]{alignResult},
				ngmScript.toString());

		align.addEdgeGroup(ngm);

		return align;
	}
	
	private Step getSort(){
		Step sort = new Step(this.logdb, this.runName);
		//add the edgegroups

		StringBuilder script = new StringBuilder();
		script.append("picard AddOrReplaceReadGroups I=aligned_reads.sam O=rg_added_sorted.bam SO=coordinate RGID=1 RGLB=library RGPL=ILLUMINA RGPU=machine RGSM=placeholder");
		script.append(System.getProperty("line.separator"));
		script.append("picard MarkDuplicates I=rg_added_sorted.bam O=dedup.bam METRICS_FILE=dedupmetrics.txt ASSUME_SORT_ORDER=coordinate CREATE_INDEX=true VALIDATION_STRINGENCY=SILENT");
		//add the bam index
		script.append(System.getProperty("line.separator"));
		script.append("picard BuildBamIndex I=dedup.bam O=dedup.bai");
		EdgeGroup eg = new EdgeGroup("picardSortDedup", 
				null,
				new String[]{"aligned_reads.sam"}, 
				new String[]{"rg_added_sorted.bam", "dedup.bam", "dedup.bai"},
				script.toString()
				);
		
		sort.addEdgeGroup(eg);
		return sort;
		
	}
	
	private Step getCreateTarget(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T RealignerTargetCreator -R ");
		script.append(getFile("fa"));
		script.append(" -I dedup.bam -o target_intervals.list -known ");
		script.append(getFile("mills"));
		script.append(" -known ");
		script.append(getFile("phase1"));
		script.append(" --filter_reads_with_N_cigar"); //ignore reads with cigar operator N
		EdgeGroup eg = new EdgeGroup("gatkCreatetarget",
				null,
				new String[]{"dedup.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi"), getFile("phase1"), getFile("phase1_tbi")}, 
				new String[]{"target_intervals.list"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	private Step getRealign(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new DoubleParameter("entropy", 0.1, 0.9, 0.2));
		StringBuilder script = new StringBuilder();
		script.append("gatk -T IndelRealigner -R ");
		script.append(getFile("fa"));
		script.append(" -I dedup.bam -targetIntervals target_intervals.list -o realigned_reads.bam -known ");
		script.append(getFile("mills"));
		script.append(" -known ");
		script.append(getFile("phase1"));
		script.append(" -entropy $#entropy#$ --filter_reads_with_N_cigar");
		EdgeGroup eg = new EdgeGroup("Realign_GATK",
				paramList.toArray(new Parameter[paramList.size()]),
				new String[]{"dedup.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi"), getFile("phase1"), getFile("phase1_tbi")}, 
				new String[]{"realigned_reads.bam", "realigned_reads.bai"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	private Step getRecal(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T BaseRecalibrator -R ");
		script.append(getFile("fa"));
		script.append(" -I realigned_reads.bam -o recal_data.grp -knownSites ");
		script.append(getFile("mills"));
		script.append(" -knownSites ");
		script.append(getFile("phase1"));
		script.append(" -knownSites ");
		script.append(getFile("dbsnp138"));
		//script.append(" --indels_context_size $#ics#$ --mismatches_context_size $#mcs#$");
		EdgeGroup eg = new EdgeGroup("Baserecal_GATK", 
				null,
				new String[]{"realigned_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi"), getFile("phase1"), getFile("phase1_tbi"), getFile("dbsnp138"), getFile("dbsnp138_tbi")}, 
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
		script.append(" -I realigned_reads.bam -BQSR recal_data.grp -o recal_reads.bam");
		EdgeGroup eg = new EdgeGroup("PrintReads_GATK", 
				null, 
				new String[]{"realigned_reads.bam", "recal_data.grp", getFile("fa"), getFile("fai"), getFile("dict")}, 
				new String[]{"recal_reads.bam", "recal_reads.bai"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}

	private Step getRawVariants(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("ploidy", 1, 4, 1));
		paramList.add(new IntegerParameter("min_base_quality_score", 5, 20, 5));

		StringBuilder script = new StringBuilder();
		script.append("gatk -T HaplotypeCaller -R ");
		script.append(getFile("fa"));
		script.append(" -I recal_reads.bam -o raw_variants.vcf");
		script.append(" --genotyping_mode DISCOVERY --output_mode EMIT_VARIANTS_ONLY --dbsnp ");
		script.append(getFile("dbsnp138"));
		//script.append("	--min_base_quality_score $#mbq#$ --minReadsPerAlignmentStart $#minReadsPerAlignStart#$ ");
		script.append(" --sample_ploidy $#ploidy#$ -mbq $#min_base_quality_score#$ ");
		String[] out = new String[]{"raw_variants.vcf"};
		String[] in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("dbsnp138"), getFile("dbsnp138_tbi")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("dbsnp138"), getFile("dbsnp138_tbi"), getFile("annodb")};
		}
		EdgeGroup eg = new EdgeGroup("Haplotype_GATK",
				paramList.toArray(new Parameter[paramList.size()]),
				in,
				out,
				script.toString()
		);

		step.addEdgeGroup(eg);


		paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("ploidy", 1, 4, 1));
		paramList.add(new IntegerParameter("min_base_quality_score", 5, 20, 5));

		script = new StringBuilder();
		script.append("gatk -T UnifiedGenotyper -R ");
		script.append(getFile("fa"));
		script.append(" -I recal_reads.bam -o raw_variants.vcf --dbsnp ");
		script.append(getFile("dbsnp138"));
		script.append(" --sample_ploidy $#ploidy#$ -mbq $#min_base_quality_score#$ ");
		out = new String[]{"raw_variants.vcf"};
		in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("dbsnp138"), getFile("dbsnp138_tbi")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("dbsnp138"), getFile("dbsnp138_tbi"), getFile("annodb")};
		}
		eg = new EdgeGroup("UnifiedGenotyper_GATK",
				paramList.toArray(new Parameter[paramList.size()]),
				in,
				out,
				script.toString()
		);

		step.addEdgeGroup(eg);


		script = new StringBuilder();
		script.append("samtools mpileup -uf ");
		script.append(getFile("fa"));
		script.append(" recal_reads.bam | bcftools call -mv > var.raw.vcf ");
		script.append(System.getProperty("line.separator"));
		//filter all lines that are not header information or have a quality above 50
		script.append("cat var.raw.vcf | awk '$6>=50 || /^##/' > raw_variants.vcf");
		out = new String[]{"raw_variants.vcf"};
		in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("annodb")};
		}
		eg = new EdgeGroup("samtools_mpileup",
				null,
				in,
				out,
				script.toString()
		);

		step.addEdgeGroup(eg);

		// freebayes: https://github.com/ekg/freebayes
		// freebayes -p 2 -f ucsc.hg19.clean.fasta recal_reads.bam > bayes.raw.vcf
		paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("ploidy", 1, 4, 1));

		script = new StringBuilder();
		script.append("freebayes -p $#ploidy#$ -f ");
		script.append(getFile("fa"));
		script.append(" recal_reads.bam > raw_variants.vcf");
		out = new String[]{"raw_variants.vcf"};
		in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("annodb")};
		}
		eg = new EdgeGroup("freebayes",
				paramList.toArray(new Parameter[paramList.size()]),
				in,
				out,
				script.toString()
		);
		step.addEdgeGroup(eg);

		return step;
	}

}
