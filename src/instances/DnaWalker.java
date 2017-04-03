package instances;

import java.util.ArrayList;
import java.util.logging.Level;

import logdb.LogDB;
import sampler.*;

public class DnaWalker extends BashWalker {
	protected Workflow workflow = null;
	
	public DnaWalker(LogDB logdb, String runName, String basedir, String inputFile, int threads, TargetFunction target) {
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
			workflow.add(getAlign()); //0
			workflow.add(getSort()); //1
			workflow.add(getCreateTarget()); //2
			workflow.add(getRealign()); //3
			workflow.add(getRecal()); //4
			workflow.add(getPrintReads()); //5
			workflow.add(getRawVariants()); //6
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
	
	protected Step getAlign(){
		Step align = new Step(this.logdb, this.runName);
		String alignResult ="aligned_reads.sam";
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new IntegerParameter("seeds", 19, 24, 5));
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
		bwaScript.append(" -k $#seeds#$ ");
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
		paramList.add(new IntegerParameter("seedlength", 20, 25, 5));
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
		bowtiwScript.append(" -L $#seedlength#$ ");
		// bowtiwScript.append(" -L $#seedlength#$ --mp $#mismatchMax#$,$#mismatchMin#$ ");
		/* TODO: assuming the bowtie2idx files have this name as prefix
		 *	Maybe add a variable key containing the name
		 */
		bowtiwScript.append("-x bowtie2idx ");
		//only add the second read file if the paired mode is active (default)
		if(!this.isSingle()){
			bowtiwScript.append("-1 ");
			bowtiwScript.append(getFile("fq1"));
			bowtiwScript.append(" -2 "); //to seperate fq1 and fq2
			bowtiwScript.append(getFile("fq2"));
			bowtie2In.add(getFile("fq2"));
		}else{
			//unpaired reads
			bowtiwScript.append("-U ");
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
		paramList.add(new IntegerParameter("seeds", 10, 15, 5));
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
	
	protected Step getSort(){
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
	
	protected Step getCreateTarget(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T RealignerTargetCreator -R ");
		script.append(getFile("fa"));
		script.append(" -I dedup.bam -o target_intervals.list -known ");
		script.append(getFile("mills"));
		script.append(" -known ");
		script.append(getFile("phase1"));
		//script.append(" --maxIntervalSize $#maxIntervalSize#$ --minReadsAtLocus $#minReads#$ --windowSize $#windowSize#$");
		EdgeGroup eg = new EdgeGroup("gatkCreatetarget", 
				null,
				new String[]{"dedup.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi"), getFile("phase1"), getFile("phase1_tbi")}, 
				new String[]{"target_intervals.list"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	protected Step getRealign(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups

		ArrayList<Parameter> paramList = new ArrayList<>();
		paramList.add(new DoubleParameter("entropy", 0.15, 0.85, 0.7));
		StringBuilder script = new StringBuilder();
		script.append("gatk -T IndelRealigner -R ");
		script.append(getFile("fa"));
		script.append(" -I dedup.bam -targetIntervals target_intervals.list -o realigned_reads.bam -known ");
		script.append(getFile("mills"));
		script.append(" -known ");
		script.append(getFile("phase1"));
		script.append(" -entropy $#entropy#$ ");
		//script.append(" -entropy $#entropy#$ --maxIsizeForMovement $#maxIsize#$ --maxPositionalMoveAllowed $#maxPosMove#$ --maxReadsForRealignment $#maxReads#$ ");
		EdgeGroup eg = new EdgeGroup("Realign_GATK",
				paramList.toArray(new Parameter[paramList.size()]),
				new String[]{"dedup.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("mills"), getFile("mills_tbi"), getFile("phase1"), getFile("phase1_tbi")}, 
				new String[]{"realigned_reads.bam", "realigned_reads.bai"}, 
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}
	
	protected Step getRecal(){
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
	
	protected Step getPrintReads(){
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
	
	protected Step getRawVariants(){
		Step step = new Step(this.logdb, this.runName);
		//add the edgegroups
		
		StringBuilder script = new StringBuilder();
		script.append("gatk -T HaplotypeCaller -R ");
		script.append(getFile("fa"));
		script.append(" -I recal_reads.bam -o raw_variants.vcf");
		script.append(" --genotyping_mode DISCOVERY --output_mode EMIT_VARIANTS_ONLY --dbsnp ");
		script.append(getFile("dbsnp138"));
		//script.append("	--maxReadsInRegionPerSample $#maxReadsInRegionPerSample#$ --min_base_quality_score $#mbq#$ --minReadsPerAlignmentStart $#minReadsPerAlignStart#$ ");
		//script.append("	--standard_min_confidence_threshold_for_calling $#stand_call_conf#$ --standard_min_confidence_threshold_for_emitting $#stand_emit_conf#$");
		
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
		String[] in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("dbsnp138"), getFile("dbsnp138_tbi")};
		if(tf.isAnnotate()){
			//add jannovar annotation
			script.append(System.getProperty("line.separator"));
			script.append("jannovar annotate -i raw_variants.vcf -d ");
			script.append(getFile("annodb"));
			out = new String[]{"raw_variants.vcf", "raw_variants.jv.vcf"};
			in = new String[]{"recal_reads.bam", getFile("fa"), getFile("fai"), getFile("dict"), getFile("annodb"), getFile("dbsnp138"), getFile("dbsnp138_tbi")};
		}
		EdgeGroup eg = new EdgeGroup("Haplotype_GATK", 
				null,
				in, 
				out,  
				script.toString()
				);
		
		step.addEdgeGroup(eg);
		return step;
	}

	@Override
	protected boolean searchMaxima(){
		return true;
	}

}
