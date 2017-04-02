package instances;

import general.ExitCode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import logdb.LogDB;
import logdb.Variant;

public class TargetFunction {
	public static final int SIMILARITYgoldstandard = 1;
	public static final int META = 2;
	public static final int PATHWAY = 3;
	private static final int MAXBATCHSIZE = 2500;
	
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private int target = META;
	private LogDB logdb;
	private boolean overwrite = false;
	private boolean annotate = false;
	
	public TargetFunction(LogDB logdb){
		this.logdb = logdb;
	}

	public void setTarget(int target){
		if(target == SIMILARITYgoldstandard || target == META){
			this.target = target;
		}else{
			this.target = META;
		}
	}
	
	public int getTarget(){

		return this.target;
	}
	
	/**
	 * Adds the variants of the given gold standard to the database, given that
	 * no standard is currently set. If variants are found within the database
	 * before the new gold standard is set, this methode will return without effect
	 * @param pathToFile
	 * @param runName The name of the current sampling process
	 */
	public void setGoldstandard(String pathToFile, String runName){
		logger.finest("Checking if variants already exist in the database...");
		if(logdb.hasVariants(runName, true)){
			if(isOverwrite()){
				logger.fine("The result set already contains variants. The new gold standard overwrite these.");
				logdb.deleteVariants(runName, true);
			}else{
				logger.info("The result set already contains variants. The new gold standard will not be saved. If this is an error, you can overwrite the saved results with '--overwrite'");
				return;
			}
		}
		logger.info("Adding the gold standard "+pathToFile+" to the database. This may take a while...");
		int logged = 0;
		int position;
		try (BufferedReader br = new BufferedReader(new FileReader(pathToFile))) {
		    String line, chrom, pos;
		    int firstTab;
		    int secondTab;
		    ArrayList<Variant> vlist = new ArrayList<>();
		    read:while ((line = br.readLine()) != null) {
		    	if(line.isEmpty()) continue read; //skip empty lines
		    	if(line.startsWith("#")) continue read;
		    	
		    	//get the chromosom and the position
		    	firstTab = line.indexOf("\t");
		    	secondTab = line.indexOf("\t", firstTab+1);
		    	chrom = line.substring(0, firstTab);
		    	pos = line.substring(firstTab+1, secondTab);
		    	position = Integer.parseInt(pos);
		    	vlist.add(new Variant(chrom, position, line.substring(secondTab+1)));
		    	if(vlist.size() >= MAXBATCHSIZE){
		    		logdb.addGoldVariant(runName, vlist);
		    		logged += vlist.size();
		    		vlist.clear();
		    	}
		    }
		    //add the last variants
		    if(vlist.size() > 0){
		    	logdb.addGoldVariant(runName, vlist);
	    		logged += vlist.size();
	    		vlist.clear();
	    	}
		}catch(Exception ex){
			ex.printStackTrace();
			System.exit(ExitCode.UNKNOWNERROR);
		}
		if(logged == 0){
			logger.severe("Did not log a single variant from the gold standard, something went wrong. Do all variant lines start with 'chr'?");
			System.exit(ExitCode.UNKNOWNERROR);
		}
		logger.info("Added the gold standard "+pathToFile+" to the database.");
	}
	
	/**
	 * 
	 * @param runName The name of the current sampling process
	 * @param vcfPath
	 * @param configId The id of the targeted configuration
	 */
	public void submitVcf(String runName, String vcfPath, long configId){
		ArrayList<Variant> variants = parseFile(runName, vcfPath, configId);
		
		// comparison to the gold standard must be handled differently
		if(target == SIMILARITYgoldstandard){
			// for this case the cosine distance gets reduced to
			// commonSet.size / sqrt(a.size) * sqrt(gold.size)
			double commonSetSize = logdb.getGoldstandardHits(runName, variants);
			double goldSetSize = logdb.getGoldstandardHits(runName, null);
			double distance = 0.0;
			if((commonSetSize>0) && goldSetSize > 0){
				distance = commonSetSize / (Math.sqrt(variants.size()) * Math.sqrt(goldSetSize));
			}
			logger.fine("commonSetSize="+commonSetSize+", goldSetSize="+goldSetSize+" >> distance="+distance);
			logdb.updateConfiguration(configId,  Double.toString(distance), runName);
			return;
		}
		//else
		// Only meta is currently implemented
		int totalConfigurations = logdb.getTotalNumberOfConfigurations(runName);
		ArrayList<Integer> allOccurences = logdb.getCommonVariantOccurrences(runName, null);
		ArrayList<Double> allScores = new ArrayList<>();
		for(int i : allOccurences){
			double newScore = i/(double)totalConfigurations;
			allScores.add(newScore);
		}

		//update all previous results that are in the database
		//do this for each new result
		ArrayList<Long> confs = logdb.getConfigurations(runName);
		confs.add(configId);
		for(long id : confs){
			double filtScores = logdb.getCommonVariantSumForConfig(runName, id)/totalConfigurations;
			double a = logdb.getNumberOfVariantsForConfig(runName, id);
			logdb.updateConfiguration(id, calculateCosineDistance(filtScores, a, allScores), runName);
		}
	}
	
	/** 
	 * Given two vectors, calculates the distance between them
	 * @param sumTop Scalar of both vectors, divided by the sum of total configurations
	 * @param vectorA Amount of rows where vector A is not null (a.i != 0)
	 * @param vectorBcomplete The score vector that was pulled from the database
	 * @return Cosine distance between the two vectors as string
	 */
	private String calculateCosineDistance(double sumTop, double vectorA, ArrayList<Double> vectorBcomplete){
		/* Q: Why not two score vectors?
		 * A: Only one is needed, the one from the database. The vector of the
		 * submitted vcf has 1 for each variant and 0 for each one that is not included.
		 */
		double normComplete = 0.0;
		for(double Bi : vectorBcomplete){
			normComplete += Math.pow(Bi, 2);
		}
		// vectorA would habe been Sum(a.i^2) with each a.i either 1 or 0, so we skipped the sum step before and
		// vectorA is the number of all rows where a.i = 1
		double score = sumTop / (Math.sqrt(vectorA) * Math.sqrt(normComplete));
		//double score = Math.cos(grad);
		String result = String.valueOf(score);
		//TODO: current result 'sumTop=1744.0, normalComplete=42.36603304072753, score=41.16505310571441'
		logger.finer("sumTop="+sumTop+", sqrt(vectorA)="+Math.sqrt(vectorA)+", sqrt(normalComplete)="+Math.sqrt(normComplete)+", score="+score);
		return result;
	}
	
	/**
	 * Read the VCF line per line and send the result to the database
	 * @param runName The name of the current sampling process
	 * @param vcfPath Path to the vcf file
	 * @param configId The id of the targeted configuration
	 * @return A list containing all found variants
	 */
	private ArrayList<Variant> parseFile(String runName, String vcfPath, long configId){
		ArrayList<Variant> list = new ArrayList<>();
		ArrayList<String[]> annolist = new ArrayList<>();
		ArrayList<Variant> tempList = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(vcfPath))) {
		    String line, chrom, pos;
		    int firstTab;
		    int secondTab;
		    read:while ((line = br.readLine()) != null) {
		    	if(line.isEmpty()) continue read; //skip empty lines
		    	if(line.startsWith("#")) continue read;
		    	
		    	//get chromosom and position
		    	firstTab = line.indexOf("\t");
		    	secondTab = line.indexOf("\t", firstTab+1);
		    	chrom = line.substring(0, firstTab);
		    	pos = line.substring(firstTab+1, line.indexOf("\t", firstTab+1));
		    	int position = Integer.parseInt(pos);
		    	String remaining = line.substring(secondTab+1);
		    	if(this.isAnnotate()){
		    		//assume that the annotation has been done and parse the remaining parts
		    		String[] splitted = remaining.split("\\|");
		    		String[] av = {splitted[3], splitted[10], splitted[1], splitted[5], splitted[7], splitted[2]};
		    		
		    		annolist.add(av);
		    	}
		    	Variant v = new Variant(chrom, position, remaining);
		    	list.add(v);
		    	tempList.add(v);
		    	if(tempList.size() >= MAXBATCHSIZE){
		    		logdb.addResultVariant(runName, configId, tempList);
		    		tempList.clear();
		    		if(this.isAnnotate()){
			    		logdb.addAnnotatedResultVariant(runName, configId, annolist);
		    			annolist.clear();
			    	}
		    	}
		    }
		    if(tempList.size() > 0){
		    	logdb.addResultVariant(runName, configId, tempList);
	    		tempList.clear();
	    		if(this.isAnnotate()){
		    		logdb.addAnnotatedResultVariant(runName, configId, annolist);
	    			annolist.clear();
		    	}
	    	}
		}catch(IOException ioe){
			ioe.printStackTrace();
			System.exit(5);
		}catch(Exception ex){
			ex.printStackTrace();
			System.exit(6);
		}
		return list;
	}

	/** 
	 * Whether the target function overwrites old results in the result table or discards the new variants.
	 * This is only effective if the target function is set to comparison with a gold standard
	 */
	public boolean isOverwrite() {
		return overwrite;
	}

	/** 
	 * Define whether the target function overwrites old results in the result table or discards the new variants.
	 * This is only effective if the target function is set to comparison with a gold standard
	 * @param overwrite
	 */
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	/**
	 *
	 * @return True if the VCF file has been annotated
	 */
	public boolean isAnnotate() {
		return annotate;
	}

	public void setAnnotate(boolean anno) {
		this.annotate = anno;
	}
	
}
