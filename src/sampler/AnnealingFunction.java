package sampler;

import logdb.ValuePair;

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by B.Gross on 21.03.17.
 * Simple class to modify the behavior with rising sample number
 */
public class AnnealingFunction {
    protected static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private Random generator;

    public AnnealingFunction(long randomSeed){
        generator = new Random(randomSeed);
    }

    public double nextDouble(){
        return generator.nextDouble();
    }

    /**
     *
     * @param scores A value pair list, ordered by score (ascending)
     * @param temperature double value between 0 and 1, rising over time
     * @return
     */
    public double getRelativeScoreSum(ArrayList<ValuePair> scores, double temperature, double range, double currentScore){
        double sum = 0.0;
        for(int i=0; i<scores.size(); i++){
            sum += getRelativeScoreForElement(scores.get(i), temperature, range, currentScore);
        }
        return sum;
    }

    /**
     *
     * @param vp
     * @param temperature
     * @param range
     * @param currentScore
     * @return
     */
    public double getRelativeScoreForElement(ValuePair vp, double temperature, double range, double currentScore){
        double modifier = 2;

        double lowerArea = currentScore - (range * 0.5 * (1 - temperature));
        if(lowerArea < 0){
            lowerArea = 0.0;
        }

        double score = Double.parseDouble(vp.getValue());
        double rescore;
        if( score <= lowerArea ) {
            rescore = 0;
        }else{
            rescore = ( score * Math.pow(modifier, (1+temperature)) );
        }
        return rescore;
    }

    public boolean acceptScore(double currentScore, double candidateScore, double temperature){
        double t = (1-temperature);
        double delta = candidateScore - currentScore;
        double pa = Math.min(1, Math.exp(delta/t));

        double selected = nextDouble();
        logger.finer("Old value: "+currentScore+", new value: "+candidateScore+" - Acceptance chance: "+pa+", selected value: "+selected);
        if(selected < pa){
            return true;
        }
        return false;
    }

}
