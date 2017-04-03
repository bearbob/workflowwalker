package sampler;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * Created by B.Gross on 21.03.17.
 * Simple class to modify the behavior with rising sample number
 */
public class AnnealingFunction {
    protected static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static double distanceModifier = 0.10;
    private static double tzero = 1;

    public static long getDistance(long allPossibilities){

        return Math.round(allPossibilities*distanceModifier);
    }

    public static long getNextCandidate(long all, long current){
        long dist = getDistance(all)/2;
        long mod = ThreadLocalRandom.current().nextLong(-(dist+1), dist+1);

        return ((current + mod)%all)+1;
    }

    /**
     *
     * @param currentScore The score of the currently accepted solution
     * @param candidateScore The score of the candidate solution
     * @param temperature Double value between 0 and 1. Higher temperature makes the algorithm more greedy
     * @param searchMaxima If true, the acceptance chance will model the search for an maxima or else for a minima
     * @return True if the candidate solution was accepted
     */
    public static boolean accept(double currentScore, double candidateScore, double temperature, boolean searchMaxima){
        if(temperature < 0.0) {
            temperature = 0.0;
        }
        if(temperature > 1.0) {
            temperature = 1.0;
        }
        tzero = tzero / (1+temperature);
        double delta = (candidateScore - currentScore);
        double pa = Math.min(1, Math.exp(-delta / tzero)); //default: search minima
        if(searchMaxima) {
            pa = Math.min(1, Math.exp(delta / tzero));
        }

        Random generator = new Random();
        double selected = generator.nextDouble();

        logger.info("Old value: "+currentScore+", new value: "+candidateScore+" - Acceptance chance: "+pa+", selected value: "+selected);
        if(selected < pa){
            return true;
        }
        return false;
    }

}
