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

    public static boolean compareConfigs(double currentScore, double candidateScore, double temperature){
        tzero = tzero / (1+temperature);
        double delta = (candidateScore - currentScore);
        double pa = Math.min(1, Math.exp(delta/tzero));

        Random generator = new Random();
        double selected = generator.nextDouble();

        logger.info("Old value: "+currentScore+", new value: "+candidateScore+" - Acceptance chance: "+pa+", selected value: "+selected);
        if(selected < pa){
            return true;
        }
        return false;
    }

}
