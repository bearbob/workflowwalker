package sampler;

/**
 * Created by B.Gross on 21.03.17.
 * Simple class to modify the behavior with rising sample number
 */
public class Temperature {

    /**
     * Used to get the score dependent on the current system temperature.
     * @param score Positive value
     * @param temperature Value between 0 and 1, rising over time
     * @return
     */
    public static double getRelativeScore(double score, double temperature){
        // t(x) = (1+x)^(1+t)
        double r = Math.pow(score +1, 1 + temperature);
        //double r = score * (1 + 99*temperature);
        return r;
    }

}
