package sampler;

import logdb.ValuePair;

import java.util.ArrayList;

/**
 * Created by B.Gross on 21.03.17.
 * Simple class to modify the behavior with rising sample number
 */
public class Temperature {
    private static double modifier = 5.0;

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

    /**
     *
     * @param scores A value pair list, ordered by score (ascending)
     * @return
     */
    public static double getRelativeScoreSum(ArrayList<ValuePair> scores, double temperature){
        double sum = 0.0;
        int border = (int)(scores.size() * temperature); //casting will round to nearest lowest int value
        border -= 1; //to match the list positions
        for(int i=0; i<scores.size(); i++){
            if(i < border ) {
                sum += (Double.parseDouble(scores.get(i).getValue()) / modifier);
            }else{
                sum += (Double.parseDouble(scores.get(i).getValue()) * modifier);
            }
        }
        return sum;
    }

    /**
     *
     * @param vp
     * @param temperature
     * @param position The ordered position of this element within the element list, ordered ascending by score
     * @param size The size of the list
     * @return
     */
    public static double getRelativeScoreForElement(ValuePair vp, double temperature, int position, int size){
        double score = 0.0;
        int border = (int)(size * temperature); //casting will round to nearest lowest int value
        border -= 1; //to match the list positions
        if(position < border ) {
            score += (Double.parseDouble(vp.getValue()) / modifier);
        }else{
            score += (Double.parseDouble(vp.getValue()) * modifier);
        }
        return score;
    }

}
