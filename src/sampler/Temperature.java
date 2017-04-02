package sampler;

import logdb.ValuePair;

import java.util.ArrayList;

/**
 * Created by B.Gross on 21.03.17.
 * Simple class to modify the behavior with rising sample number
 */
public class Temperature {
    private static double modifier = 10.0;

    /**
     *
     * @param scores A value pair list, ordered by score (ascending)
     * @param temperature double value between 0 and 1, rising over time
     * @return
     */
    public static double getRelativeScoreSum(ArrayList<ValuePair> scores, double temperature){
        double sum = 0.0;
        for(int i=0; i<scores.size(); i++){
            sum += getRelativeScoreForElement(scores.get(i), temperature, i, scores.size());
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
        int border = (int)(size * temperature); //casting will round to nearest lowest int value
        border -= 1; //to match the list positions
        double score = Double.parseDouble(vp.getValue());
        double rescore;
        if( position < border ) {
            rescore = ( score  / Math.pow(modifier, (1+temperature)));
        }else{
            rescore = ( score * Math.pow(modifier, (1+temperature)) );
        }
        return rescore;
    }

}
