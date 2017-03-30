package sampler;

import java.util.ArrayList;

public class Workflow {
    private int size = 0;
    private ArrayList<Step> steps;

    public Workflow(){
        this.steps = new ArrayList<>();
    }

    public void add(Step s){
        s.setId(size++);
        this.steps.add(s);
    }

    public Step get(int index){
        return this.steps.get(index);
    }

    public Step[] asArray(){
        return this.steps.toArray(new Step[this.steps.size()]);
    }

    public int size(){
        return this.size;
    }
}
