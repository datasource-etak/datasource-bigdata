package gr.ntua.ece.cslab.datasource.bda.analyticsml.beans;

public class JobStage {

    private boolean succeeded;

    private String result;

    public JobStage() {
        succeeded = false;
        result = "";
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }


}
