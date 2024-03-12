package gr.ntua.ece.cslab.datasource.bda.analyticsml.enums;

public enum WorkflowStatus {
    INITIALIZED("INITIALIZED"),
    STARTING("STARTING"),

    RUNNING("RUNNING"),
    COMPLETED("COMPLETED"),
    ERROR("ERROR")
    ;

    private final String text;

    /**
     * @param text
     */
    WorkflowStatus(final String text) {
        this.text = text;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return text;
    }
}