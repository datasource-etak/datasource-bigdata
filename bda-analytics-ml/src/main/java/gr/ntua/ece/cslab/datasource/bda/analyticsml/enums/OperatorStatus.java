package gr.ntua.ece.cslab.datasource.bda.analyticsml.enums;

public enum OperatorStatus {
    INITIALIZED("INITIALIZED"),
    RUNNING("RUNNING"),
    COMPLETED("COMPLETED"),
    ERROR("ERROR")
    ;

    private final String text;

    /**
     * @param text
     */
    OperatorStatus(final String text) {
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