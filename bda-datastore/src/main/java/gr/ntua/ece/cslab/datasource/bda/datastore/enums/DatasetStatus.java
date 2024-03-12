package gr.ntua.ece.cslab.datasource.bda.datastore.enums;

public enum DatasetStatus {
    DOWNLOADING("DOWNLOADING"),
    DOWNLOADED("DOWNLOADED"),

    DOWNLOAD_ERROR("DOWNLOAD_ERROR"),
    CREATING("CREATING"),
    CREATED("CREATED"),
    ERROR("ERROR")
    ;

    private final String text;

    /**
     * @param text
     */
    DatasetStatus(final String text) {
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