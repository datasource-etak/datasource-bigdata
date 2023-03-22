package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import java.io.Serializable;

public class FilterValue implements Serializable {
    private String id;
    private String description;

    public FilterValue() {
    }

    public FilterValue(String id, String description) {
        this.id = id;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "FilterValue{" + "id='" + id + '\'' + ", description='" + description + '\'' + '}';
    }

}
