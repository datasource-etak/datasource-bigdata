package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilterValue implements Serializable, Comparable<FilterValue> {
    private String id;
    private String description;

    public FilterValue() {
    }

    public FilterValue(String id, String description) {
        this.id = id;
        this.description = description;
    }

    public FilterValue(Map<String, Object> ajson) {
        this.id = (String) ajson.get("id");
        this.description = (String) ajson.get("description");
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

    @Override
    public int compareTo(FilterValue o) {
        return this.id.compareToIgnoreCase(o.id);
    }

    protected static List<FilterValue> sorted(List<FilterValue> filterValues) {
        return filterValues.stream().sorted().collect(Collectors.toList());
    }
}
