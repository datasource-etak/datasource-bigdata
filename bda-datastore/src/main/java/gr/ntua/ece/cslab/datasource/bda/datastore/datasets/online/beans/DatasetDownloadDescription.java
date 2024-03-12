package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import java.util.List;
import java.io.Serializable;
import java.util.Map;


public class DatasetDownloadDescription implements Serializable {
    String alias;

    Map<String, List<String>> selectedFilters;

    public DatasetDownloadDescription() {
    }

    public DatasetDownloadDescription(String alias, Map<String, List<String>> selectedFilters) {
        this.alias = alias;
        this.selectedFilters = selectedFilters;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Map<String, List<String>> getSelectedFilters() {
        return selectedFilters;
    }

    public void setSelectedFilters(Map<String, List<String>> selectedFilters) {
        this.selectedFilters = selectedFilters;
    }

    @Override
    public String toString() {
        return "DatasetDownloadDescription{" +
                "alias='" + alias + '\'' +
                ", selectedFilters=" + selectedFilters +
                '}';
    }

}
