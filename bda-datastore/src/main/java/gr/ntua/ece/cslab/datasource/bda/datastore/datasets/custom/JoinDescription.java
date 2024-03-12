package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.beans.JoinDatasetDescription;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JoinDescription implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(JoinDescription.class.getCanonicalName());

    String alias;

    List<String> keys;

    List<JoinDatasetDescription> datasets;

    String type;

    public JoinDescription(String alias, List<String> keys, List<JoinDatasetDescription> datasets, String type) {
        this.alias = alias;
        this.keys = keys;
        this.datasets = datasets;
        this.type = type;
    }

    public JoinDescription() {
    }


    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public List<JoinDatasetDescription> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<JoinDatasetDescription> datasets) {
        this.datasets = datasets;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "JoinDescription{" +
                "alias='" + alias + '\'' +
                ", keys=" + keys +
                ", datasets=" + datasets +
                ", type='" + type + '\'' +
                '}';
    }

    public String validate(String slug) throws Exception {

        String validationErrorCause = "";
        if (datasets.isEmpty()) {

            validationErrorCause = "No dataset provided for join.";
            LOGGER.log(Level.WARNING, validationErrorCause);
            return validationErrorCause;
        }


        if (datasets.size() < 2) {
            validationErrorCause =  "Invalid number of input datasets";
            LOGGER.log(Level.WARNING, validationErrorCause);
            return validationErrorCause;
        }


        HashMap<String, String> filters = new HashMap<>();
        filters.put("alias", alias);
        if (!(new StorageBackend(slug).select("dataset_history_store", filters).isEmpty())) {
            validationErrorCause = "Dataset alias has already been used";
            LOGGER.log(Level.WARNING, validationErrorCause);
            return validationErrorCause;
        }


        if (!(keys.
                stream().
                map(x->datasets.stream().map(y->(y.timeContainingColumnRenaming(x)) == 1).reduce((y,z)->y && z).get()).
                reduce((y,z)->y&&z).get())) {
            validationErrorCause =  "Something went wrong when settings join_keys. Available causes: \n 1. Join keys do not have newly selected column names \n 2. Join keys exist more or less than exactly one time per dataset.";
            LOGGER.log(Level.WARNING, validationErrorCause);
            return validationErrorCause;
        };


        for (JoinDatasetDescription dataset : datasets) {
            String result = dataset.validate(slug);

            if (!result.equals("valid")) {
                validationErrorCause = "Dataset " + dataset.getAlias() + " validation failed with " + result;
                LOGGER.log(Level.WARNING, validationErrorCause);
                return validationErrorCause;
            }

        }

        return "valid";
    }
}
