package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MasterData;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;
import org.json.JSONArray;

import java.util.*;
import java.util.stream.Collectors;

public class DatasetDescription {
    String uuid;
    String alias;
    String datasetId;

    String datasetName;

    String timestamp;

    String datasetDescription;
    String sourceName;
    String associatedFilter;

    String status;
    List<Filter> filters;

    public DatasetDescription() {
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDatasetDescription() {
        return datasetDescription;
    }

    public void setDatasetDescription(String datasetDescription) {
        this.datasetDescription = datasetDescription;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getAssociatedFilter() {
        return associatedFilter;
    }

    public void setAssociatedFilter(String associatedFilter) {
        this.associatedFilter = associatedFilter;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public DatasetDescription(String uuid, String alias, String datasetId, String datasetName, String datasetDescription, String sourceName, List<Filter> filters, String status, String timestamp) {
        this.uuid = uuid;
        this.alias = alias;
        this.datasetId = datasetId;
        this.datasetName = datasetName;
        this.datasetDescription = datasetDescription;
        this.sourceName = sourceName;
        this.filters = filters != null ? filters : new ArrayList<>();
        this.associatedFilter = filters != null ? Base64.getEncoder().encodeToString((new JSONArray(filters)).toString().getBytes()) : "";
        this.status = status;
        this.timestamp = timestamp;
    }

    public DatasetDescription(Map<String, Object> data) {
        this.uuid = (String) data.getOrDefault("uuid", "");
        this.alias = (String) data.getOrDefault("alias", "");
        this.datasetId = (String) data.getOrDefault("dataset_id", "");
        this.datasetName = (String) data.getOrDefault("dataset_name", "");
        this.datasetDescription = (String) data.getOrDefault("dataset_name", datasetName);
        this.sourceName = (String) data.getOrDefault("source_name", "");
        this.filters =  new ArrayList<>();
        this.associatedFilter = (String) data.getOrDefault("associated_filter", "");
        this.status = (String) data.getOrDefault("status", "");
        this.timestamp = (String) data.getOrDefault("timestamp", "");
    }

    public void save(String slug) {
        DimensionTable dt = new DimensionTable();
        dt.setName("dataset_history_store");
        dt.setSchema(StorageBackend.datasetDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", uuid),
                new KeyValue("alias", alias),
                new KeyValue("dataset_id", datasetId),
                new KeyValue("dataset_name", datasetName),
                new KeyValue("dataset_description", datasetDescription),
                new KeyValue("source_name", sourceName),
                new KeyValue("associated_filter", associatedFilter),
                new KeyValue("status", status),
                new KeyValue("timestamp", timestamp)
        });
        dt.setData(Arrays.asList(new Tuple[]{new Tuple(data)}));


        try {
            new StorageBackend(slug).init(
                    new MasterData(Arrays.asList(new DimensionTable[]{dt})),
                    false
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DatasetDescription load(String slug, String uuid) throws Exception {
        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("uuid", uuid);
        List<Tuple> fetched = (new StorageBackend(slug).select("dataset_history_store", mapfilters));
        Map<String, Object> result = fetched.stream().map(Tuple::toMap).collect(Collectors.toList()).get(0);
        return new DatasetDescription(result);
    }

}
