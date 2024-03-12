package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.beans;

import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class JoinDatasetDescription implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(JoinDatasetDescription.class.getCanonicalName());

    String id;
    String alias;

    Map<String, Map<String, String>> columns;

    public JoinDatasetDescription() {
    }

    public JoinDatasetDescription(String slug, String id, String alias, Map<String, Map<String, String>> columns) throws Exception {
        this.id = id;
        this.alias = alias;
        this.columns = columns;

        parseId(slug);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Map<String, Map<String, String>> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Map<String, String>> columns) {
        this.columns = columns;
    }

    public void parseId(String slug) throws Exception {
        if ((this.id == null) || this.id.isEmpty()) {
            try {
                HashMap<String, String> filters = new HashMap<>();
                filters.put("alias", this.alias);

                this.id = new StorageBackend(slug).select("dataset_history_store", filters).
                        get(0).
                        getTuple().
                        stream().
                        filter(x->x.getKey().equals("uuid")).
                        collect(Collectors.toList()).
                        get(0).getValue();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "An error occured while fetching id");
                throw e;
            }
        }
    }

    @Override
    public String toString() {
        return "JoinDatasetDescription{" +
                "id='" + id + '\'' +
                ", alias='" + alias + '\'' +
                ", columns=" + columns +
                '}';
    }

    public int timeContainingColumnRenaming(String new_name) {
        return columns.values().stream().
                map(stringStringMap -> stringStringMap.get("new_name")).
                map(x->x.equals(new_name) ? 1 : 0).
                reduce((integer, integer2) -> integer+integer2).get();
    }

    public String validate(String slug) {
        return "valid";
    }

    public String parseJSONRenamingString() {
        return new JSONObject(columns.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        x->x.getValue().get("new_name")
                )
        )).toString();
    }

    public String parseJSONCastingString() {
        return new JSONObject(columns.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        x->x.getValue().get("cast_type")
                )
        )).toString();
    }

    public static String joinDatasetDescrListBase64encode(List<JoinDatasetDescription> datasetDesc) {
        return Base64.encodeBase64String((new JSONArray(datasetDesc)).toString().getBytes());
    }

    public static List<HashMap<String, Object>> listJoinDatasetDescrFromBase64String(String base64String) {
        String decodedString = new String(Base64.decodeBase64(base64String));
        JSONArray decodedObj = new JSONArray(decodedString);

        return decodedObj.toList().stream().map(x-> (HashMap<String, Object>) x).collect(Collectors.toList());
    }
}
