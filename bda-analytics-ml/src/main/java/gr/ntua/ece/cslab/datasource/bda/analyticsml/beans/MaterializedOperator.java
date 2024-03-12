package gr.ntua.ece.cslab.datasource.bda.analyticsml.beans;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.WorkflowStatus;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Operator;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MasterData;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.datasource.bda.datastore.statics.DatasetSchemaFetcher;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MaterializedOperator implements Serializable, Comparable<MaterializedOperator> {

    private final static Logger LOGGER = Logger.getLogger(MaterializedOperator.class.getCanonicalName());


    String uuid;

    Integer operatorId;

    Map<String, Object> parameters;

    String result;

    String status;

    String workflowId;

    Operator operator;

    Integer stageOrdering;

    String submittedAt;

    String completedAt;

    String name;

    public MaterializedOperator() {
    }

    public MaterializedOperator(Integer operatorId, Map<String, Object> parameters, String result, String status,
                                String workflowId, String submittedAt, String completedAt) {
        this.operatorId = operatorId;
        this.parameters = parameters;
        this.result = result;
        this.status = status;
        this.workflowId = workflowId;
        this.completedAt = completedAt;
        this.submittedAt = submittedAt;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getStageOrdering() {
        return stageOrdering;
    }

    public void setStageOrdering(Integer stageOrdering) {
        this.stageOrdering = stageOrdering;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public Integer getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(Integer operatorId) {
        this.operatorId = operatorId;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(String submittedAt) {
        this.submittedAt = submittedAt;
    }

    public String getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(String completedAt) {
        this.completedAt = completedAt;
    }

    @Override
    public String toString() {
        return "MaterializedOperator{" +
                "uuid='" + uuid + '\'' +
                ", operatorId=" + operatorId +
                ", parameters=" + parameters +
                ", result='" + result + '\'' +
                ", status='" + status + '\'' +
                ", workflowId='" + workflowId + '\'' +
                ", operator=" + operator +
                ", stageOrdering=" + stageOrdering +
                ", submittedAt='" + submittedAt + '\'' +
                ", completedAt='" + completedAt + '\'' +
                '}';
    }

    public boolean validateParameters(String slug) throws Exception {

        MaterializedWorfklow materializedWorfklow = MaterializedWorfklow.load(slug, this.workflowId);

        String[] otherColumnsArray = DatasetSchemaFetcher.fetchOtherDatasetColumns(
                slug,
                materializedWorfklow.getDatasetId(),
                materializedWorfklow.getFeatures(),
                materializedWorfklow.getTarget()
        );

        try {
            this.operator = Operator.getOperatorById(
                    this.operatorId,
                    materializedWorfklow.getDatasetId(),
                    materializedWorfklow.getFeatures(),
                    materializedWorfklow.getTarget(),
                    otherColumnsArray
            );
            Map<String, String> types = operator.operatorParametersByOperatorIdAsTypeMap(
                    materializedWorfklow.getDatasetId(),
                    materializedWorfklow.getFeatures(),
                    materializedWorfklow.getTarget(),
                    otherColumnsArray
            );
            Map<String, Map<String, Object>> restrictions = operator.operatorParametersByOperatorIdAsRestrictionMap(
                    materializedWorfklow.getDatasetId(),
                    materializedWorfklow.getFeatures(),
                    materializedWorfklow.getTarget(),
                    otherColumnsArray
            );

            for (String parameterName : parameters.keySet()) {
                if (!isValid(parameters.get(parameterName), types.get(parameterName), restrictions.get(parameterName))) {
                    LOGGER.log(Level.WARNING,
                            "Value of parameter " + parameterName +
                                    " of operator " + operatorId + " " +
                                    operator.getName() + " is invalid"
                    );
                    return false;
                }
            }

        } catch (SQLException | SystemConnectorException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private boolean isValid(Object value, String type, Map<String, Object> restrictions) {
        if (type.equals("string"))
            return isValidString((String) value, restrictions);

        if (type.equals("integer"))
            return isValidInteger((Integer) value, restrictions);

        if (type.equals("double"))
            return isValidDouble((Double) value, restrictions);

        return false;
    }

    private boolean isValidInteger(Integer value, Map<String, Object> restrictions) {
        String type = (String) restrictions.get("type");
        if (type.equals("from_list")){
            Map<String, List<String>> evaluate = (Map<String, List<String>>) restrictions.get("evaluate");
            return evaluate.get("choice_list").contains(value);
        }

        if (type.equals("range")) {
            Map<String, Integer> evaluate = (Map<String, Integer>) restrictions.get("evaluate");
            Integer minimum = evaluate.get("minimum");
            Integer maximum = evaluate.get("maximum");
            if (maximum != -1)
                return ((value >= minimum) && (value <= maximum));
            else if (maximum == -1)
                return (value >= minimum);
        }

        return false;
    }

    private boolean isValidDouble(Double value, Map<String, Object> restrictions) {
        String type = (String) restrictions.get("type");
        if (type.equals("from_list")){
            Map<String, List<String>> evaluate = (Map<String, List<String>>) restrictions.get("evaluate");
            return evaluate.get("choice_list").contains(value);
        }

        if (type.equals("range")) {
            Map<String, Double> evaluate = (Map<String, Double>) restrictions.get("evaluate");
            Double minimum = evaluate.get("minimum");
            Double maximum = evaluate.get("minimum");
            return ((value >= minimum) && (value <= maximum));
        }

        return false;
    }

    private boolean isValidString(String value, Map<String, Object> restrictions) {
        String type = (String) restrictions.get("type");
        Map<String, List<String>> evaluate = (Map<String, List<String>>) restrictions.get("evaluate");
        if (type.equals("from_list"))
            return evaluate.get("choice_list").contains(value);

        return false;
    }


    public void save(String slug) throws Exception {

        if ((this.uuid == null) || (this.uuid.isEmpty())) {
            this.uuid = UUID.randomUUID().toString();
        }

        if ((this.parameters == null)) {
            this.parameters = new HashMap<>();
        }

        DimensionTable dt = new DimensionTable();
        dt.setName("operator_history_store");
        dt.setSchema(StorageBackend.operatorDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", this.uuid),
                new KeyValue("operator_id", String.valueOf(this.operatorId)),
                new KeyValue("parameters", new JSONObject(this.parameters).toString()),
                new KeyValue("result", this.result),
                new KeyValue("status", this.status),
                new KeyValue("workflow_id", this.workflowId),
                new KeyValue("submitted_at", this.submittedAt),
                new KeyValue("completed_at", this.completedAt)
        });
        dt.setData(Arrays.asList(new Tuple[]{new Tuple(data)}));

        try {
            new StorageBackend(slug).init(
                    new MasterData(Arrays.asList(new DimensionTable[]{dt})),
                    false
            );
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
            LOGGER.warning("Materialized operator " + this.uuid + " could not be saved.");
            throw e;
        }
    }

    public static MaterializedOperator load(String slug, String uuid) throws Exception {
        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("uuid", uuid);
        List<Tuple> fetched = (new StorageBackend(slug).select("operator_history_store", mapfilters));

        System.out.println(fetched);

        Map<String, Object> result = fetched.stream().map(Tuple::toMap).collect(Collectors.toList()).get(0);

        return new MaterializedOperator(result);
    }

    public static List<MaterializedOperator> loadOperatorsForWorkflow(String slug, String workflowId) throws Exception {
        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("workflow_id", workflowId);
        List<Tuple> fetched = (new StorageBackend(slug).select("operator_history_store", mapfilters));

        return fetched.stream().map(Tuple::toMap).map(MaterializedOperator::new).collect(Collectors.toList());
    }


    public List<Map<String, Object>> getParsedResult() {
        return Utils.parseJSONArrayString(this.result);
    }

    public Map<String, Object> getStatusDescription() {
        Map<String, Object> result = new HashMap<>();
        result.put("uuid", this.uuid);
        result.put("name", this.name);
        result.put("parameters", this.parameters);
        result.put("status", this.status);
        result.put("submitted_at", this.submittedAt);
        result.put("completed_at", this.completedAt);
        return result;
    }


    public MaterializedOperator(Map<String, Object> data) {

         this.uuid = (String) data.getOrDefault("uuid", "");
         this.operatorId = Integer.valueOf((String) data.getOrDefault("operator_id", ""));
         this.parameters = new JSONObject((String) data.getOrDefault("parameters", "{}")).toMap();
         this.result = (String) data.getOrDefault("result", "{}");
         this.status = (String) data.getOrDefault("status", "");
         this.workflowId = (String) data.getOrDefault("workflow_id", "");
         this.completedAt = (String) data.getOrDefault("completed_at", "");
         this.submittedAt = (String) data.getOrDefault("submitted_at", "");
        try {
            this.stageOrdering = Operator.getOperatorStageOrdering(this.operatorId);
            this.name = Operator.getOperatorNameById(this.operatorId);
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch stage ordering");
            throw new RuntimeException(e);
        }
    }




    @Override
    public int compareTo(MaterializedOperator o) {
        int ordering = this.stageOrdering.compareTo(o.stageOrdering);
        if (ordering == 0) {
            return this.uuid.compareTo(o.uuid);
        }
        else {
            return ordering;
        }
    }
}
