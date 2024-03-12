package gr.ntua.ece.cslab.datasource.bda.analyticsml.beans;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.OperatorStatus;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.WorkflowStatus;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Operator;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.WorkflowStage;
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

public class MaterializedWorfklow implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(MaterializedWorfklow.class.getCanonicalName());

    String uuid;

    String alias;

    String description;

    String datasetId;

    String[] features;

    String target;

    List<MaterializedOperator> operators;

    String status;

    String result;

    String submittedAt;

    String completedAt;

    Integer workflowTypeId;

    public MaterializedWorfklow(String uuid, String alias, String description, String datasetId, String[] features, String target, List<MaterializedOperator> operators, String status, Integer workflowTypeId,
                                String submittedAt, String completedAt) {
        this.uuid = uuid;
        this.alias = alias;
        this.description = description;
        this.datasetId = datasetId;
        this.features = features;
        this.target = target;
        this.operators = operators;
        this.status = status;
        this.workflowTypeId= workflowTypeId;
        this.completedAt = completedAt;
        this.submittedAt = submittedAt;
    }

    public MaterializedWorfklow(Map<String, Object> data) {
        this.uuid = (String) data.getOrDefault("uuid", "");
        this.alias = (String) data.getOrDefault("alias", "");
        this.description = (String) data.getOrDefault("description", "");
        this.datasetId = (String) data.getOrDefault("dataset_id", "");
        this.features = ((String) data.getOrDefault("features", "")).split(",");
        this.target = (String) data.getOrDefault("target", "");
        this.operators = new ArrayList<>();
        this.status = (String) data.getOrDefault("status", "");
        this.workflowTypeId = Integer.valueOf((String) data.getOrDefault("workflow_type_id", 0));
        this.result = (String) data.getOrDefault("result", "[]");
        this.submittedAt = (String) data.getOrDefault("submitted_at", "");
        this.completedAt = (String) data.getOrDefault("completed_at", "");
    }

    public MaterializedWorfklow() {
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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }


    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String[] getFeatures() {
        return features;
    }

    public void setFeatures(String[] features) {
        this.features = features;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public List<MaterializedOperator> getOperators() {
        return operators;
    }

    public void setOperators(List<MaterializedOperator> operators) {
        this.operators = operators;
    }

    public Integer getWorkflowTypeId() {
        return workflowTypeId;
    }

    public void setWorkflowTypeId(Integer workflowTypeId) {
        this.workflowTypeId = workflowTypeId;
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
        return "MaterializedWorfklow{" +
                "uuid='" + uuid + '\'' +
                ", alias='" + alias + '\'' +
                ", description='" + description + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", features=" + Arrays.toString(features) +
                ", target='" + target + '\'' +
                ", operators=" + operators +
                ", status='" + status + '\'' +
                ", result='" + result + '\'' +
                ", submittedAt='" + submittedAt + '\'' +
                ", completedAt='" + completedAt + '\'' +
                ", workflowTypeId=" + workflowTypeId +
                '}';
    }

    public boolean validate(String slug) {
        return operators.
                stream().
                map(x-> {
                    try {
                        return x.validateParameters(slug);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to validate operator.");
                        return false;
                    }
                }).
                reduce((x,y)->x&&y).
                get();
    }

    public void save(String slug) throws Exception {
        String concatFeatures = String.join(",", features);
        DimensionTable dt = new DimensionTable();
        dt.setName("workflow_history_store");
        dt.setSchema(StorageBackend.workflowDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", this.uuid),
                new KeyValue("alias", this.alias),
                new KeyValue("description", this.description),
                new KeyValue("workflow_type_id", String.valueOf(this.workflowTypeId)),
                new KeyValue("dataset_id", this.datasetId),
                new KeyValue("features", concatFeatures),
                new KeyValue("target", this.target),
                new KeyValue("status", this.status),
                new KeyValue("result", this.result),
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
            LOGGER.warning("Materialized workflow " + this.uuid + " could not be saved.");
            throw e;
        }
    }

    public void saveOperators(String slug) throws Exception {
        if (!operators.isEmpty()) {
            for (MaterializedOperator mop : operators) {
                try {
                    mop.setWorkflowId(this.uuid);
                    mop.setResult("[]");
                    mop.setStatus(OperatorStatus.INITIALIZED.toString());
                    mop.setSubmittedAt("");
                    mop.setCompletedAt("");
                    mop.save(slug);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Could not store all materialized operators for workflow with uuid " + this.uuid);
                    throw e;
                }
            }
        }
    }

    public static MaterializedWorfklow load(String slug, String uuid) throws Exception {
        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("uuid", uuid);
        List<Tuple> fetched = (new StorageBackend(slug).select("workflow_history_store", mapfilters));

        System.out.println(fetched);

        List<Map<String, Object>> result = fetched.stream().map(Tuple::toMap).collect(Collectors.toList());


        return new MaterializedWorfklow(result.get(0));

    }

    public void fetchAbstractOperators(String slug) throws SQLException, SystemConnectorException {
        for (MaterializedOperator mop : operators) {
            Operator abstractOperator = Operator.getOperatorById(
                    mop.getOperatorId(),
                    this.datasetId,
                    this.features,
                    this.target,
                    DatasetSchemaFetcher.fetchOtherDatasetColumns(
                            slug,
                            this.datasetId,
                            this.features,
                            this.target
                    )
            );
            mop.setOperator(abstractOperator);

            Integer stageOrdering ;
            try {
                stageOrdering = WorkflowStage.getWorkflowStageById(abstractOperator.getStageId()).getOrdering();
            } catch (SQLException | SystemConnectorException e) {
                LOGGER.log(Level.SEVERE, "Could not fetch workflow stage with id " + abstractOperator.getStageId());
                throw e;
            }

            mop.setStageOrdering(stageOrdering);
        }

        System.out.println(this.toString());

        this.operators = this.operators.stream().sorted().collect(Collectors.toList());
    }

    private void fetchFromInitiliazedInstance(MaterializedWorfklow initialized) {
        this.alias = initialized.alias;
        this.description = initialized.description;
        this.datasetId = initialized.datasetId;
        this.features = initialized.features;
        this.target = initialized.target;
        this.status = initialized.status;
        this.result = initialized.result;
        this.workflowTypeId = initialized.workflowTypeId;
        this.completedAt = initialized.completedAt;
        this.submittedAt = initialized.submittedAt;
    }


    public void loadWorkflowInitializationData(String slug) throws Exception {
        try {
            MaterializedWorfklow initializedWorkflow = MaterializedWorfklow.load(slug, this.uuid);
            LOGGER.log(Level.INFO, "Found initialized workflow " + initializedWorkflow.toString());
            this.fetchFromInitiliazedInstance(initializedWorkflow);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Could not fetch initiliazed data for workflow with uuid : " + uuid);
            throw e;
        }
    }

    public float progress(String slug) throws Exception {
        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("workflow_id", uuid);
        List<Tuple> fetched = (new StorageBackend(slug).select("operator_history_store", mapfilters));

        List<String> result =
                fetched.stream().
                        map(Tuple::toMap).
                        map(x-> (String) x.get("status")).
                        collect(Collectors.toList());

        int completedOperators = result.stream().map(x->(x.equals(OperatorStatus.COMPLETED.toString()) ? 1 : 0)).reduce((x, y)->x+y).get();
        return ((float) completedOperators)/result.size();
    }

    public static Map<String, Object> getWorkflowStatusDescription(String slug, String uuid) {
        MaterializedWorfklow materializedWorfklow;
        try {
             materializedWorfklow = MaterializedWorfklow.load(slug, uuid);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error when loading materialized workflow");
            throw new RuntimeException(e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put("uuid", materializedWorfklow.uuid);
        result.put("alias", materializedWorfklow.alias);
        result.put("description", materializedWorfklow.description);
        result.put("status", materializedWorfklow.status);
        result.put("submitted_at", materializedWorfklow.submittedAt);
        result.put("completed_at", materializedWorfklow.completedAt);
        List<MaterializedOperator> operators;
        try {
            operators = MaterializedOperator.loadOperatorsForWorkflow(slug, uuid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        result.put("operators", operators.stream().sorted().map(MaterializedOperator::getStatusDescription).collect(Collectors.toList()));

        return result;

    }

    public List<Map<String, Object>> getParsedResult() {
        return Utils.parseJSONArrayString(this.result);
    }

    public static Map<String, Object> getWorkflowResultReport(String slug, String uuid) {
        MaterializedWorfklow materializedWorfklow;
        try {
            materializedWorfklow = MaterializedWorfklow.load(slug, uuid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put("result", materializedWorfklow.getParsedResult());
        List<MaterializedOperator> operators;
        try {
            operators = MaterializedOperator.loadOperatorsForWorkflow(slug, uuid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        result.put("operators", operators.stream().sorted().map(MaterializedOperator::getParsedResult).collect(Collectors.toList()));

        return result;

    }
}
