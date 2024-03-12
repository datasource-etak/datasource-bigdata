package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.PostgresqlConnector;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkflowStage implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(WorkflowStage.class.getCanonicalName());

    /*
    CREATE TABLE workflow_stages (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    ordering            INTEGER,
    minimum_selection         INTEGER,
    maximum_selection       INTEGER,
    workflow_type_id            INTEGER,
    allow_duplicate_operators_with_other_parameters BOOLEAN
    );
     */
    private final static String INSERT_WORKFLOW_STAGE =
            "INSERT INTO workflow_stages (name, description, ordering, minimum_selection, maximum_selection, workflow_type_id, allow_duplicate_operators_with_other_parameters) " +
                    "VALUES (? ,?, ?, ?, ?, ?, ?) RETURNING id;";

    private final static String GET_WORKFLOW_STAGES =
            "SELECT * FROM workflow_stages;";

    private final static String GET_WORKFLOW_STAGE_BY_ID =
            "SELECT * FROM workflow_stages where id=?;";

    private final static String GET_WORKFLOW_STAGES_BY_TYPE_ID =
            "SELECT * FROM workflow_stages where workflow_type_id=? order by ordering;";

    private final static String GET_WORKFLOW_STAGE_ORDERING_BY_ID =
            "SELECT ordering FROM workflow_stages where id=?;";

    private Integer id;

    private String name;

    private String description;

    private Integer ordering;

    private Integer minimumSelection;

    private Integer maximumSelection;

    private Integer workflowTypeId;

    private Boolean allowDuplicateOperatorsWithOtherParameters;

    private List<Operator> operators;


    public WorkflowStage() {
        this.operators = new ArrayList<>();
    }

    public WorkflowStage(Integer id, String name, String description, Integer ordering, Integer minimumSelection, Integer maximumSelection, Integer workflowTypeId, Boolean allowDuplicateOperatorsWithOtherParameters) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.ordering = ordering;
        this.minimumSelection = minimumSelection;
        this.maximumSelection = maximumSelection;
        this.workflowTypeId = workflowTypeId;
        this.operators = new ArrayList<>();
        this.allowDuplicateOperatorsWithOtherParameters = allowDuplicateOperatorsWithOtherParameters;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getMinimumSelection() {
        return minimumSelection;
    }

    public void setMinimumSelection(Integer minimumSelection) {
        this.minimumSelection = minimumSelection;
    }

    public Integer getMaximumSelection() {
        return maximumSelection;
    }

    public void setMaximumSelection(Integer maximumSelection) {
        this.maximumSelection = maximumSelection;
    }

    public Integer getWorkflowTypeId() {
        return workflowTypeId;
    }

    public void setWorkflowTypeId(Integer workflowTypeId) {
        this.workflowTypeId = workflowTypeId;
    }

    public List<Operator> getOperators() {
        return operators;
    }

    public void setOperators(List<Operator> operators) {
        this.operators = operators;
    }

    public Integer getOrdering() {
        return ordering;
    }

    public void setOrdering(Integer ordering) {
        this.ordering = ordering;
    }

    public Boolean getAllowDuplicateOperatorsWithOtherParameters() {
        return allowDuplicateOperatorsWithOtherParameters;
    }

    public void setAllowDuplicateOperatorsWithOtherParameters(Boolean allowDuplicateOperatorsWithOtherParameters) {
        this.allowDuplicateOperatorsWithOtherParameters = allowDuplicateOperatorsWithOtherParameters;
    }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        // The object does not exist, it should be inserted.
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(INSERT_WORKFLOW_STAGE);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setInt(3, this.ordering);
            statement.setInt(4, this.minimumSelection);
            statement.setInt(5, this.maximumSelection);
            statement.setInt(6, this.workflowTypeId);
            statement.setBoolean(7, this.allowDuplicateOperatorsWithOtherParameters);

            ResultSet resultSet = statement.executeQuery();

            connection.commit();

            if (resultSet.next()) {
                this.id = resultSet.getInt("id");
            }
        } catch (SQLException e) {
            connection.rollback();
            LOGGER.log(Level.SEVERE, "Operator Parameter with name " + this.name + " could not saved");
            LOGGER.log(Level.SEVERE, e.getMessage());
            throw new SQLException("Failed to insert Connector object.");
        }

    }

    public static Integer getStageOrderingById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_STAGE_ORDERING_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                return resultSet.getInt("ordering");
            }

        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Stage with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Error fetching stage orgering");
    }



    public static WorkflowStage getWorkflowStageById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_STAGE_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                return new WorkflowStage(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("ordering"),
                        resultSet.getInt("minimum_selection"),
                        resultSet.getInt("maximum_selection"),
                        resultSet.getInt("workflow_type_id"),
                        resultSet.getBoolean("allow_duplicate_operators_with_other_parameters")
                );
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Connector object not found.");
    }

    public static List<WorkflowStage> getWorkflowStagesByWorfklowTypeId(int workflowTypeId, String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<WorkflowStage> stages = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_STAGES_BY_TYPE_ID);
            statement.setInt(1, workflowTypeId);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                WorkflowStage stage = new WorkflowStage(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("ordering"),
                        resultSet.getInt("minimum_selection"),
                        resultSet.getInt("maximum_selection"),
                        resultSet.getInt("workflow_type_id"),
                        resultSet.getBoolean("allow_duplicate_operators_with_other_parameters")

                );

                stage.fetchAll(dataset, features, target, otherColumnArray);

                stages.add(stage);
            }

            return stages;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }

    private void fetchAll(String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        try {
            this.operators = Operator.getOperatorsByStageId(this.id, dataset, features, target, otherColumnArray);
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.WARNING, "Could not fetch operators for stage id " + id);
            throw e;
        }
    }

    public static List<WorkflowStage> getWorkflowStages() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<WorkflowStage> stages = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_STAGES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                WorkflowStage stage = new WorkflowStage(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("ordering"),
                        resultSet.getInt("minimum_selection"),
                        resultSet.getInt("maximum_selection"),
                        resultSet.getInt("workflow_type_id"),
                        resultSet.getBoolean("allow_duplicate_operators_with_other_parameters")

                );

                stages.add(stage);
            }

            return stages;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }
}
