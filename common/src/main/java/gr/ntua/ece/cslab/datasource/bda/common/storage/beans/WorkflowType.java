package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import com.sun.corba.se.spi.orbutil.threadpool.Work;
import com.sun.org.apache.xpath.internal.operations.Bool;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.PostgresqlConnector;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class WorkflowType implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(WorkflowType.class.getCanonicalName());

    /*
    CREATE TABLE workflow_types (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    library             TEXT,
    needs_target        BOOLEAN
    );
     */
    private final static String INSERT_WORKFLOW_TYPE =
            "INSERT INTO workflow_types (name, description, library, needs_target) " +
                    "VALUES (? ,?, ?) RETURNING id;";

    private final static String GET_WORKFLOW_TYPES =
            "SELECT * FROM workflow_types;";

    private final static String GET_WORKFLOW_BY_ID =
            "SELECT * FROM workflow_types where id=?;";


    private Integer id;
    private String name;
    private String description;
    private String library;
    private Boolean needsTarget;

    private List<WorkflowStage> stages;


    public WorkflowType() {
        this.stages = new ArrayList<>();
    }

    public WorkflowType(Integer id, String name, String description, String library, Boolean needsTarget) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.library = library;
        this.needsTarget = needsTarget;
        this.stages = new ArrayList<>();
    }

    public WorkflowType(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt("id");
        this.name = resultSet.getString("name");
        this.description = resultSet.getString("description");
        this.library = resultSet.getString("library");
        this.needsTarget = resultSet.getBoolean("needs_target");
        this.stages = new ArrayList<>();
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

    public String getLibrary() {
        return library;
    }

    public void setLibrary(String library) {
        this.library = library;
    }

    public List<WorkflowStage> getStages() {
        return stages;
    }

    public void setStages(List<WorkflowStage> stages) {
        this.stages = stages;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getNeedsTarget() { return needsTarget; }

    public void setNeedsTarget(Boolean needsTarget) { this.needsTarget = needsTarget; }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        // The object does not exist, it should be inserted.
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(INSERT_WORKFLOW_TYPE);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setString(3, this.library);
            statement.setBoolean(4, this.needsTarget);

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


    @Override
    public String toString() {
        return "WorkflowType{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", library='" + library + '\'' +
                ", stages=" + stages +
                '}';
    }

    public static WorkflowType getWorkflowTypeById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                WorkflowType workflowType = new WorkflowType(resultSet);
                //workflowType.fetchAll();
                return workflowType;
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Connector object not found.");
    }

    public void fetchAll(String dataset, String[] features, String target, String[] otherColumnArray) {
        try {
            this.stages = WorkflowStage.getWorkflowStagesByWorfklowTypeId(this.id, dataset, features, target, otherColumnArray);
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.WARNING, "Could not fetch workflow stages completely");
            throw new RuntimeException(e);
        }
    }


    public static List<WorkflowType> getWorkflowTypes() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<WorkflowType> types = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_TYPES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                WorkflowType type = new WorkflowType(resultSet);

                types.add(type);
            }

            return types;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }

    public Map<String, Object> toMap() {
        Map<String, Object> workflowTypeMap = new HashMap<String, Object>();

        workflowTypeMap.put("id", id);
        workflowTypeMap.put("name", library + "/" + name);
        workflowTypeMap.put("description", description);

        return workflowTypeMap;
    }

    public static List<Map<String, Object>> getWorkflowTypesAsMap() throws SQLException, SystemConnectorException {
        return WorkflowType.getWorkflowTypes().stream().map(WorkflowType::toMap).collect(Collectors.toList());
    }
}
