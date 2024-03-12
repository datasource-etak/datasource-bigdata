package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

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

public class Operator implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(Operator.class.getCanonicalName());

    /*
    CREATE TABLE operators (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    shared_recipe_id    INTEGER,
    stage_id            INTEGER
    );
     */

    private final static String INSERT_OPERATOR =
            "INSERT INTO operators (name, description, shared_recipe_id, stage_id) " +
                    "VALUES (? ,?, ?, ?) RETURNING id;";

    private final static String GET_OPERATORS =
            "SELECT * FROM operators;";

    private final static String GET_OPERATOR_BY_ID =
            "SELECT * FROM operators where id=?;";

    private final static String GET_OPERATOR_NAME_BY_ID =
            "SELECT name FROM operators where id=?;";

    private final static String GET_OPERATORS_BY_STAGE_ID =
            "SELECT * FROM operators where stage_id=?;";


    private Integer id;

    private String name;

    private String description;

    private Integer sharedRecipeId;

    private Integer stageId;

    private List<OperatorParameter> parameters;

    public Operator() {
        this.parameters = new ArrayList<>();
    }

    public Operator(Integer id, String name, String description, Integer sharedRecipeId, Integer stageId) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.sharedRecipeId = sharedRecipeId;
        this.stageId = stageId;
        this.parameters = new ArrayList<>();
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

    public Integer getSharedRecipeId() {
        return sharedRecipeId;
    }

    public void setSharedRecipeId(Integer sharedRecipeId) {
        this.sharedRecipeId = sharedRecipeId;
    }

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public List<OperatorParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<OperatorParameter> parameters) {
        this.parameters = parameters;
    }

    private void fetchOperatorParameters(String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        try {
            this.parameters = OperatorParameter.getOperatorParametersByOperatorId(id, dataset, features, target, otherColumnArray);

        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.WARNING, "Could not fetch parameters for Operator " + id);
            throw e;
        }

    }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        // The object does not exist, it should be inserted.
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(INSERT_OPERATOR);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setInt(3, this.sharedRecipeId);
            statement.setInt(4, this.stageId);

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



    public static Operator getOperatorById(int id, String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                Operator operator =  new Operator(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("shared_recipe_id"),
                        resultSet.getInt("stage_id")
                );

                operator.fetchOperatorParameters(dataset, features, target, otherColumnArray);

                return operator;

            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Connector object not found.");
    }

    public static String getOperatorNameById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_NAME_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                return resultSet.getString("name");

            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Operator not found.");
    }


    public static Integer getOperatorStageOrdering(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                int stageId = resultSet.getInt("stage_id");
                return WorkflowStage.getStageOrderingById(stageId);

            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Connector object not found.");
    }


    public static List<Operator> getOperatorsByStageId(int stageId, String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<Operator> operators = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATORS_BY_STAGE_ID);
            statement.setInt(1, stageId);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                Operator operator = new Operator(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("shared_recipe_id"),
                        resultSet.getInt("stage_id")
                );

                operator.fetchOperatorParameters(dataset, features, target, otherColumnArray);

                operators.add(operator);
            }

            return operators;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }



    public static List<Operator> getOperators() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<Operator> operators = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATORS);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                Operator operator = new Operator(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getInt("shared_recipe_id"),
                        resultSet.getInt("stage_id")
                );

                operators.add(operator);
            }

            return operators;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }

    public Map<String, String> operatorParametersByOperatorIdAsTypeMap(String dataset, String[] features, String target, String[] otherColumns) throws SQLException, SystemConnectorException {

        if ((this.parameters == null) || this.parameters.isEmpty())
            this.parameters = OperatorParameter.getOperatorParametersByOperatorId(this.id, dataset, features, target, otherColumns);

        return parameters.
                stream().
                collect(
                        Collectors.toMap(
                                OperatorParameter::getName,
                                OperatorParameter::getType,
                                (a, b) -> b,
                                () -> new HashMap<>(parameters.size())
                        )
                );
    }

    public Map<String, Map<String, Object>> operatorParametersByOperatorIdAsRestrictionMap(String dataset, String[] features, String target, String[] otherColumns) throws SQLException, SystemConnectorException {
        if ((this.parameters == null) || this.parameters.isEmpty())
            this.parameters = OperatorParameter.getOperatorParametersByOperatorId(this.id,  dataset, features,  target, otherColumns);

        return parameters.
                stream().
                collect(
                        Collectors.toMap(
                                OperatorParameter::getName,
                                OperatorParameter::getRestrictions,
                                (a, b) -> b,
                                () -> new HashMap<>(parameters.size())
                        )
                );
    }


}
