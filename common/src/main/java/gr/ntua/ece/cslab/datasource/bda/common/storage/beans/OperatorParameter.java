package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.PostgresqlConnector;
import org.json.JSONObject;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OperatorParameter implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(OperatorParameter.class.getCanonicalName());

    /*
    CREATE TABLE operator_parameters (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    type                TEXT,
    operator_id         INTEGER,
    restrictions        JSON
    );
     */
    private final static String INSERT_OPERATOR_PARAMETER =
            "INSERT INTO operator_parameters (name, type, operator_id, restrictions) " +
                    "VALUES (? ,?, ?, ?::json) RETURNING id;";

    private final static String GET_OPERATOR_PARAMETERS =
            "SELECT * FROM operator_parameters;";

    private final static String GET_OPERATOR_PARAMETER_BY_ID =
            "SELECT * FROM operator_parameters where id=?;";

    private final static String GET_OPERATOR_PARAMETERS_BY_OPERATOR_ID =
            "SELECT * FROM operator_parameters where operator_id=?;";


    private Integer id;

    private String name;

    private String type;

    private Integer operatorId;

    private Map<String, Object> restrictions;

    public OperatorParameter() {
    }

    public OperatorParameter(Integer id, String name, String type, Integer operatorId, String restrictions) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.operatorId = operatorId;
        parseRestrictionJSONString(restrictions);
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(Integer operatorId) {
        this.operatorId = operatorId;
    }

    public Map<String, Object> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(Map<String, Object> restrictions) {
        this.restrictions = restrictions;
    }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        // The object does not exist, it should be inserted.
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(INSERT_OPERATOR_PARAMETER);

            statement.setString(1, this.name);
            statement.setString(2, this.type);
            statement.setInt(3, this.operatorId);
            statement.setString(4, getRestrictionsAsJSONString());

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



    public static OperatorParameter getOperatorParameterById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_PARAMETER_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                return new OperatorParameter(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("type"),
                        resultSet.getInt("operator_id"),
                        resultSet.getString("restrictions")
                );
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Operator with id " + id + " could not be fetched");
            LOGGER.log(Level.WARNING, e.getMessage());
        }

        throw new SQLException("Connector object not found.");
    }

    public static List<OperatorParameter> getOperatorParametersByOperatorId(int operatorId, String dataset, String[] features, String target, String[] otherColumnArray) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<OperatorParameter> operatorParameters = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_PARAMETERS_BY_OPERATOR_ID);
            statement.setInt(1, operatorId);
            ResultSet resultSet = statement.executeQuery();


            while (resultSet.next()) {

                OperatorParameter operatorParameter = new OperatorParameter(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("type"),
                        resultSet.getInt("operator_id"),
                        resultSet.getString("restrictions")
                );

                operatorParameter.materializeRestrictionMap(dataset, features, target, otherColumnArray);

                operatorParameters.add(operatorParameter);
            }

            return operatorParameters;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");


    }

    private void materializeRestrictionMap(String dataset, String[] features, String target, String[] otherColumnArray) {
        Map<String, Object> evaluation = (Map<String, Object>) this.getRestrictions().getOrDefault("evaluate", new HashMap<>());
        if (!evaluation.isEmpty()) {
            List<String> evaluation_list = (List<String>) evaluation.getOrDefault("choice_list", new ArrayList<>());
            if (!evaluation_list.isEmpty()) {
                if (evaluation_list.get(0).equals("other_dataset_columns")) {
                    evaluation.put("choice_list", Arrays.asList(otherColumnArray));
                    this.restrictions.put("evaluate", evaluation);
                }
            }
        }
    }

    protected void parseRestrictionJSONString(String restrictionJSON) {
        this.restrictions = new JSONObject(restrictionJSON).toMap();
    }

    protected String getRestrictionsAsJSONString() {
        return new JSONObject(this.restrictions).toString();
    }


    public static List<OperatorParameter> getOperatorParameters() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<OperatorParameter> operatorParameters = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_OPERATOR_PARAMETERS);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                OperatorParameter operatorParameter = new OperatorParameter(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getString("type"),
                        resultSet.getInt("operator_id"),
                        resultSet.getString("restrictions")
                );

                operatorParameters.add(operatorParameter);
            }

            return operatorParameters;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }
}
