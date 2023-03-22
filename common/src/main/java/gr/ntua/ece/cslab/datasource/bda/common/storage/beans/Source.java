/*
 * Copyright 2022 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.PostgresqlConnector;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

@XmlRootElement(name = "Source")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Source implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(Source.class.getCanonicalName());

    private String getAllDatasetIdsUrl;
    private String parseAllDatasetIdsResponsePath;
    private String sourceName;
    private Integer sourceId;
    private String filters;
    private String download;

    public Source() { }

    public Source(String getAllDatasetIdsUrl, String parseAllDatasetIdsResponsePath, String sourceName, String filters, String download) {
        this.getAllDatasetIdsUrl = getAllDatasetIdsUrl;
        this.parseAllDatasetIdsResponsePath = parseAllDatasetIdsResponsePath;
        this.sourceName = sourceName;
        this.sourceId = 0;
        this.filters = filters;
        this.download = download;
    }

    public Source(String getAllDatasetIdsUrl, String parseAllDatasetIdsResponsePath, String sourceName,
                  Integer sourceId, String filters, String download) {
        this.getAllDatasetIdsUrl = getAllDatasetIdsUrl;
        this.parseAllDatasetIdsResponsePath = parseAllDatasetIdsResponsePath;
        this.sourceName = sourceName;
        this.sourceId = sourceId;
        this.filters = filters;
        this.download = download;
    }

    private final static String INSERT_SOURCE =
            "INSERT INTO sources (source_name, get_all_dataset_ids_url, parse_all_dataset_ids_response_path, filters, download) " +
                    "VALUES (? ,?, ?::json, ?::json, ?::json) RETURNING source_id;";

    private final static String GET_SOURCES =
            "SELECT * FROM sources;";

    private final static String GET_SOURCE_BY_ID =
            "SELECT * FROM sources where source_id=?;";

    public String getGetAllDatasetIdsUrl() {
        return getAllDatasetIdsUrl;
    }

    public void setGetAllDatasetIdsUrl(String getAllDatasetIdsUrl) {
        this.getAllDatasetIdsUrl = getAllDatasetIdsUrl;
    }

    public String getParseAllDatasetIdsResponsePath() {
        return parseAllDatasetIdsResponsePath;
    }

    public void setParseAllDatasetIdsResponsePath(String parseAllDatasetIdsResponsePath) {
        this.parseAllDatasetIdsResponsePath = parseAllDatasetIdsResponsePath;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public Integer getSourceId() {
        return sourceId;
    }

    public void setSourceId(Integer sourceId) {
        this.sourceId = sourceId;
    }

    public String getFilters() {
        return filters;
    }

    public void setFilters(String filters) {
        this.filters = filters;
    }

    public String getDownload() { return download; }

    public void setDownload(String download) { this.download = download; }


    @Override
    public String toString() {
        return "Source{" + "getAllDatasetIdsUrl='" + getAllDatasetIdsUrl + '\'' + ", parseAllDatasetIdsResponsePath" +
                "='" + parseAllDatasetIdsResponsePath + '\'' + ", sourceName='" + sourceName + '\'' + ", sourceId=" + sourceId + ", filters='" + filters + '\'' + ", download='" + download + '\'' + '}';
    }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        // The object does not exist, it should be inserted.
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(INSERT_SOURCE);

            statement.setString(1, this.sourceName);
            statement.setString(2, this.getAllDatasetIdsUrl);
            statement.setString(3, this.parseAllDatasetIdsResponsePath);
            statement.setString(4, this.filters);
            statement.setString(5, this.download);

            System.out.println(statement.toString());
            ResultSet resultSet = statement.executeQuery();

            connection.commit();

            if (resultSet.next()) {
                this.sourceId = resultSet.getInt("source_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
            throw new SQLException("Failed to insert Connector object.");
        }

    }



    public static Source getSourceInfoById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_SOURCE_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                Source source = new Source(
                        resultSet.getString("get_all_dataset_ids_url"),
                        resultSet.getString("parse_all_dataset_ids_response_path"),
                        resultSet.getString("source_name"),
                        resultSet.getInt("source_id"),
                        resultSet.getString("filters"),
                        resultSet.getString("download")
                );


                return source;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Connector object not found.");
    }


    public static List<Source> getSources() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<Source> sources = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_SOURCES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {

                Source source = new Source(
                        resultSet.getString("get_all_dataset_ids_url"),
                        resultSet.getString("parse_all_dataset_ids_response_path"),
                        resultSet.getString("source_name"),
                        resultSet.getInt("source_id"),
                        resultSet.getString("filters"),
                        resultSet.getString("download")
                );

                sources.add(source);
            }

            return sources;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }

}
