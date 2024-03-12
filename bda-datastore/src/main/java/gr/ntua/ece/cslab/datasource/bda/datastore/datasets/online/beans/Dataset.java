package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.parsers.xml.DatasetInfoXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.requests.HttpGetRequestDispatcher;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.SolrConnector;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

public class Dataset implements Serializable {
    private String dataset_id;
    private String dataset_name;
    private String dataset_description;
    private String source_id;
    private String source_name;

    public Dataset() { }

    public Dataset(String id, String name, String description, String source_id) {
        this.dataset_id = id;
        this.dataset_name = name;
        this.dataset_description = description;
        this.source_id = source_id;
    }

    public Dataset(SolrDocument doc) {
        this.dataset_id = (String) doc.getFieldValue("dataset_id");
        this.dataset_name = (String) doc.getFieldValue("dataset_name");
        this.dataset_description = (String) doc.getFieldValue("dataset_description");
        this.source_id = String.valueOf(doc.getFieldValue("source_id"));
    }

    public String getDataset_id() {
        return this.dataset_id;
    }

    @Field("dataset_id")
    public void setDataset_id(String id) {
        this.dataset_id = id;
    }

    public String getDataset_name() {
        return this.dataset_name;
    }
    
    @Field("dataset_name")
    public void setDataset_name(String name) {
        this.dataset_name = name;
    }

    public String getSource_id() {
        return this.source_id;
    }
    
    @Field("source_id")
    public void setSource_id(Integer sourceID) {
        this.source_id = String.valueOf(sourceID);
    }

    public String getDataset_description() {
        return this.dataset_description;
    }

    @Field("dataset_description") 
    public void setDataset_description(String description) {
        this.dataset_description = description;
    }


    public String getSource_name() {
        return source_name;
    }

    public void setSource_name(String source_name) {
        this.source_name = source_name;
    }

    @Override
    public String toString() {
        return "Dataset{" + "dataset_id='" + dataset_id + '\'' + ", dataset_name='" + dataset_name + '\'' + ", " +
                "dataset_description='" + dataset_description + '\'' + ", source_id='" + source_id + '\'' + '}';
    }

    public static List<Dataset> getDatasetsByQuery(String query) throws SystemConnectorException {
        SolrConnector connector = (SolrConnector) SystemConnector.getInstance().getSolrConnector();
        HttpSolrClient client = connector.getSolrClient();

        try {
            final Map<String, String> queryParamMap = new HashMap<String, String>();
            queryParamMap.put("q", "dataset_name:" + query); // Our query params
            queryParamMap.put(CommonParams.ROWS, Integer.toString(Integer.MAX_VALUE));

            MapSolrParams queryParams = new MapSolrParams(queryParamMap);
            final QueryResponse response = client.query(queryParams); // Query the server and get a response

            List<Dataset> results = response.getBeans(Dataset.class);

            for (Dataset dataset : results) {
                dataset.retrieveSourceName();
            }
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    public static Dataset getDatasetById(String did, String source_id) throws SystemConnectorException {
        SolrConnector connector = (SolrConnector) SystemConnector.getInstance().getSolrConnector();
        HttpSolrClient client = connector.getSolrClient();

        try {
            final Map<String, String> queryParamMap = new HashMap<String, String>();
            queryParamMap.put("q", "dataset_id:" + did); // Our query params
            queryParamMap.put(CommonParams.ROWS, Integer.toString(Integer.MAX_VALUE));

            MapSolrParams queryParams = new MapSolrParams(queryParamMap);
            final QueryResponse response = client.query(queryParams); // Query the server and get a response

            List<Dataset> results = response.getBeans(Dataset.class);

            Dataset dataset = null;
            for (Dataset d : results) {
                if (d.getDataset_id().equals(did) && d.getSource_id().equals(source_id)) {
                    dataset = d;
                    dataset.retrieveSourceName();
                }
            }

            return dataset;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Dataset();
    }

    public static Collection<Dataset> getDatasetsFromSourceURL(Source source) throws ParserConfigurationException, SAXException, IOException {

        HttpGetRequestDispatcher dispatcher = new HttpGetRequestDispatcher(source.getGetAllDatasetIdsUrl());

        Document doc = dispatcher.dispatch();

        JSONObject jsonObject = new JSONObject(source.getParseAllDatasetIdsResponsePath());

        DatasetInfoXMLResponseParser parser = new DatasetInfoXMLResponseParser(doc, jsonObject, source.getSourceId());

        return parser.parse();

    }

    public void retrieveSourceName() throws SystemConnectorException, SQLException {
        Source source = null;
        try {
            source = Source.getSourceInfoById(Integer.valueOf(this.source_id));
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } catch (SystemConnectorException e) {
            e.printStackTrace();
            throw e;
        }
        this.source_name = source.getSourceName();
    }

}
