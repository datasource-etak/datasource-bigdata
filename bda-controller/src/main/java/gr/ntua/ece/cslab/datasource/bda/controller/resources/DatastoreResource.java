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

package gr.ntua.ece.cslab.datasource.bda.controller.resources;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.datasource.bda.common.Configuration;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.ExecutionLanguage;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.DbInfo;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.HDFSConnector;
import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.SolrConnector;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;

import gr.ntua.ece.cslab.datasource.bda.datastore.beans.*;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Dataset;

import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Filter;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.TimeSeriesData;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;
import com.google.common.base.Splitter;
import org.apache.commons.io.IOUtils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class holds the REST API of the datastore object.
 */
@RestController
@CrossOrigin(origins = {"http://10.8.0.6","http://localhost:3000","http://10.8.0.10","http://78.108.34.54"}, allowCredentials = "true")
@RequestMapping("datastore")
public class DatastoreResource {
    private final static Logger LOGGER = Logger.getLogger(DatastoreResource.class.getCanonicalName());

    /**
     * Perform initial upload of libraries and shared recipes in HDFS and populate
     * shared_recipes, execution_engines and execution_languages tables.
     */
    @PostMapping(value = "init", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> initialUploads() {

        Configuration configuration = Configuration.getInstance();
        ClassLoader classLoader = RunnerInstance.class.getClassLoader();
        ClassLoader datastoreClassLoader = StorageBackend.class.getClassLoader();


        
        try {
            ExecutionLanguage lang = new ExecutionLanguage("python");
            lang.save();
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Failed to populate execution languages table.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            ExecutionEngine eng = new ExecutionEngine("python3", "/usr/bin/python3", true, "{}");
            eng.save();
            eng = new ExecutionEngine("spark-livy", "http://bda-livy:8998", false, "{}");
            eng.save();
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Failed to populate execution engines table.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        if (configuration.execEngine.getRecipeStorageType().startsWith("hdfs")) {
            // Use HDFS storage for recipes and libraries.
            HDFSConnector connector = null;
            try {
                connector = (HDFSConnector)
                        SystemConnector.getInstance().getHDFSConnector();
            } catch (SystemConnectorException e) {
                e.printStackTrace();
                new ResponseEntity<>("Failed to get HDFS connector.", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            org.apache.hadoop.fs.Path outputFilePath = null;

            String[] recipe_names = new String[] {"RecipeDataLoader.py", "algorithmic_library.py"};
            org.apache.hadoop.fs.FileSystem fs = connector.getFileSystem();
            InputStream fileInStream = null;
            org.apache.hadoop.fs.FSDataOutputStream outputStream = null;

            for (String recipe_name :recipe_names) {



                fileInStream = classLoader.getResourceAsStream(recipe_name);

                byte[] recipeBytes = new byte[0];
                try {
                    recipeBytes = IOUtils.toByteArray(fileInStream);

                    fileInStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to create stream of Recipes library file.", HttpStatus.INTERNAL_SERVER_ERROR);
                }

                // Create HDFS file path object.
                outputFilePath =
                        new org.apache.hadoop.fs.Path("/" + recipe_name);

                // Write to HDFS.
                try {
                    outputStream = fs.create(
                            outputFilePath
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to create Recipes library file in HDFS.", HttpStatus.INTERNAL_SERVER_ERROR);
                }

                try {
                    outputStream.write(recipeBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to upload Recipes library to HDFS.", HttpStatus.INTERNAL_SERVER_ERROR);
                } finally {
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }

            try {
                String path = System.getProperty("user.dir").replaceFirst("bda-controller","bda-analytics-ml/src/main/resources/shared_recipes/");
                org.apache.hadoop.fs.Path folderToUpload = new org.apache.hadoop.fs.Path(path);

                // Create HDFS file path object.
                outputFilePath = new org.apache.hadoop.fs.Path("/");
                fs.copyFromLocalFile(folderToUpload, outputFilePath);
            } catch (Exception e) {
                e.printStackTrace();
                new ResponseEntity<>("Failed to upload shared recipes to HDFS.", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            String[] jar_links = configuration.execEngine.getSparkConfJars().split(",");
            for (String link: jar_links) {

                try {
                    fileInStream = new URL(link).openStream();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Spark jar clients download failed!! Please check the URLs");
                }

                byte[] jarBytes = new byte[0];
                try {
                    jarBytes = IOUtils.toByteArray(fileInStream);

                    fileInStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to create Spark jar stream.", HttpStatus.INTERNAL_SERVER_ERROR);
                }
                String[] jar_name = link.split("/");
                outputFilePath =
                        new org.apache.hadoop.fs.Path("/" + jar_name[jar_name.length - 1]);

                try {
                    outputStream = fs.create(
                            outputFilePath
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to create Spark jar file in HDFS.", HttpStatus.INTERNAL_SERVER_ERROR);
                }

                try {
                    outputStream.write(jarBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                    new ResponseEntity<>("Failed to upload Spark jars to HDFS.", HttpStatus.INTERNAL_SERVER_ERROR);
                } finally {
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                RecipeArguments args = new RecipeArguments();
                List<String> args_list = new LinkedList<>();
                args_list.add("labelColumnName");
                args_list.add("maxIter");
                args_list.add("regParam");
                args_list.add("elasticNetParam");
                args.setOther_args(args_list);
                List<String> input_df_args_list = new LinkedList<>();
                input_df_args_list.add("eventLogMessageType1");
                args.setMessage_types(input_df_args_list);

                Recipe r = new Recipe("Linear Regression model train", "Simple regression training algorithm using Spark MLlib",
                        1, "hdfs:///shared_recipes/linear_regression_train.py", 2, args);
                r.save_as_shared();

                r = new Recipe("Binomial Logistic Regression model train", "Simple binary classification training algorithm using Spark MLlib",
                        1, "hdfs:///shared_recipes/binomial_logistic_regression_train.py", 2, args);
                r.save_as_shared();

                args = new RecipeArguments();
                r = new Recipe("Linear Regression prediction", "Simple regression prediction using Spark MLlib",
                        1, "hdfs:///shared_recipes/linear_regression_predict.py", 2, args);
                r.save_as_shared();
                r = new Recipe("Binomial Logistic Regression prediction", "Simple binary classification prediction using Spark MLlib",
                        1, "hdfs:///shared_recipes/binomial_logistic_regression_predict.py", 2, args);
                r.save_as_shared();

                args = new RecipeArguments();
                args_list = new LinkedList<>();
                args_list.add("renaming");
                args_list.add("type_cast");
                args.setOther_args(args_list);
                input_df_args_list = new LinkedList<>();
                input_df_args_list.add("dataset1");
                args.setMessage_types(input_df_args_list);

                r = new Recipe("Init Join Sequence", "Export first table to be joined in a join sequence",
                        1, "hdfs:///shared_recipes/init_dataframe_join.py", 2, args);
                r.save_as_shared();

                args_list.add("join_type");
                args_list.add("join_keys");

                args.setOther_args(args_list);
                r = new Recipe("Chain Join Sequence", "Chain a join sequence",
                        1, "hdfs:///shared_recipes/chain_dataframe_join.py", 2, args);
                r.save_as_shared();

                args = new RecipeArguments();
                args_list = new LinkedList<>();
                args_list.add("message_type");
                args_list.add("slug");
                args_list.add("alias");
                args.setOther_args(args_list);


                r = new Recipe("Store Join Sequence", "Store join result to hbase",
                        1, "hdfs:///shared_recipes/store_dataframe_join.py", 2, args);
                r.save_as_shared();

            } catch (Exception e) {
                e.printStackTrace();
                new ResponseEntity<>("Failed to populate shared recipes table.", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        else {
            // Upload any shared recipes for local execution
        }

        InputStream url_stream = datastoreClassLoader.getResourceAsStream("urls.csv");

        BufferedReader reader = new BufferedReader(new InputStreamReader(url_stream));

        while(true) {
            try {
                if (!reader.ready()) break;
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Could not make resource reader ready");
            }
            try {
                String line = reader.readLine();

                if ((line == null) || (line.length() == 0))
                    break;

                String[] tokens = line.split(";");

                String sourceName = tokens[0];
                String getAllDatasetsUrl = tokens[1];
                String parseAllDatasetsInfo = tokens[2];
                String filters = tokens[3];
                String download = tokens[4];

                Source source = new Source(getAllDatasetsUrl,parseAllDatasetsInfo, sourceName, filters, download);

                source.save();

            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Error in reading line from resource.");
            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "SQL error in inserting source to DB.");
            } catch (SystemConnectorException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "System connector exception occured in source insertion.");
            }

        }

        String solrUrlString = configuration.storageBackend.getSolrURL();
        String collectionName = configuration.storageBackend.getSolrCollection();

        // Init Solr Collection
        HttpURLConnection con = null;
        try {

            String numShards = configuration.storageBackend.getSolrNumShards();
            String replicationFactor = configuration.storageBackend.getSolrReplicationFactor();

            URL url = new URL(solrUrlString +"admin/collections?action=CREATE&name="+collectionName+"&numShards="+numShards+"&replicationFactor="+replicationFactor);
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.getInputStream();

            LOGGER.log(Level.INFO, "Sent collection create request to Solr. Response Code : " + con.getResponseCode());

        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("IOException occured", HttpStatus.INTERNAL_SERVER_ERROR);
        } finally {
            con.disconnect();
        }


        InputStream solr_json_description_stream = datastoreClassLoader.getResourceAsStream("schema.json");

        BufferedReader json_reader = new BufferedReader(new InputStreamReader(solr_json_description_stream));

        String solr_json_description = "";

        while(true) {
            try {
                if (!json_reader.ready()) break;
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Could not make resource reader for solr json schema ready");
            }
            try {
                String line = json_reader.readLine();

                if ((line == null) || (line.length() == 0)) break;

                solr_json_description += line;

            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Error in reading line from resource.");
            }
        }

        LOGGER.log(Level.INFO, "Schema " + solr_json_description + " parsed.");

        con = null;
        try {

            URL url = new URL(solrUrlString + collectionName+"/schema");
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            try(OutputStream os = con.getOutputStream()) {
                byte[] input = solr_json_description.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            LOGGER.log(Level.INFO, "Sent collection create request to Solr. Response Code : " + con.getResponseCode());

        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (ProtocolException e1) {
            e1.printStackTrace();
        } catch (MalformedURLException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        finally {
            con.disconnect();
        }


        SolrConnector connector = null;
        try {
            connector = (SolrConnector) SystemConnector.getInstance().getSolrConnector();
            HttpSolrClient client = connector.getSolrClient();


            try {
                List<Source> sources = Source.getSources();

                for (Source source : sources) {
                    Collection<Dataset> datasetCollection = Dataset.getDatasetsFromSourceURL(source);

                    LOGGER.log(Level.INFO, "Datasets' Information  from source " + source.getSourceName() + " are successfully downloaded.");


                    //client.addBeans(collectionName, datasetCollection, 10000);
                    if (!datasetCollection.isEmpty())
                        client.addBeans(datasetCollection,10000);

                    LOGGER.log(Level.INFO, "Datasets' Information  from source " + source.getSourceName() + " are successfully successfully added to Solr.");

                }
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (SystemConnectorException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (SolrServerException e) {
                e.printStackTrace();
            }
        } catch (SystemConnectorException e) {
            e.printStackTrace();
        }


        return ResponseEntity.ok("");
    }

    /**
     * Create new repository databases/schemas/tables.
     * @param info a repository description.
     */
    @PostMapping(
            consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> createNewRepository(@RequestBody DbInfo info) {
        LOGGER.log(Level.INFO, info.toString());

        String details = "";

        try {
            List<Connector> connectors = Connector.getConnectors();

            boolean correctConnector = false;
            for (Connector connector : connectors) {
                if (connector.getId() == info.getConnectorId()) {
                    correctConnector = true;
                }
            }

            if (correctConnector) {
                info.save();
            } else {
                LOGGER.log(Level.WARNING, "Bad connector id provided!");
                new ResponseEntity<>("Connector id does not exist.", HttpStatus.BAD_REQUEST);
            }

            details = Integer.toString(info.getId());
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Could not register new repository.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            StorageBackend.createNewDB(info);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.INFO, "Clearing repository registry and databases after failure.");
            try {
                StorageBackend.destroyDBs(info);
            } catch (Exception e1) {
            }
            try {
                DbInfo.destroy(info.getId());
            } catch (Exception e1) {
                e1.printStackTrace();
                LOGGER.log(Level.SEVERE, "Could not clear registry, after databases creation failed!");
            }
            new ResponseEntity<>("Could not create new repository.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.ok(details);
    }

    /**
     * Returns all the registered repositories.
     */
    @GetMapping(
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<DbInfo> getDbInfoView() {
        List<DbInfo> dbInfos = new LinkedList<DbInfo>();

        try {
            dbInfos = DbInfo.getDbInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dbInfos;
    }

    /**
     * Returns information about a specific repository.
     * @param id the repository registry id.
     */
    @GetMapping(value = "{repoId}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public DbInfo getInfo(@PathVariable("repoId") Integer id) {
        DbInfo info = null;

        try {
            info = DbInfo.getDbInfoById(id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return info;
    }

    /**
     * Destroy an repository databases/schemas/tables.
     * @param repoId a repository id.
     */
    @DeleteMapping(value = "{repoId}")
    public ResponseEntity<?> destroyRepository(@PathVariable("repoId") Integer repoId) {

        DbInfo info = null;
        Boolean externalConnectors = null;

	// TODO: unschedule jobs and destroy live sessions
        try {
            info = DbInfo.getDbInfoById(repoId);
            externalConnectors = MessageType.checkExternalMessageTypesExist(info.getSlug());
            StorageBackend.destroyDBs(info);
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Could not destroy repository databases.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            DbInfo.destroy(repoId);
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Could not destroy repository.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        //PubSubConnector.getInstance().reloadSubscriptions(info.getSlug(), false);
        //if (externalConnectors)
        //    PubSubConnector.getInstance().reloadSubscriptions(info.getSlug(), true);

        return ResponseEntity.ok("");
    }

    /**
     * Message insertion method
     * @param m the message to insert
     */
    @PostMapping(value = "{slug}",
            consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> insert(@PathVariable("slug") String slug,
                           @RequestBody Message m) {
        LOGGER.log(Level.INFO, m.toString());
        try {
            new StorageBackend(slug).insert(m);
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Could not insert new message.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return ResponseEntity.ok("");
    }


    /**
     * Responsible for datastore bootstrapping
     * @param masterData the schema of the dimension tables along with their content
     * @return a response for the status of bootstrapping
     */
    @PostMapping(value = "{slug}/boot",
            consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> bootstrap(@PathVariable("slug") String slug,
                              @RequestBody MasterData masterData) {
        try {
            new StorageBackend(slug).init(masterData, true);
        } catch (Exception e) {
            e.printStackTrace();
            new ResponseEntity<>("Could not insert master data.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return ResponseEntity.ok("");
    }

    /**
     * Returns the filtered content of a given dimension table
     * @param tableName is the name of the table to search
     * @param filters contains the names and values of the columns to be filtered
     * @return the selected content of the dimension table
     */
    @GetMapping(value = "{slug}/dtable", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<Tuple> getTable(
            @RequestParam("tableName") String tableName,
            @RequestParam("filters") String filters,
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters;
            if(filters != null && !filters.isEmpty()) {
                Map<String, String> map = Splitter.on(';').withKeyValueSeparator(":").split(filters);
                mapfilters = new HashMap<String, String>(map);
            }
            else {
                mapfilters = new HashMap<String, String>();
            }
            return new StorageBackend(slug).select(tableName, mapfilters);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }


    /**
     * Returns the schema of all dimension tables
     * @return a list of the dimension tables schemas
     */
    @GetMapping(value = "{slug}/schema", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<DimensionTable> getSchema(@PathVariable("slug") String slug) {
        try {
            List<String> tables = new StorageBackend(slug).listTables();
            List<DimensionTable> res = new LinkedList<>();
            for (String table: tables){
                DimensionTable schema = new StorageBackend(slug).getSchema(table);
                LOGGER.log(Level.INFO, "Table: " +table + ", Columns: "+ schema.getSchema().getColumnNames());
                res.add(schema);
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    /**
     * Returns the last entries (i.e., messages) stored in the event log.
     * @param type one of days, count
     * @param n the number of days/messages to fetch
     * @return the denormalized messages
     */
    @GetMapping(value = "{slug}/entries", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<Tuple> getEntries(@RequestParam("type") String type,
                                  @RequestParam("n") Integer n,
                                  @PathVariable("slug") String slug) {
        try {
            return new StorageBackend(slug).fetch(type,n);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    /**
     * Returns the filtered entries (i.e., messages) stored in the event log.
     * @param filters contains the names and values of the columns to be filtered
     * @return the denormalized messages
     */
    @GetMapping(value = "{slug}/select", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<Tuple> getSelectedEntries(
            @RequestParam("filters") String filters,
            @PathVariable("slug") String slug
    ) {
        try {
            Map<String,String> map= Splitter.on(';').withKeyValueSeparator(":").split(filters);
            HashMap<String, String> mapfilters = new HashMap<String, String>(map);
            return new StorageBackend(slug).select(mapfilters);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    @GetMapping(value = "{slug}/search", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<Dataset> getSearchResults(
        @RequestParam("q") String query
    ) {
        try {
            return Dataset.getDatasetsByQuery(query);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Dataset Search went wrong.");
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    @GetMapping(value = "{slug}/search/{source}/{dataset}/filters", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity getDatasetFilters(
            @PathVariable("source") String source_id,
            @PathVariable("dataset") String dataset_id
    ) {

        Source source = null;
        try {
            source = Source.getSourceInfoById(Integer.valueOf(source_id));
        } catch (SQLException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error fetching requested source.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SystemConnectorException e) {
            return new ResponseEntity<>("Error fetching requested source.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        Collection<Filter> filters;
        try {
            filters = Filter.getDatasetFiltersFromSourceURL(source, dataset_id);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error parsing filter response from url.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SAXException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error parsing filter response from url.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error parsing filter response from url.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(filters, HttpStatus.OK);
    }

    @PostMapping(value = "{slug}/download/{source}/{dataset}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity downloadDataset(
            @PathVariable("slug") String slug,
            @PathVariable("source") String source_id,
            @PathVariable("dataset") String dataset_id,
            @RequestBody List<Filter> filters,
            @RequestBody String alias
    ) {

        if (!StringUtils.isAlphanumeric(alias.replace("_", "")))
            return new ResponseEntity<>("Alias only allows alphanumeric and the '_' character", HttpStatus.BAD_REQUEST);

        if (getTable("dataset_history_store",
                "alias:" + alias,
                slug).size() > 0) {
            return new ResponseEntity<>("Dataset alias has been used for another dataset", HttpStatus.CONFLICT);
        }


        String filter_store = Base64.getEncoder().encodeToString((new JSONArray(filters)).toString().getBytes());

        if (this.getTable("dataset_history_store",
                "dataset_id:" + dataset_id + ";associated_filter:" + filter_store,
                slug).size() > 0) {
            return new ResponseEntity<>("This dataset and filters combination has already been used.", HttpStatus.FOUND);
        }

        Dataset dataset = null;
        try {
             dataset = Dataset.getDatasetById(dataset_id, source_id);
        } catch (SystemConnectorException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error in fetching dataset description", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        String uuid = UUID.randomUUID().toString();


        String msg_type_name = uuid;
        JSONObject format_json = new JSONObject();
        format_json.append("message_type", uuid);
        format_json.append("alias", alias);
        format_json.append("scn_slug", slug);
        format_json.append("source_id", "string");
        format_json.append("dataset_id", "string");
        format_json.append("time", "string");
        format_json.append("value", "double");
        for (Filter filter : filters) {
            format_json.append(filter.getFid().getId(), "string");
        }
        format_json.append("payload", "");

        String format = format_json.toString();
        String description = dataset.getSource_name();

        MessageType msg_type = new MessageType(msg_type_name, description, true, format, null, "");

        try {
            msg_type.save(slug);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ResponseEntity<>("edw1", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SystemConnectorException e) {
            e.printStackTrace();
            return new ResponseEntity<>("edw2", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        String status;
        try {
            status = TimeSeriesData.downloadAndStoreData(uuid, slug, dataset, alias, filters);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error on downloading and storing the data", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        DimensionTable dt = new DimensionTable();
        dt.setName("dataset_history_store");
        dt.setSchema(StorageBackend.datasetDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", uuid),
                new KeyValue("alias", alias),
                new KeyValue("dataset_id", dataset_id),
                new KeyValue("dataset_name", dataset.getDataset_name()),
                new KeyValue("dataset_description", dataset.getDataset_description()),
                new KeyValue("source_name", dataset.getSource_name()),
                new KeyValue("associated_filter", filter_store),
                new KeyValue("status", status)
        });
        dt.setData(Arrays.asList(new Tuple[]{new Tuple(data)}));


        try {
            new StorageBackend(slug).init(
                    new MasterData(Arrays.asList(new DimensionTable[]{dt})),
                    false
            );
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error when storing downloaded dataset info", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(uuid, HttpStatus.OK);
    }
}