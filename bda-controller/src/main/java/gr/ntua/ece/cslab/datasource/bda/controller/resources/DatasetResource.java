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


import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.*;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.controller.handlers.thread.JoinThreadRunnable;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.*;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.JoinDescription;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.beans.JoinDatasetDescription;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Dataset;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.DatasetDownloadDescription;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Filter;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.TimeSeriesDataDownloader;
import gr.ntua.ece.cslab.datasource.bda.datastore.enums.DatasetStatus;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class holds the REST API related to datasets.
 */
@RestController
@CrossOrigin(origins = {"http://10.8.0.6","http://localhost:3000","http://10.8.0.10","http://78.108.34.54", "http://79.129.229.82"}, allowCredentials = "true")
@RequestMapping("datasets")
public class DatasetResource {
    private final static Logger LOGGER = Logger.getLogger(DatasetResource.class.getCanonicalName());

    @GetMapping(value = "{slug}/online/search", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity getSearchResults(
        @RequestParam("q") String query
    ) {
        try {
            return new ResponseEntity<>(
                    Dataset.getDatasetsByQuery(query),
                    HttpStatus.OK
            );
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Dataset Search went wrong.");
            e.printStackTrace();
        }
        return new ResponseEntity<>(
                new LinkedList<>(),
                HttpStatus.INTERNAL_SERVER_ERROR
        );
    }

    @GetMapping(value = "{slug}/online/search/{source}/{dataset}/filters", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
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

        List<Filter> filters;
        try {
            filters = Filter.getDatasetFiltersFromSourceURL(source, dataset_id);

            if (filters == null)
                return new ResponseEntity<>("Could not fetch filters. Source is currently unavailable. Please try again later.", HttpStatus.BAD_GATEWAY);
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
        return new ResponseEntity<>(Filter.toMap(filters), HttpStatus.OK);
    }

    @PostMapping(value = "{slug}/download/{source}/{dataset}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity downloadDataset(
            @PathVariable("slug") String slug,
            @PathVariable("source") String source_id,
            @PathVariable("dataset") String dataset_id,
            @RequestBody DatasetDownloadDescription datasetDownloadDescription

            ) {

        Map<String, List<String>> selectedFilters = datasetDownloadDescription.getSelectedFilters();
        String alias = datasetDownloadDescription.getAlias();

        LOGGER.log(Level.INFO, "selectedFilters: " + selectedFilters.toString());
        DatastoreResource r = new DatastoreResource();

        if (!StringUtils.isAlphanumeric(alias.replace("_", "")))
            return new ResponseEntity<>("Alias only allows alphanumeric and the '_' character", HttpStatus.BAD_REQUEST);

        if (!r.getTable("dataset_history_store",
                "alias:" + alias,
                slug).isEmpty()) {
            return new ResponseEntity<>("Dataset alias has been used for another dataset", HttpStatus.CONFLICT);
        }


        Source source = null;
        try {
            source = Source.getSourceInfoById(Integer.valueOf(source_id));
        } catch (SQLException e) {
            e.printStackTrace();
            return new ResponseEntity<>("Error fetching requested source.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SystemConnectorException e) {
            return new ResponseEntity<>("Error fetching requested source.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        Collection<Filter> allFilters;
        try {
            allFilters = Filter.getDatasetFiltersFromSourceURL(source, dataset_id);
            if (allFilters == null)
                return new ResponseEntity<>("Could not fetch filters. Source is currently unavailable. Please try again later.", HttpStatus.BAD_GATEWAY);
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

        List<Filter> filters = Filter.selectFilterValues(allFilters, selectedFilters);

        String filter_store = Filter.filterListBase64encode(filters);


        HashMap<String, String> mapfilters = new HashMap<String, String>();
        mapfilters.put("dataset_id", dataset_id);
        mapfilters.put("associated_filter", filter_store);

        List<Tuple> existing =
                null;
        try {
            existing = (new StorageBackend(slug).select("dataset_history_store", mapfilters));
        } catch (Exception e) {

            return new ResponseEntity<>("Something went wrong", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        LOGGER.log(Level.INFO, "Filter base64 encode : " + filter_store);
        LOGGER.log(Level.INFO, "Queried for filter results : " + existing.stream().map(Tuple::toMap).collect(Collectors.toList()));
        if (!existing.isEmpty()) {
            return new ResponseEntity<>("This dataset and filters combination has already been downloaded. The dataset will not be downloaded again", HttpStatus.FOUND);
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
        format_json.put("message_type", uuid);
        format_json.put("slug", slug);
        format_json.put("alias", "string");
        format_json.put("source_id", "string");
        format_json.put("dataset_id", "string");
        format_json.put("time", "string");
        format_json.put(alias, "double");
        for (Filter filter : filters) {
            format_json.put(filter.getName(), "string");
        }
        format_json.put("payload", "");

        String format = format_json.toString();
        String description = dataset.getDataset_name();

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


        TimeSeriesDataDownloader downloader = new TimeSeriesDataDownloader(uuid, slug, dataset, alias, filters);

        Thread downloadingThread = new Thread(downloader);

        downloadingThread.start();

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
                new KeyValue("status", DatasetStatus.DOWNLOADING.toString()),
                new KeyValue("timestamp", Utils.getCurrentLocalTimestamp())
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

    @GetMapping(value = "{slug}/fetch/by-id", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_dataset_by_id(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {
        try {
            MessageType msgType = MessageType.getMessageByName(slug, id);
            JSONObject jsonObject = new JSONObject(msgType.getFormat());
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("message_type", id);

            List<Tuple> fetched = (new StorageBackend(slug).select(mapfilters));
            List<String> excluded = Arrays.asList("message_type", "source_id", "dataset_id", "alias", "payload", "slug");

            if (fetched.isEmpty()) {
                return new ResponseEntity<>(new ArrayList<>(), HttpStatus.NOT_FOUND);
            }

            LOGGER.log(Level.INFO, "Will decode dataset entries with schema : " + jsonObject);
            return new ResponseEntity<>(
                    fetched.
                            stream().
                            map(x ->
                                    new Tuple(
                                            x.getTuple().
                                                    stream().
                                                    filter(y->!excluded.contains(y.getKey())).
                                                    collect(Collectors.toList())
                                    )
                            ).
                            map(x->x.toMap(jsonObject)).
                            collect(Collectors.toList()),
                    HttpStatus.OK
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/fetch/by-alias", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_dataset_by_alias(
            @RequestParam("alias") String alias,
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("alias", alias);
            List<Tuple> fetched = (new StorageBackend(slug).select(mapfilters));
            List<String> excluded = Arrays.asList("message_type", "source_id", "dataset_id", "alias", "payload", "slug");

            if (fetched.isEmpty()) {
                return new ResponseEntity<>(new ArrayList<>(), HttpStatus.NOT_FOUND);
            }

            String id = fetched.get(0).getTuple().stream().filter(x->x.getKey().equals("message_type")).collect(Collectors.toList()).get(0).getValue();
            MessageType msgType = MessageType.getMessageByName(slug, id);
            JSONObject jsonObject = new JSONObject(msgType.getFormat());

            return new ResponseEntity<>(
                    fetched.
                            stream().
                            map(x ->
                                    new Tuple(
                                            x.getTuple().
                                                    stream().
                                                    filter(y->!excluded.contains(y.getKey())).
                                                    collect(Collectors.toList())
                                    )
                            ).
                            map(x->x.toMap(jsonObject)).
                            collect(Collectors.toList()),
                    HttpStatus.OK
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/description", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_datasets_description(
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            List<Tuple> fetched = (new StorageBackend(slug).select("dataset_history_store", mapfilters));


            return new ResponseEntity<>(
                    fetched.
                            stream().
                            map(Tuple::toMap).
                            map(y->y.entrySet().stream().collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    ((Map.Entry<String, Object>) entry).getKey().equals("associated_filter") ?
                                                            Filter.mapFilterRepresentationFromBase64String((String) ((Map.Entry<String, Object>) entry).getValue()) :
                                                            ((Map.Entry<?, ?>) entry).getValue(),
                                            (a, b) -> b,
                                            () -> new HashMap<String, Object>()
                                    )
                            )).
                            collect(Collectors.toList()),
                    HttpStatus.OK
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/description/by-id", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_datasets_description_by_id(
            @RequestParam("id") String uuid,
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("uuid", uuid);
            List<Tuple> result = (new StorageBackend(slug).select("dataset_history_store", mapfilters));

            if (result.isEmpty())
                return new ResponseEntity<>(new ArrayList<>(), HttpStatus.NOT_FOUND);

            Map<String, Object> description =
                    result.
                            get(0).toMap().entrySet().stream().collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    ((Map.Entry<String, Object>) entry).getKey().equals("associated_filter") ?
                                                            Filter.mapFilterRepresentationFromBase64String((String) ((Map.Entry<String, Object>) entry).getValue()) :
                                                            ((Map.Entry<?, ?>) entry).getValue(),
                                            (a, b) -> b,
                                            () -> new HashMap<String, Object>()
                                    ));

            return new ResponseEntity<>(description, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/description/by-alias", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_datasets_description_by_alias(
            @RequestParam("alias") String alias,
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("alias", alias);

            List<Tuple> result = (new StorageBackend(slug).select("dataset_history_store", mapfilters));

            if (result.isEmpty())
                return new ResponseEntity<>(new ArrayList<>(), HttpStatus.NOT_FOUND);

            Map<String, Object> description =
                    result.
                            get(0).toMap().entrySet().stream().collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    ((Map.Entry<String, Object>) entry).getKey().equals("associated_filter") ?
                                                            Filter.mapFilterRepresentationFromBase64String((String) ((Map.Entry<String, Object>) entry).getValue()) :
                                                            ((Map.Entry<?, ?>) entry).getValue(),
                                            (a, b) -> b,
                                            () -> new HashMap<String, Object>()
                                    ));

            return new ResponseEntity<>(description, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/schema/by-id", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_dataset_schema(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        try {
            MessageType msg_type = MessageType.getMessageByName(slug, id);
            JSONObject ajson = new JSONObject(msg_type.getFormat());
            List<String> excluded = Arrays.asList("message_type", "source_id", "dataset_id", "alias", "payload", "slug");


            Map<String, String> schema = ajson.keySet().
                    stream().
                    filter(x->!excluded.contains(x)).
                    collect(
                            Collectors.toMap(
                                    y->y,
                                    ajson::getString,
                                    (a, b) -> b,
                                    HashMap::new
                            )
                    );

            return new ResponseEntity<>(schema, HttpStatus.OK);

        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "SQLException : " +  e.getMessage());
            return new ResponseEntity<>("Dataset with id  " + id + "does not exist", HttpStatus.NOT_FOUND);
        } catch (SystemConnectorException e) {
            LOGGER.log(Level.WARNING, "SystemConnectorException" + e.getMessage());
            return new ResponseEntity<>("Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "{slug}/join", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity join_datasets(
            @RequestBody JoinDescription joinDescription,
            @PathVariable("slug") String slug
    ) {

        try {

            if (!joinDescription.validate(slug).equals("valid")) {
                return new ResponseEntity<>("Invalid Join Dataset Description.", HttpStatus.BAD_REQUEST);
            }

            JoinThreadRunnable joinThreadRunnable = new JoinThreadRunnable(slug, joinDescription);
            new Thread(joinThreadRunnable).start();

            DimensionTable dt = new DimensionTable();
            dt.setName("dataset_history_store");
            dt.setSchema(StorageBackend.datasetDTStructure);
            List<KeyValue> data = Arrays.asList(new KeyValue[]{
                    new KeyValue("uuid", joinThreadRunnable.getUuid()),
                    new KeyValue("alias", joinDescription.getAlias()),
                    new KeyValue("dataset_id", joinThreadRunnable.getUuid()),
                    new KeyValue("dataset_name", joinDescription.getAlias()),
                    new KeyValue("dataset_description",
                            "Joined dataset from datasets " +
                                    joinDescription.getDatasets().stream().
                                            map(JoinDatasetDescription::getId).
                                            collect(Collectors.joining(","))  +
                                    " on keys : " + joinDescription.getKeys().toString()
                            ),
                    new KeyValue("source_name", "JOIN"),
                    new KeyValue("associated_filter", ""),
                    new KeyValue("status", "CREATING"),
                    new KeyValue("timestamp", Utils.getCurrentLocalTimestamp())
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

            return new ResponseEntity<>(joinThreadRunnable.getUuid(), HttpStatus.OK);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception occured on requested join : " + e.getMessage());
        }

        return new ResponseEntity<>("Join initiation failed", HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/join/types", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity join_datasets_types(@PathVariable("slug") String slug) {
        return new ResponseEntity<>(new String[]{"inner", "outer"}, HttpStatus.OK);
    }

    @GetMapping(value = "{slug}/join/datatypes", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity join_datasets_data_types(@PathVariable("slug") String slug) {
        return new ResponseEntity<>(new String[]{"int", "string", "double"}, HttpStatus.OK);
    }


    @GetMapping(value = "{slug}/status/by-id", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_datasets_status_by_id(
            @RequestParam("id") String uuid,
            @PathVariable("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("uuid", uuid);
            String status =
                    (new StorageBackend(slug).select("dataset_history_store", mapfilters)).
                            get(0).getTuple().stream().filter(x->x.getKey().equals("status")).collect(Collectors.toList()).get(0).getValue();

            return new ResponseEntity<>(status, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "{slug}/sourceinfo", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_source_info_by_dataset_id(
            @RequestParam("id") String uuid,
            @PathVariable("slug") String slug
    ) {

        try {
            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("uuid", uuid);
            String source_name =
                    (new StorageBackend(slug).select("dataset_history_store", mapfilters)).
                            get(0).getTuple().stream().filter(x->x.getKey().equals("source_name")).collect(Collectors.toList()).get(0).getValue();

            List<Source> sources = Source.getSources();

            Source source =
                    sources.
                            stream().
                            filter(x->x.getSourceName().equals(source_name)).
                            collect(Collectors.toList()).get(0);
            return new ResponseEntity<>(source, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(new ArrayList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

}