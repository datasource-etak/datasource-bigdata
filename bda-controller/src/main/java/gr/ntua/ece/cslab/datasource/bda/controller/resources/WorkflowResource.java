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


import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.MaterializedOperator;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.MaterializedWorfklow;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.WorkflowStatus;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.WorkflowType;

import gr.ntua.ece.cslab.datasource.bda.controller.handlers.thread.JoinThreadRunnable;
import gr.ntua.ece.cslab.datasource.bda.controller.handlers.thread.WorkflowThreadRunnable;
import gr.ntua.ece.cslab.datasource.bda.datastore.statics.DatasetSchemaFetcher;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;
import org.keycloak.authorization.client.util.Http;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class holds the REST API related to datasets.
 */
@RestController
@CrossOrigin(origins = {"http://10.8.0.6","http://localhost:3000","http://10.8.0.10","http://78.108.34.54"}, allowCredentials = "true")
@RequestMapping("workflows")
public class WorkflowResource {
    private final static Logger LOGGER = Logger.getLogger(WorkflowResource.class.getCanonicalName());

    @GetMapping(value = "{slug}/types", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity getSearchResults(@PathVariable("slug") String slug) {
        List<Map<String, Object>> workflowTypesAsMap;
        try {
            workflowTypesAsMap = WorkflowType.getWorkflowTypesAsMap();
        } catch (SQLException | SystemConnectorException e) {
            return new ResponseEntity<>(new LinkedList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(workflowTypesAsMap, HttpStatus.OK);
    }

    @GetMapping(value = "{slug}/types/{workflowTypeId}/datasets", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity getWorkflowTypeIdAvailableData(
            @PathVariable("slug") String slug,
            @PathVariable("workflowTypeId") Integer workflowTypeId
    ) {

        try {

            HashMap<String, String> mapfilters = new HashMap<String, String>();
            mapfilters.put("status", "CREATED");
            List<Tuple> fetched = (new StorageBackend(slug).select("dataset_history_store", mapfilters));

            for (Tuple fetchedTuple: fetched) {
                fetchedTuple.setTuple(
                        fetchedTuple.getTuple().stream().filter(
                                x->Arrays.asList("dataset_description", "uuid", "dataset_name", "alias").contains(x.getKey())
                        ).collect(Collectors.toList())
                );
            }

            List<Map<String, Object>> datasets = fetched.stream().map(Tuple::toMap).collect(Collectors.toList());

            for (Map<String, Object> dataset : datasets) {
                String uuid = (String) dataset.get("uuid");
                Map<String, String> schema = DatasetSchemaFetcher.fetchSchema(slug, uuid);
                dataset.put("columns", schema);
            }


            WorkflowType workflowType = WorkflowType.getWorkflowTypeById(workflowTypeId);

            Map<String, Object> returnObj = new HashMap<String, Object>();
            returnObj.put("needs_target", workflowType.getNeedsTarget());
            returnObj.put("datasets", datasets);

            return new ResponseEntity<>(returnObj, HttpStatus.OK);
        } catch (Exception e) {
            LOGGER.severe(e.toString());
            return new ResponseEntity<>(new LinkedList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @PostMapping(value = "{slug}/types/{workflowTypeId}/initialize", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity initializeWorkflowTypeId(
            @PathVariable("slug") String slug,
            @PathVariable("workflowTypeId") Integer workflowTypeId,
            @RequestBody MaterializedWorfklow materializedWorfklow
            )
    {

        String uuid = UUID.randomUUID().toString();

        materializedWorfklow.setUuid(uuid);
        materializedWorfklow.setStatus(WorkflowStatus.INITIALIZED.toString());
        materializedWorfklow.setWorkflowTypeId(workflowTypeId);
        materializedWorfklow.setResult("[]");
        materializedWorfklow.setSubmittedAt("");
        materializedWorfklow.setCompletedAt("");

        try {
            materializedWorfklow.save(slug);
        } catch (Exception e) {
            LOGGER.severe(e.toString());
            LOGGER.warning("Could not store materialized workflow " + materializedWorfklow.toString());
            return new ResponseEntity<>("Could not store materialized workflow initialization.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        WorkflowType workflowType;
        try {
            workflowType = WorkflowType.getWorkflowTypeById(workflowTypeId);

            String[] otherColumnsArray = DatasetSchemaFetcher.fetchOtherDatasetColumns(
                    slug,
                    materializedWorfklow.getDatasetId(),
                    materializedWorfklow.getFeatures(),
                    materializedWorfklow.getTarget()
            );

            workflowType.fetchAll(
                    materializedWorfklow.getDatasetId(),
                    materializedWorfklow.getFeatures(),
                    materializedWorfklow.getTarget(),
                    otherColumnsArray
            );
        } catch (SQLException | SystemConnectorException e) {
            return new ResponseEntity<>(new LinkedList<>(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("uuid", uuid);
        result.put("stages", workflowType.getStages());
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "{slug}/submit", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity submitWorkflow(
            @PathVariable("slug") String slug,
            @RequestBody MaterializedWorfklow materializedWorfklow
    ) {

        LOGGER.log(Level.INFO, "Submitting workflow with id " + materializedWorfklow.getUuid() + "form user " + slug);

        try {
            materializedWorfklow.loadWorkflowInitializationData(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not fetch initialization data.");
            LOGGER.log(Level.SEVERE, e.getMessage());
            e.printStackTrace();
            return new ResponseEntity<>("Could not fetch initialization data. Workflow might not be properly initialized", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            materializedWorfklow.setSubmittedAt(Utils.getCurrentLocalTimestamp());
            materializedWorfklow.save(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not store submitted workflow.");
            e.printStackTrace();
            return new ResponseEntity<>("Could not store submitted workflow.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            materializedWorfklow.saveOperators(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not store submitted workflow.");
            e.printStackTrace();
            return new ResponseEntity<>("Could not store submitted operators.", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        WorkflowThreadRunnable workflowThreadRunnable;
        try {
             workflowThreadRunnable = new WorkflowThreadRunnable(slug, materializedWorfklow);
        } catch (SQLException | SystemConnectorException e) {
            materializedWorfklow.setStatus(WorkflowStatus.ERROR.toString());
            try {
                materializedWorfklow.save(slug);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Could not update job for error.");
                ex.printStackTrace();
                return new ResponseEntity<>("SEVERE ERROR", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            LOGGER.log(Level.SEVERE, "Could not create thread to run workflow.");
            e.printStackTrace();
            return new ResponseEntity<>("Could not create thread to run workflow.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        new Thread(workflowThreadRunnable).start();

        materializedWorfklow.setStatus(WorkflowStatus.STARTING.toString());
        try {
            materializedWorfklow.save(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not update workflow for starting status.");
            e.printStackTrace();
            return new ResponseEntity<>("Could not update workflow for starting status. Workflow might be running", HttpStatus.INTERNAL_SERVER_ERROR);
        }


        LOGGER.log(Level.INFO, "Materialized Workflow " +materializedWorfklow.toString()+ " ready to start execution.");

        return new ResponseEntity<>(materializedWorfklow.getUuid(), HttpStatus.OK);
    }


    @GetMapping(value = "{slug}/status", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_workflow_status(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        String status = null;
        try {
            status = MaterializedWorfklow.load(slug, id).getStatus();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not fetch materialized workflow with id " + id);
            e.printStackTrace();
            return new ResponseEntity<>("Error in fetching workflow.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(status, HttpStatus.OK);
    }


    @GetMapping(value = "{slug}/status/analytical", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_workflow_status_analytical(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        Map<String, Object> status;
        try {
            status = MaterializedWorfklow.getWorkflowStatusDescription(slug, id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not fetch materialized workflow with id " + id);
            e.printStackTrace();
            return new ResponseEntity<>("Error in fetching workflow.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(status, HttpStatus.OK);
    }


    @GetMapping(value = "{slug}/progress", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_workflow_progress(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        float progressPercentage;
        try {
            progressPercentage = MaterializedWorfklow.load(slug, id).progress(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not fetch progress percentage of materialized workflow with id " + id);
            e.printStackTrace();
            return new ResponseEntity<>("Error in fetching workflow percentage.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(100.0*progressPercentage, HttpStatus.OK);
    }


    @GetMapping(value = "{slug}/results/operator", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_operator_result(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        MaterializedOperator operator;
        try {
             operator = MaterializedOperator.load(slug, id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "Materialized operator [" + id + "] could not be fetched.");
            return new ResponseEntity<>("Problem when fetching operator", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(operator.getParsedResult(), HttpStatus.OK);

    }

    @GetMapping(value = "{slug}/results/workflow", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_workflow_result(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        MaterializedWorfklow workflow;
        try {
            workflow = MaterializedWorfklow.load(slug, id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "Materialized operator [" + id + "] could not be fetched.");
            return new ResponseEntity<>("Problem when fetching operator", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(workflow.getParsedResult(), HttpStatus.OK);

    }

    @GetMapping(value = "{slug}/results/report", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity get_workflow_result_report(
            @RequestParam("id") String id,
            @PathVariable("slug") String slug
    ) {

        Map<String, Object> workflow;
        try {
            workflow = MaterializedWorfklow.getWorkflowResultReport(slug, id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "Materialized operator [" + id + "] could not be fetched.");
            return new ResponseEntity<>("Problem when fetching operator", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(workflow, HttpStatus.OK);

    }









}