package gr.ntua.ece.cslab.datasource.bda.controller.handlers.thread;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.runners.LivyRunner;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.JobStage;
import gr.ntua.ece.cslab.datasource.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.datasource.bda.controller.resources.RecipeResource;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.*;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.JoinDescription;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.beans.JoinDatasetDescription;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class JoinThreadRunnable extends JobStage implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(JoinThreadRunnable.class.getCanonicalName());

    private JoinDescription joinDescription;

    private static final RecipeResource RECIPE_RESOURCE = new RecipeResource();

    private static final JobResource JOB_RESOURCE = new JobResource();

    private static Recipe initJoinSharedRecipe = null;

    private static Recipe chainJoinSharedRecipe = null;

    private static  Recipe storeJoinSharedRecipe = null;

    private String slug;

    private String uuid;

    public JoinThreadRunnable(String slug, JoinDescription joinDescription) {
        this.joinDescription = joinDescription;
        this.slug = slug;
        this.uuid = UUID.randomUUID().toString();

        loadSharedRecipesForJoins();
    }

    public JoinDescription getJoinDescription() {
        return joinDescription;
    }

    public void setJoinDescription(JoinDescription joinDescription) {
        this.joinDescription = joinDescription;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "JoinThreadRunnable{" +
                "joinDescription=" + joinDescription +
                '}';
    }

    private void loadSharedRecipesForJoins() {
        if (initJoinSharedRecipe == null) {
            try {
                initJoinSharedRecipe = Recipe.getFromSharedRecipesByName("Init Join Sequence");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


        if (chainJoinSharedRecipe == null) {
            try {
                chainJoinSharedRecipe = Recipe.getFromSharedRecipesByName("Chain Join Sequence");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (storeJoinSharedRecipe == null) {
            try {
                storeJoinSharedRecipe = Recipe.getFromSharedRecipesByName("Store Join Sequence");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<Job> createJobs() throws Exception {

        List<Job> jobs = new ArrayList<>();

        Integer previousJobId = 0;

        List<JoinDatasetDescription> datasets = joinDescription.getDatasets();

        for (int i = 0; i < datasets.size(); i++) {
            JoinDatasetDescription dataset = datasets.get(i);
            dataset.parseId(slug);


            String dataset_id = dataset.getId();
            String renamings = dataset.parseJSONRenamingString();
            String casting = dataset.parseJSONCastingString();

            RecipeArguments args = new RecipeArguments();

            List<String> dataset_to_use = new ArrayList<String>();
            dataset_to_use.add(dataset_id);

            List<String> other_args = new ArrayList<String>();
            other_args.add(renamings);
            other_args.add(casting);

            args.setMessage_types(dataset_to_use);
            args.setOther_args(other_args);

            String job_name = this.uuid + ((i == 0) ? "_init" : "_chain_" + i);
            String job_descr = ((i == 0) ? "Starting " : "Chain " + i + " ") +
                    "join job to create dataset with UUID " + this.uuid + " and alias " + joinDescription.getAlias();
            int shared_recipe_id = (i == 0) ? this.initJoinSharedRecipe.getId() : this.chainJoinSharedRecipe.getId();
            String recipe_description = "join_create_" + joinDescription.getAlias() + ((i == 0) ? "_init" : "chain_" + i);

            if (i != 0) {
                other_args.add(joinDescription.getType());

                other_args.add(new JSONArray(joinDescription.getKeys()).toString());
            }

            int recipe_id = RECIPE_RESOURCE.insert(slug, shared_recipe_id, recipe_description, args).getBody();

            Job job = new Job(job_name, job_descr, true, 0, recipe_id, "hdfs", null, previousJobId);

            int job_id = JOB_RESOURCE.insert(slug, job).getBody();

            try {
                jobs.add(Job.getJobById(slug, job_id));
            } catch (SQLException | SystemConnectorException e) {
                LOGGER.log(Level.SEVERE, "Error fetching auto-generated job for join");
                throw e;
            }

            previousJobId = job_id;
        }

        RecipeArguments args = new RecipeArguments();

        List<String> other_args = new ArrayList<String>();

        other_args.add(this.uuid);
        other_args.add(this.slug);
        other_args.add(this.joinDescription.getAlias());

        args.setOther_args(other_args);

        int recipe_id = RECIPE_RESOURCE.insert(slug, storeJoinSharedRecipe.getId(), "join_create_" + joinDescription.getAlias() + "_store", args).getBody();

        Job job = new Job(this.uuid + "_store", "", true, 0, recipe_id, "hdfs", null, previousJobId);

        int job_id = JOB_RESOURCE.insert(slug, job).getBody();

        try {
            jobs.add(Job.getJobById(slug, job_id));
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Error fetching auto-generated job for join");
            throw e;
        }

        return jobs;
    }


    public void cleanDB(List<Job> jobs) throws SQLException, SystemConnectorException {

        jobs = jobs.stream().sorted().collect(Collectors.toList());
        for (Job job : jobs) {
            int recipe_id = job.getRecipeId();

            Job.destroy(slug, job.getId());

            Recipe.destroy(slug, recipe_id);


        }
    }


    @Override
    public void run() {

        List<Job> jobs_to_execute;
        try {
            jobs_to_execute = createJobs();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in Job Creation for join");
            return;
        }

        String job_result = "";
        for (Job job : jobs_to_execute) {

            try {
                job = Job.getJobById(slug, job.getId());
            } catch (SQLException | SystemConnectorException e) {
                throw new RuntimeException(e);
            }
            LOGGER.log(Level.INFO, "Attempting to execute stage with name " + job.getName());
            LOGGER.log(Level.INFO, job.toString());
            this.setSucceeded(false);
            try {
                LivyRunner livyRunner = new LivyRunner(slug, job, this);

                Thread t = new Thread(livyRunner);

                t.start();
                t.join();

                if (!this.isSucceeded()) {
                    LOGGER.log(Level.SEVERE, "Join could not be completed");
                    break;
                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in executing join job with name " + job.getName());
                break;
            }
        }

        LOGGER.log(Level.INFO, "Join was " +  (isSucceeded() ? "": " not ") + " completed with result " + getResult());

        JSONObject msgSchema = new JSONObject(getResult());
        msgSchema.put("message_type", getUuid());
        msgSchema.put("slug", slug);
        MessageType msg_type = new MessageType(getUuid(), "Join dataset " + getJoinDescription().getAlias(), true, msgSchema.toString(), null, "");
        LOGGER.log(Level.INFO, "About to create MessageType " + msg_type.toString());
        try {
            msg_type.save(slug);
        } catch (SQLException | SystemConnectorException e) {
            throw new RuntimeException(e);
        }

        String status = "CREATED";
        if (!isSucceeded()) {
            Job aJob = null;
            try {
                aJob = Job.getJobById(slug, jobs_to_execute.get(0).getId());
            } catch (SQLException | SystemConnectorException e) {
                throw new RuntimeException(e);
            }
            LivyRunner.deleteSession(String.valueOf(aJob.getSessionId()));
            status = "ERROR";
        }

        DimensionTable dt = new DimensionTable();
        dt.setName("dataset_history_store");
        dt.setSchema(StorageBackend.datasetDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", getUuid()),
                new KeyValue("alias", joinDescription.getAlias()),
                new KeyValue("dataset_id", ""),
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
                new KeyValue("status", status),
                new KeyValue("timestamp", Utils.getCurrentLocalTimestamp())
        });
        dt.setData(Arrays.asList(new Tuple[]{new Tuple(data)}));

        try {
            new StorageBackend(slug).init(
                    new MasterData(Arrays.asList(new DimensionTable[]{dt})),
                    false
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            cleanDB(jobs_to_execute);
        } catch (SQLException | SystemConnectorException e) {
            throw new RuntimeException(e);
        }

    }
}
