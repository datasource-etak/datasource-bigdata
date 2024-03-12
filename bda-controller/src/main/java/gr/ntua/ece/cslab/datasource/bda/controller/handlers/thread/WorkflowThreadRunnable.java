package gr.ntua.ece.cslab.datasource.bda.controller.handlers.thread;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.JobStage;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.MaterializedOperator;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.beans.MaterializedWorfklow;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.OperatorStatus;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.enums.WorkflowStatus;
import gr.ntua.ece.cslab.datasource.bda.analyticsml.runners.LivyRunner;
import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Operator;
import gr.ntua.ece.cslab.datasource.bda.controller.handlers.statics.MetadataHandler;
import gr.ntua.ece.cslab.datasource.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.datasource.bda.controller.resources.RecipeResource;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.*;
import gr.ntua.ece.cslab.datasource.bda.datastore.statics.DatasetSchemaFetcher;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class WorkflowThreadRunnable extends JobStage implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(WorkflowThreadRunnable.class.getCanonicalName());

    private final MaterializedWorfklow materializedWorfklow;

    private static final RecipeResource RECIPE_RESOURCE = new RecipeResource();

    private static final JobResource JOB_RESOURCE = new JobResource();

    private Recipe initWorkflowSharedRecipe;

    private final List<Recipe> sharedRecipes;

    private Recipe finalizeWorkflowSharedRecipe;
    private String slug;

    public WorkflowThreadRunnable(String slug, MaterializedWorfklow materializedWorfklow) throws SQLException, SystemConnectorException {
        this.materializedWorfklow  = materializedWorfklow;
        this.slug = slug;
        loadSharedRecipesForWorkflows();
        try {
            this.sharedRecipes = Recipe.getSharedRecipes();
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch shared recipes");
            throw e;
        }
    }

    public Recipe findSharedRecipeById(int id) {
        return this.sharedRecipes.stream().filter(x->x.getId() == id).collect(Collectors.toList()).get(0);
    }


    private void loadSharedRecipesForWorkflows() {
        if (initWorkflowSharedRecipe == null) {
            try {
                initWorkflowSharedRecipe = Recipe.getFromSharedRecipesByName("workflow_init");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


        if (finalizeWorkflowSharedRecipe == null) {
            try {
                finalizeWorkflowSharedRecipe = Recipe.getFromSharedRecipesByName("workflow_finalize");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private int createInitializeWorkflowJob() throws SQLException, SystemConnectorException {

        RecipeArguments args = new RecipeArguments();
        List<String> dataset_to_use = new ArrayList<String>();
        dataset_to_use.add(materializedWorfklow.getDatasetId());
        args.setMessage_types(dataset_to_use);

        List<String> otherArgs = new ArrayList<>();
        otherArgs.add(String.join(",", materializedWorfklow.getFeatures()));
        otherArgs.add(materializedWorfklow.getTarget());
        String schema = new JSONObject(DatasetSchemaFetcher.fetchSchema(slug, materializedWorfklow.getDatasetId())).toString();
        otherArgs.add(schema);
        args.setOther_args(otherArgs);

        int recipe_id = RECIPE_RESOURCE.insert(slug, initWorkflowSharedRecipe.getId(), "init_" + materializedWorfklow.getUuid().replace("-", ""), args).getBody();

        String jobNameDescr = String.valueOf(initWorkflowSharedRecipe.getId()) + "_" + materializedWorfklow.getUuid();
        Job job = new Job(jobNameDescr, jobNameDescr, true, 0, recipe_id, "hdfs", null, 0);

        return JOB_RESOURCE.insert(slug, job).getBody();
    }

    private int createFinalizeWorkflowJob(int previousJobId) {
        RecipeArguments args = new RecipeArguments();

        List<String> otherArgs = new ArrayList<>();
        otherArgs.add(slug);
        otherArgs.add(materializedWorfklow.getAlias());
        args.setOther_args(otherArgs);

        int recipe_id = RECIPE_RESOURCE.insert(slug, finalizeWorkflowSharedRecipe.getId(), "final_"+ materializedWorfklow.getUuid().replace("-", ""), args).getBody();

        String jobNameDescr = String.valueOf(finalizeWorkflowSharedRecipe.getId()) + "_" + materializedWorfklow.getUuid();
        Job job = new Job(jobNameDescr, jobNameDescr, true, 0, recipe_id, "hdfs", null, previousJobId);

        return JOB_RESOURCE.insert(slug, job).getBody();
    }

    private RecipeArguments materializeSharedRecipeParameters(Recipe sharedRecipe, MaterializedOperator mop) {
        RecipeArguments args = new RecipeArguments();

        List<String> materializedArguments = new ArrayList<>();
        for (String argument : sharedRecipe.getArgs().getOther_args()) {
            String value;
            if (argument.equals("workflow_alias")) {
                value = materializedWorfklow.getAlias();
            } else if (argument.equals("slug")) {
                value = slug;
            } else {
                value = String.valueOf(mop.getParameters().get(argument));
            }
            materializedArguments.add(value);
        }

        args.setOther_args(materializedArguments);

        return args;
    }

    private int createJobFromOperator(MaterializedOperator mop, int previousJobId) {

        Operator op = mop.getOperator();
        int sharedRecipeId = op.getSharedRecipeId();

        Recipe sharedRecipe = findSharedRecipeById(sharedRecipeId);

        RecipeArguments args = materializeSharedRecipeParameters(sharedRecipe, mop);

        int recipe_id = RECIPE_RESOURCE.insert(slug, sharedRecipeId, mop.getUuid().replace("-", ""), args).getBody();

        String jobNameDescr = String.valueOf(sharedRecipeId) + "_" + mop.getUuid();
        Job job = new Job(jobNameDescr, jobNameDescr, true, 0, recipe_id, "hdfs", null, previousJobId);

        return JOB_RESOURCE.insert(slug, job).getBody();
    }

    private List<Job> createJobs() throws Exception {
        List<Job> jobs = new ArrayList<>();

        int previousJobId = createInitializeWorkflowJob();

        try {
            jobs.add(Job.getJobById(slug, previousJobId));
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Error fetching auto-generated job for workflow initialization");
            throw e;
        }

        List<MaterializedOperator> operators = materializedWorfklow.getOperators();

        for (int i = 0; i < operators.size(); i++) {
            MaterializedOperator operator = operators.get(i);
            int job_id = createJobFromOperator(operator, previousJobId);
            try {
                jobs.add(Job.getJobById(slug, job_id));
            } catch (SQLException | SystemConnectorException e) {
                LOGGER.log(Level.SEVERE, "Error fetching auto-generated job for workflow materialized operator " + operator.getUuid());
                throw e;
            }
            previousJobId = job_id;
        }

        int job_id = createFinalizeWorkflowJob(previousJobId);

        try {
            jobs.add(Job.getJobById(slug, job_id));
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Error fetching auto-generated job for finalized materliazed workflow operator.");
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

        try {
            materializedWorfklow.fetchAbstractOperators(slug);
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch abstract operators.");
            LOGGER.log(Level.SEVERE, e.getMessage());
            throw new RuntimeException(e);
        }

        List<Job> jobs_to_execute;
        try {
            jobs_to_execute = createJobs();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in Job Creation for join");
            return;
        }

        String job_result = "";
        for (int k = 0 ; k < jobs_to_execute.size(); k++) {

            Job job = jobs_to_execute.get(k);


            try {
                job = Job.getJobById(slug, job.getId());
            } catch (SQLException | SystemConnectorException e) {
                throw new RuntimeException(e);
            }
            LOGGER.log(Level.INFO, "Attempting to execute workflow operator with job name " + job.getName());
            LOGGER.log(Level.INFO, job.toString());
            this.setSucceeded(false);
            try {

                LivyRunner livyRunner = new LivyRunner(slug, job, this);

                Thread t = new Thread(livyRunner);

                t.start();

                if (k==0) {
                    materializedWorfklow.setStatus(WorkflowStatus.RUNNING.toString());
                    materializedWorfklow.save(slug);
                }

                if ((k > 0) && (k < (jobs_to_execute.size() - 1))) {
                    MaterializedOperator mop = materializedWorfklow.getOperators().get(k-1);
                    mop.setSubmittedAt(Utils.getCurrentLocalTimestamp());
                    mop.setStatus(OperatorStatus.RUNNING.toString());
                    mop.save(slug);
                }

                t.join();

                if (!this.isSucceeded()) {
                    LOGGER.log(Level.SEVERE, "Workflow could not be completed");
                    break;
                } else {
                    if ((k > 0) && (k < (jobs_to_execute.size() - 1))) {
                        MaterializedOperator mop = materializedWorfklow.getOperators().get(k-1);
                        mop.setStatus(OperatorStatus.COMPLETED.toString());
                        mop.setCompletedAt(Utils.getCurrentLocalTimestamp());
                        String edittedResult = MetadataHandler.storeInternalDatasetMetadataIfNeeded(slug, getResult());
                        mop.setResult(edittedResult);
                        mop.save(slug);
                    }
                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in executing join operator with name " + job.getName());
                break;
            }
        }

        LOGGER.log(Level.INFO, "Workflow was " +  (isSucceeded() ? "": " not ") + " completed with result " + getResult());


        String status = WorkflowStatus.COMPLETED.toString();
        if (!isSucceeded()) {
            Job aJob = null;
            try {
                aJob = Job.getJobById(slug, jobs_to_execute.get(0).getId());
            } catch (SQLException | SystemConnectorException e) {
                throw new RuntimeException(e);
            }
            LivyRunner.deleteSession(String.valueOf(aJob.getSessionId()));
            status = WorkflowStatus.ERROR.toString();
        }

        materializedWorfklow.setStatus(status);
        String edittedResult = MetadataHandler.storeInternalDatasetMetadataIfNeeded(slug, getResult());
        materializedWorfklow.setResult(edittedResult);
        materializedWorfklow.setCompletedAt(Utils.getCurrentLocalTimestamp());
        try {
            materializedWorfklow.save(slug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not update workflow result and status");
            throw new RuntimeException(e);
        }

        try {
            cleanDB(jobs_to_execute);
        } catch (SQLException | SystemConnectorException e) {
            throw new RuntimeException(e);
        }


    }

}
