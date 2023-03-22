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

package gr.ntua.ece.cslab.datasource.bda.common;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;



import java.lang.IllegalStateException;



/**
 * This class holds all the configuration options used by the Big Data Analytics component as a whole
 * (including the various subsystems).
 */
public class Configuration {
    private static Logger LOGGER =Logger.getLogger(Configuration.class.getCanonicalName());

    private static Configuration configuration = null;

    public final StorageBackend storageBackend;
    public final ExecutionEngine execEngine;
    public final PubSubServer pubsub;
    public final SecurityBackend securityBackend;
    public final KPIBackend kpiBackend;

    public class StorageBackend {
        // TODO: Should add username/password for every StorageBackend.
        //       Modify Constructor accordingly.
        private String eventLogURL, dimensionTablesURL, bdaDatabaseURL, dbUsername, dbPassword;
        private String dbPrivilegedUsername, dbPrivilegedPassword;

        private String eventLogMaster = null;
        private String eventLogQuorum = null;

        private String hdfsMasterURL, hdfsUsername, hdfsPassword;

        private String solrURL, solrCollection, solrNumShards, solrReplicationFactor;

        public StorageBackend() {

        }

        public String getEventLogURL() { return eventLogURL; }

        public String getDimensionTablesURL() { return dimensionTablesURL; }

        public String getBdaDatabaseURL() { return bdaDatabaseURL; }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }

        public String getDbPrivilegedUsername() { return dbPrivilegedUsername; }

        public String getDbPrivilegedPassword() { return dbPrivilegedPassword; }

        public String getEventLogMaster() { return eventLogMaster; }

        public String getEventLogQuorum() { return eventLogQuorum; }

        public String getHDFSMasterURL() { return hdfsMasterURL; }

        public String getHDFSUsername() { return hdfsUsername; }

        public String getHDFSPassword() { return hdfsPassword; }

        public String getSolrURL() { return solrURL; }

        public String getSolrCollection() { return solrCollection; }

        public String getSolrNumShards() { return solrNumShards; }
        
        public String getSolrReplicationFactor() { return solrReplicationFactor; }

    }
    public class ExecutionEngine {
        private String sparkMaster;
        private String sparkDeployMode;
        private String sparkConfJars;
        private String sparkConfPackages;
        private String sparkConfRepositories;
        private String sparkConfDriverMemory;
        private String sparkConfExecutorCores;
        private String sparkConfExecutorMemory;

        private String recipeStorageLocation;
        private String recipeStorageType;

        private String livyURL;

        public ExecutionEngine(){}

        public String getSparkMaster() { return sparkMaster; }

        public String getSparkDeployMode() { return sparkDeployMode; }

        public String getSparkConfJars() { return sparkConfJars; }

        public String getSparkConfPackages() { return sparkConfPackages; }

        public String getSparkConfRepositories() { return sparkConfRepositories; }

        public String getSparkConfDriverMemory() { return sparkConfDriverMemory; }

        public String getSparkConfExecutorCores() { return sparkConfExecutorCores; }

        public String getSparkConfExecutorMemory() { return sparkConfExecutorMemory; }

        public String getRecipeStorageLocation() { return recipeStorageLocation; }

        public String getRecipeStorageType() { return recipeStorageType; }

        public String getLivyURL() { return livyURL; }
    }
    public class PubSubServer {
        private String authHash, certificateLocation;

        public PubSubServer(){
        }

        public String getAuthHash() { return authHash; }

        public String getCertificateLocation() { return certificateLocation; }

    }

    public class SecurityBackend {
        private Boolean authEnabled, sslEnabled;
        private String authServerUrl, realm, clientId, clientSecret, bdaUsername, bdaPassword;
        private String masterClient, masterUser, masterPass, commonUserRoleName;

        public SecurityBackend() { }

        public Boolean isAuthEnabled() { return authEnabled; }

        public Boolean isSSLEnabled() { return sslEnabled; }

        public String getAuthServerUrl() { return authServerUrl; }

        public String getRealm() { return realm; }

        public String getClientId() { return clientId; }

        public String getClientSecret() { return clientSecret; }

        public String getBdaUsername() { return bdaUsername; }

        public String getBdaPassword() { return bdaPassword; }

        public String getMasterClient() { return masterClient; }

        public String getMasterUser() { return masterUser; }

        public String getMasterPass() { return masterPass; }

        public String getCommonUserRoleName() { return commonUserRoleName; }
    }

    public class KPIBackend {
        private String dbUrl, dbUsername, dbPassword;

        public String getDbUrl() { return dbUrl; }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }

    }

    public static Configuration getInstance() throws IllegalStateException {
        if (configuration == null) {
            throw new IllegalStateException("Configuration not initialized.");
        }

        return configuration;
    }

    private Configuration() {
        this.storageBackend = new StorageBackend();
        this.pubsub = new PubSubServer();
        this.securityBackend = new SecurityBackend();
        this.kpiBackend = new KPIBackend();
        this.execEngine = new ExecutionEngine();
    }

    /**
     * parseConfiguration constructs a Configuration object through reading the provided configuration file.
     * @param configurationFile the path of the configuration file
     * @return the Configuration object
     */
    public static Configuration parseConfiguration(String configurationFile){
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(configurationFile));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }

        Configuration conf = new Configuration();

        // Dimension Tables Configuration.
        conf.storageBackend.dbUsername = properties.getProperty("backend.db.dimension.username");
        conf.storageBackend.dbPassword = properties.getProperty("backend.db.dimension.password");
        conf.storageBackend.dbPrivilegedUsername = properties.getProperty("backend.db.dimension.privileged_username");
        conf.storageBackend.dbPrivilegedPassword = properties.getProperty("backend.db.dimension.privileged_password");
        conf.storageBackend.dimensionTablesURL = properties.getProperty("backend.db.dimension.url");

        // BDA Database Configuration.

        // TODO: Should add username/password for every StorageBackend.
        // conf.storageBackend.bdaDatabaseUsername = properties.getProperty("backend.db.bda.username");
        // conf.storageBackend.bdaDatabasePassword = properties.getProperty("backend.db.bda.password");

        conf.storageBackend.bdaDatabaseURL = properties.getProperty("backend.db.bda.url");

        // Event Log Configuration.

        // TODO: Should add username/password for every StorageBackend.
        // conf.storageBackend.eventLogUsername = properties.getProperty("backend.db.event.username");
        // conf.storageBackend.eventLogPassword = properties.getProperty("backend.db.event.password");

        conf.storageBackend.eventLogURL = properties.getProperty("backend.db.event.url");
        conf.storageBackend.eventLogMaster = properties.getProperty("backend.db.event.master.host");
        conf.storageBackend.eventLogQuorum = properties.getProperty("backend.db.event.quorum");

        conf.storageBackend.hdfsMasterURL = properties.getProperty("backend.hdfs.master.url");

        // Solr Configuration.
        conf.storageBackend.solrURL = properties.getProperty("backend.solr.url");
        conf.storageBackend.solrCollection = properties.getProperty("backend.solr.first-collection");
        conf.storageBackend.solrNumShards = properties.getProperty("backend.solr.num-shards");
        conf.storageBackend.solrReplicationFactor = properties.getProperty("backend.solr.replication-factor");

        // Pub/Sub Configuration.
        conf.pubsub.authHash = properties.getProperty("pubsub.authhash");
        conf.pubsub.certificateLocation = properties.getProperty("pubsub.certificate.location");

        // Keycloak Auth Configuration.
        conf.securityBackend.sslEnabled = Boolean.valueOf(properties.getProperty("server.ssl.enabled"));
        conf.securityBackend.authEnabled = Boolean.valueOf(properties.getProperty("keycloak.enabled"));
        conf.securityBackend.authServerUrl = properties.getProperty("keycloak.auth-server-url");
        conf.securityBackend.realm = properties.getProperty("keycloak.realm");
        conf.securityBackend.clientId = properties.getProperty("keycloak.resource");
        conf.securityBackend.clientSecret = properties.getProperty("keycloak.credentials.secret");
        conf.securityBackend.bdaUsername = "";
        conf.securityBackend.bdaPassword = "";

        conf.securityBackend.masterClient = properties.getProperty("bda.keycloak.master.client_id");
        conf.securityBackend.masterUser = properties.getProperty("bda.keycloak.master.admin.user");
        conf.securityBackend.masterPass = properties.getProperty("bda.keycloak.master.admin.password");
        conf.securityBackend.commonUserRoleName = properties.getProperty("bda.keycloak.users.role_name");

        // KPIDB configuration
        conf.kpiBackend.dbUrl = properties.getProperty("kpi.db.url");
        conf.kpiBackend.dbUsername = properties.getProperty("kpi.db.username");
        conf.kpiBackend.dbPassword = properties.getProperty("kpi.db.password");

        // Execution engine configuration
        conf.execEngine.sparkMaster = properties.getProperty("spark.master");
        conf.execEngine.sparkDeployMode = properties.getProperty("spark.deploy_mode");
        conf.execEngine.sparkConfJars = properties.getProperty("spark.conf.jars");
        conf.execEngine.sparkConfPackages = properties.getProperty("spark.conf.packages");
        conf.execEngine.sparkConfRepositories = properties.getProperty("spark.conf.repositories");
        conf.execEngine.sparkConfDriverMemory = properties.getProperty("spark.conf.driver_memory");
        conf.execEngine.sparkConfExecutorCores = properties.getProperty("spark.conf.executor_cores");
        conf.execEngine.sparkConfExecutorMemory = properties.getProperty("spark.conf.executor_memory");
        conf.execEngine.recipeStorageLocation = properties.getProperty("engines.recipe.storage.prefix");
        conf.execEngine.recipeStorageType = properties.getProperty("engines.recipe.storage.type");
        conf.execEngine.livyURL = properties.getProperty("spark.livy.url");

        configuration = conf;

        return conf;
    }
}
