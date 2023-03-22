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
package gr.ntua.ece.cslab.datasource.bda.common.storage.connectors;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import gr.ntua.ece.cslab.datasource.bda.common.Configuration;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SolrConnector implements Connector {
    private final static Logger LOGGER = Logger.getLogger(SolrConnector.class.getCanonicalName());
    
    private HttpSolrClient client;
    private String collection;
    private String url;


    public SolrConnector(String fs, Configuration configuration) {
        LOGGER.log(Level.INFO, "Initializing Solr Client.");

        url = configuration.storageBackend.getSolrURL();
        collection = configuration.storageBackend.getSolrCollection();
        client = null;
        try {
            client = new HttpSolrClient.Builder(url + collection + '/')
            .withConnectionTimeout(10000)
            .withSocketTimeout(40000)
            .build();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "SolrClient Connection Failed! Check output console.");
            throw e;
        }

        LOGGER.log(Level.INFO, "Solr Client Connector initialized.");
    }
    
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public HttpSolrClient getSolrClient() {
        return client;
    }

    public String getFirstCollection() {
        return collection;
    }

    public String getSolrUrl() {
        return url;
    }
}
