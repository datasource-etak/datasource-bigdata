package gr.ntua.ece.cslab.datasource.bda.datastore.connectors;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import org.apache.directory.api.util.exception.NotImplementedException;

import gr.ntua.ece.cslab.datasource.bda.common.storage.connectors.SolrConnector;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MasterData;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Message;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;

public class DatastoreSolrConnector implements DatastoreConnector {
    
    private final static Logger LOGGER = Logger.getLogger(SolrConnector.class.getCanonicalName());

    private SolrConnector conn;

    public DatastoreSolrConnector(SolrConnector conn) {
        this.conn = conn;
    }

    public String put(Message args) throws Exception {
        throw new NotImplementedException();
    }

    public void put(MasterData args, boolean withCreate) throws Exception {
        throw new NotImplementedException();
    }

    public List<Tuple> getLast(Integer args) throws Exception {
        throw new NotImplementedException();
    }

    public List<Tuple> getFrom(Integer args) throws Exception {
        throw new NotImplementedException();
    }

    public List<Tuple> get(String args, HashMap<String,String> args2) throws Exception {
        throw new NotImplementedException();
    }

    public List<Tuple> get(HashMap<String,String> args2) throws Exception {
        throw new NotImplementedException();
    }

    public DimensionTable describe(String args) throws Exception {
        throw new NotImplementedException();
    }
    
    public List<String> list() {
        throw new NotImplementedException();
    }

}
