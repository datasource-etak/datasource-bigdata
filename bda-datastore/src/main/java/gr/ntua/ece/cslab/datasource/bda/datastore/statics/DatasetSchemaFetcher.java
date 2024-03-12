package gr.ntua.ece.cslab.datasource.bda.datastore.statics;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MessageType;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DatasetSchemaFetcher {
    private final static Logger LOGGER = Logger.getLogger(DatasetSchemaFetcher.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    public static Map<String, String> fetchSchema(String slug, String datasetUUID) throws SQLException, SystemConnectorException {
        MessageType msg_type = null;
        try {
            msg_type = MessageType.getMessageByName(slug, datasetUUID);
        } catch (SQLException | SystemConnectorException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch schema information for dataset with uuid " + datasetUUID);
            throw e;
        }
        JSONObject ajson = new JSONObject(msg_type.getFormat());
        List<String> excluded = Arrays.asList("message_type", "source_id", "dataset_id", "alias", "payload", "slug");


        return ajson.keySet().
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
    }


    public static String[] fetchOtherDatasetColumns(String slug, String datasetUUID, String[] features, String target) throws SQLException, SystemConnectorException {
        Map<String, String> schema = fetchSchema(slug, datasetUUID);

        List<String> allColumns = new ArrayList<>(schema.keySet());

        List<String> otherColumns = allColumns.
                stream().
                filter(
                        x->!(Arrays.asList(features).contains(x)) &&
                                !(x.equals(target))
                ).
                collect(Collectors.toList());

        String[] otherColumnsArray = new String[otherColumns.size()];
        otherColumnsArray = otherColumns.toArray(otherColumnsArray);

        return otherColumnsArray;
    }

}
