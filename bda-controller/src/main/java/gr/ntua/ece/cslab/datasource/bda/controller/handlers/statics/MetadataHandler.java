package gr.ntua.ece.cslab.datasource.bda.controller.handlers.statics;

import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.DatasetDescription;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Filter;
import gr.ntua.ece.cslab.datasource.bda.datastore.enums.DatasetStatus;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.crypto.Data;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetadataHandler {
    private final static Logger LOGGER = Logger.getLogger(MetadataHandler.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    public static String storeInternalDatasetMetadataIfNeeded(String slug, String jsonOutput) {

        if (jsonOutput.equals("")) {
            return "[]";
        }

        JSONArray outputs = new JSONArray(jsonOutput);

        JSONArray finalOutputs = new JSONArray();

        for (int i = 0; i < outputs.length(); i++) {
            JSONObject out = outputs.getJSONObject(i);

            if (out.getString("type").equals("dataset") ||
                    out.getString("type").equals("multiple_scatterplots_with_line")) {

                MessageType msg_type = new MessageType(
                        out.getString("uuid"),
                        out.getString("alias"),
                        true,
                        out.getJSONObject("schema").toString(),
                        null,
                        ""
                );
                LOGGER.log(Level.INFO, "About to create MessageType " + msg_type.toString());
                try {
                    msg_type.save(slug);
                    LOGGER.log(Level.INFO, "MessageType " + msg_type.toString() + " stored");
                } catch (SQLException | SystemConnectorException e) {
                    LOGGER.log(Level.SEVERE, "MessageType " + msg_type.toString() + " not stored");
                    throw new RuntimeException(e);
                }

                DatasetDescription datasetDescription = new DatasetDescription(
                  out.getString("uuid"),
                        out.getString("alias"),
                        out.getString("uuid"),
                        out.getString("name"),
                        out.getString("dataset_description"),
                        "WORKFLOW_INTERNAL",
                        new ArrayList<>(),
                        DatasetStatus.CREATED.toString(),
                        Utils.getCurrentLocalTimestamp()
                );

                datasetDescription.save(slug);

                out = new JSONObject();
                out.put("type", "dataset");
                out.put("name", datasetDescription.getDatasetName());
                out.put("uuid", datasetDescription.getUuid());
            }

            finalOutputs.put(out);

        }

        return finalOutputs.toString();


    }

}
