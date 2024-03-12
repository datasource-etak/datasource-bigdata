package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.statics.Utils;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MasterData;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.datasource.bda.datastore.enums.DatasetStatus;
import org.json.JSONArray;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimeSeriesDataDownloader implements Runnable{
    private final static Logger LOGGER = Logger.getLogger(TimeSeriesDataDownloader.class.getCanonicalName());
    private final String uuid;
    private final String slug;
    private final Dataset dataset;
    private final List<Filter> filters;

    private final String alias;

    public TimeSeriesDataDownloader(String uuid, String slug, Dataset dataset, String alias, List<Filter> filters) {
        this.uuid = uuid;
        this.slug = slug;
        this.dataset = dataset;
        this.filters = filters;
        this.alias = alias;
    }

    @Override
    public void run() {

        String status = "";
        String name = dataset.getDataset_name();
        try {
            status = TimeSeriesData.downloadAndStoreData(uuid, slug, dataset, alias, filters);

            LOGGER.log(Level.INFO, "Dataset " + name + " was successfully downloaded.");

        } catch (Exception e) {
            String x = e.getMessage();
            LOGGER.log(Level.WARNING, "Dataset " + name + " was not downloaded properly with error : " + x);

            status = DatasetStatus.DOWNLOAD_ERROR.toString();
        } finally {
            String filter_store = Base64.getEncoder().encodeToString((new JSONArray(filters)).toString().getBytes());
            DimensionTable dt = getDimensionTable(filter_store, status);

            try {
                new StorageBackend(slug).init(
                        new MasterData(Arrays.asList(new DimensionTable[]{dt})),
                        false
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }




    }

    private DimensionTable getDimensionTable(String filter_store, String status) {
        DimensionTable dt = new DimensionTable();
        dt.setName("dataset_history_store");
        dt.setSchema(StorageBackend.datasetDTStructure);
        List<KeyValue> data = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", uuid),
                new KeyValue("alias", alias),
                new KeyValue("dataset_id", dataset.getDataset_id()),
                new KeyValue("dataset_name", dataset.getDataset_name()),
                new KeyValue("dataset_description", dataset.getDataset_description()),
                new KeyValue("source_name", dataset.getSource_name()),
                new KeyValue("associated_filter", filter_store),
                new KeyValue("status", status),
                new KeyValue("timestamp", Utils.getCurrentLocalTimestamp())
        });
        dt.setData(Arrays.asList(new Tuple[]{new Tuple(data)}));
        return dt;
    }
}
