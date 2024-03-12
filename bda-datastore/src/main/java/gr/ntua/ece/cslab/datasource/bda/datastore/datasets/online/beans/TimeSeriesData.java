package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Message;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.requests.HttpGetRequestDispatcher;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.parsers.xml.TimeSeriesDataXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.enums.DatasetStatus;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimeSeriesData implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(TimeSeriesData.class.getCanonicalName());
    private List<TimeSeriesPoint> points;

    public TimeSeriesData(List<TimeSeriesPoint> points) {
        this.points = points;
    }

    public TimeSeriesData() {
    }

    public List<TimeSeriesPoint> getPoints() {
        return points;
    }

    public void setPoints(List<TimeSeriesPoint> points) {
        this.points = points;
    }

    @Override
    public String toString() {
        return "TimeSeriesData{" + "points=" + points + '}';
    }


    public static String downloadAndStoreData(String uuid, String slug, Dataset dataset, String alias, List<Filter> filters_) throws Exception {

        List<List<Filter>> filtersForDownload = Filter.prepareFiltersForDownload(filters_);

        String status = "EMPTY";

        int datasetNumber = filtersForDownload.size();

        int i = 0;
        for (List<Filter> filters : filtersForDownload) {
            TimeSeriesData data = downloadData(dataset, filters);

            if (data != null) {
                for (TimeSeriesPoint point : data.getPoints()) {
                    if (point.getValue() != null) {
                        List<KeyValue> entries = new ArrayList<>();

                        entries.add(new KeyValue("message_type", uuid));
                        entries.add(new KeyValue("alias", alias));
                        entries.add(new KeyValue("slug", slug));
                        entries.add(new KeyValue("source_id", dataset.getSource_id()));
                        entries.add(new KeyValue("dataset_id", dataset.getDataset_id()));
                        entries.add(new KeyValue("time", point.getTime()));
                        entries.add(new KeyValue(alias, String.valueOf(point.getValue())));

                        JSONObject filter_dictionary = new JSONObject();
                        for (Filter filter : filters) {
                            entries.add(new KeyValue(filter.getName(), filter.getValues().iterator().next().getDescription()));
                            filter_dictionary.put(filter.getFid().getId(), filter.getName());
                            filter_dictionary.put(filter.getValues().iterator().next().getId(), filter.getValues().iterator().next().getDescription());
                        }

                        entries.add(new KeyValue("payload", filter_dictionary.toString()));

                        Message msg = new Message(new ArrayList<>(), entries);

                        new StorageBackend(slug).insert(msg);

                        status = DatasetStatus.DOWNLOADED.toString();
                    }
                }
            }
            i++;
            LOGGER.log(Level.INFO, "Downloaded " + 100.0*i/datasetNumber + "% of the requested data");
        }
        return status;
    }

    private static TimeSeriesData downloadData(Dataset dataset, List<Filter> filters) throws SQLException, SystemConnectorException, ParserConfigurationException, SAXException, IOException {

        Source source = Source.getSourceInfoById(Integer.parseInt(dataset.getSource_id()));

        String sep = new JSONObject(source.getFilters()).getString("filter_seperator");

        filters = Filter.sorted(filters);
        LOGGER.log(Level.INFO, "Attempting to downlaod data with filters : " + Filter.toMap(filters));
        //Collections.sort(filters, Comparator.comparing(f -> f.getFid().getIndex()));

        String filters_string = "";
        if (sep.equals("")) {
            filters_string = filters.get(0).getValues().iterator().next().getId();
        }
        else {
            for (Filter filter : new ArrayList<>(filters)) {
                filters_string = filters_string + filter.getValues().iterator().next().getId() + sep;
            }
            filters_string = filters_string.substring(0, filters_string.length() - sep.length());
        }


        JSONObject jsonObject = new JSONObject(source.getDownload());

        String url = jsonObject.getString("download_url");

        url = url.replace("{dataset_id}", dataset.getDataset_id());
        url = url.replace("{filters}", filters_string);

        LOGGER.log(Level.INFO, "Attempting to fetch data from " + url);

        HttpGetRequestDispatcher dispatcher = new HttpGetRequestDispatcher(url);

        Document doc = dispatcher.dispatch();

        if (doc != null) {
            TimeSeriesDataXMLResponseParser parser = new TimeSeriesDataXMLResponseParser(doc, jsonObject);

            Collection<TimeSeriesPoint> points = parser.parse();

            return new TimeSeriesData(new ArrayList<>(points));
        }

        return null;
    }

}
