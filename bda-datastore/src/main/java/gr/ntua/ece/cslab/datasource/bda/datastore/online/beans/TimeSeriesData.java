package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Message;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.DatasetInfoXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.TimeSeriesDataXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.requests.HttpGetRequestDispatcher;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class TimeSeriesData implements Serializable {

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


    public static String downloadAndStoreData(String uuid, String slug, Dataset dataset, List<Filter> filters) throws Exception {

        TimeSeriesData data = downloadData(dataset, filters);

        String status = "EMPTY";

        for (TimeSeriesPoint point : data.getPoints()) {
            if (point.getValue() != null) {
                List<KeyValue> entries = new ArrayList<>();

                entries.add(new KeyValue("message_type", uuid));
                entries.add(new KeyValue("scn_slug", slug));
                entries.add(new KeyValue("source_id", dataset.getSource_id()));
                entries.add(new KeyValue("dataset_id", dataset.getDataset_id()));
                entries.add(new KeyValue("time", point.getTime()));
                entries.add(new KeyValue("value", String.valueOf(point.getValue())));

                JSONObject filter_dictionary = new JSONObject();
                for (Filter filter : filters) {
                    System.out.println(filter.toString());
                    entries.add(new KeyValue(filter.getFid().getId(), filter.getValues().iterator().next().getId()));
                    filter_dictionary.append(filter.getFid().getId(), filter.getName());
                    filter_dictionary.put(filter.getValues().iterator().next().getId(), filter.getValues().iterator().next().getDescription());
                }

                entries.add(new KeyValue("payload", filter_dictionary.toString()));

                Message msg = new Message(new ArrayList<>(), entries);

                new StorageBackend(slug).insert(msg);

                status = "DOWNLOADED";
            }
        }

        return status;
    }

    private static TimeSeriesData downloadData(Dataset dataset, List<Filter> filters) throws SQLException, SystemConnectorException, ParserConfigurationException, SAXException, IOException {

        Source source = Source.getSourceInfoById(Integer.parseInt(dataset.getSource_id()));

        String sep = new JSONObject(source.getFilters()).getString("filter_seperator");

        Collections.sort(filters, Comparator.comparing(f -> f.getFid().getIndex()));

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

        System.out.println(url);


        HttpGetRequestDispatcher dispatcher = new HttpGetRequestDispatcher(url);

        Document doc = dispatcher.dispatch();

        TimeSeriesDataXMLResponseParser parser = new TimeSeriesDataXMLResponseParser(doc, jsonObject);

        Collection<TimeSeriesPoint> points = parser.parse();

        return new TimeSeriesData(new ArrayList<>(points));
    }

}
