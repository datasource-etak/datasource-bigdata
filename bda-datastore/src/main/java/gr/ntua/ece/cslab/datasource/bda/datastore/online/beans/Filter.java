package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.FilterIdsXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.FilterNamesXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.FilterValuesXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.requests.HttpGetRequestDispatcher;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import org.w3c.dom.Document;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;



public class Filter implements Serializable {

    private FilterId fid;
    private String name;
    private Collection<FilterValue> values;


    public Filter() {
    }

    public Filter(FilterId fid, String name, Collection<FilterValue> values) {
        this.fid = fid;
        this.name = name;
        this.values = values;
    }

    public FilterId getFid() {
        return fid;
    }

    public void setFid(FilterId fid) {
        this.fid = fid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collection<FilterValue> getValues() {
        return values;
    }

    public void setValues(Collection<FilterValue> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "Filter{" + "fid=" + fid + ", name='" + name + '\'' + ", values=" + values + '}';
    }

    public static Collection<Filter> getDatasetFiltersFromSourceURL(Source source, String dataset_id) throws ParserConfigurationException, SAXException, IOException {

        Collection<Filter> filters = new HashSet<>();
        JSONObject jsonObject = new JSONObject(source.getFilters());

        String filter_download_url = String.format(jsonObject.getString("filter_url"), dataset_id);

        HttpGetRequestDispatcher dispatcher = new HttpGetRequestDispatcher(filter_download_url);

        Document doc = dispatcher.dispatch();

        Collection<FilterId> filterIds = FilterId.parseFilterIds(doc, jsonObject);

        for (FilterId fid : filterIds) {
            JSONObject nameInstructions = jsonObject.getJSONObject("filter_desc").getJSONObject("name");
            String name;
            if (nameInstructions.getString("path").equals("")) {
                name = nameInstructions.getString("parse");
            }
            else {
                Collection<String> name_result = (new FilterNamesXMLResponseParser(doc, nameInstructions, fid)).parse();
                name = name_result.iterator().next();
            }

            JSONObject valueInstructions = jsonObject.getJSONObject("filter_desc").getJSONObject("values");
            Collection<FilterValue> values = (new FilterValuesXMLResponseParser(doc, valueInstructions, fid)).parse();

            Filter filter = new Filter(fid, name, values);

            System.out.println(filter.toString());

            filters.add(filter);
        }

        return filters;

    }
}
