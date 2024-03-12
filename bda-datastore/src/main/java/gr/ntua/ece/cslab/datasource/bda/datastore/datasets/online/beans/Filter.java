package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans;

import gr.ntua.ece.cslab.datasource.bda.common.storage.beans.Source;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.requests.HttpGetRequestDispatcher;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.parsers.xml.FilterNamesXMLResponseParser;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.parsers.xml.FilterValuesXMLResponseParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;


public class Filter implements Serializable, Comparable<Filter> {

    private final static Logger LOGGER = Logger.getLogger(Filter.class.getCanonicalName());

    private FilterId fid;
    private String name;
    private List<FilterValue> values;


    public Filter() {
    }

    public Filter(FilterId fid, String name, List<FilterValue> values) {
        this.fid = fid;
        this.name = name;
        this.values = values;
    }

    public Filter(Map<String, Object> ajson) {
        if (ajson.get("fid") != null) {
            this.fid = new FilterId((Map<String, Object>) ajson.get("fid"));
        } else {
            LOGGER.log(Level.WARNING, "Could not parse non-null fid");
        }

        if (ajson.get("name") != null) {
            this.name = (String) ajson.get("name");
        } else {
            LOGGER.log(Level.WARNING, "Could not parse non-null fid");
        }

        if (ajson.get("values") != null) {
            values = ((List<Map<String, Object>>) ajson.get("values")).stream().map(FilterValue::new).collect(Collectors.toList());
        } else {
            LOGGER.log(Level.WARNING, "Could not parse non-null values");
        }
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

    public List<FilterValue> getValues() {
        return values;
    }

    public void setValues(List<FilterValue> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "Filter{" + "fid=" + fid + ", name='" + name + '\'' + ", values=" + values + '}';
    }

    public static List<Filter> getDatasetFiltersFromSourceURL(Source source, String dataset_id) throws ParserConfigurationException, SAXException, IOException {

        List<Filter> filters = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(source.getFilters());

        String filter_download_url = String.format(jsonObject.getString("filter_url"), dataset_id);

        HttpGetRequestDispatcher dispatcher = new HttpGetRequestDispatcher(filter_download_url);

        Document doc = dispatcher.dispatch();

        if (doc == null)
            return null;

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
            List<FilterValue> values = new ArrayList<>((new FilterValuesXMLResponseParser(doc, valueInstructions, fid)).parse());

            Filter filter = new Filter(fid, name, values);

            filters.add(filter);
        }

        return filters;

    }

    public static List<List<Filter>> prepareFiltersForDownload(List<Filter> filters) {

        LOGGER.log(Level.INFO, "Filters to download : "  + Filter.toMap(filters).toString());
        List<List<Filter>> serializedFilterList = new ArrayList<>();

        for (Filter filter: filters) {
            List<List<Filter>> tempSerializedFilterList = serializedFilterList;
            serializedFilterList = new ArrayList<>();

            for (FilterValue v : filter.getValues()) {

                if (tempSerializedFilterList.isEmpty()) {
                    List<Filter> filterList = new ArrayList<>();

                    Collection<FilterValue> filter_collection = new HashSet<>();
                    filter_collection.add(v);
                    Filter f = new Filter(filter.getFid(), filter.getName(), new ArrayList<>(filter_collection));
                    filterList.add(f);

                    serializedFilterList.add(filterList);

                } else {
                    for (List<Filter> prevList : tempSerializedFilterList) {
                        List<Filter> listcopy = new ArrayList<>(prevList);
                        Collection<FilterValue> filter_collection = new HashSet<>();
                        filter_collection.add(v);
                        Filter f = new Filter(filter.getFid(), filter.getName(), new ArrayList<>(filter_collection));
                        listcopy.add(f);
                        serializedFilterList.add(listcopy);
                    }
                }
            }
        }

        LOGGER.log(Level.INFO, "Filters parsed " + serializedFilterList.stream().map(Filter::toMap).collect(Collectors.toList()));
        return serializedFilterList;
    }

    public List<String> valueStringList() {
        return this.getValues().stream().map(x->x.getDescription()).collect(Collectors.toList());
    }

    public static Map<String, List<String>> toMap(List<Filter> filters) {
        return filters.stream().
                collect(
                        Collectors.toMap(
                                Filter::getName,
                                Filter::valueStringList,
                                (a, b) -> b,
                                () -> new HashMap<>(filters.size())
                        )
                );
    }

    public static List<Filter> selectFilterValues (Collection<Filter> allFilters, Map<String, List<String>> selection) {
        LOGGER.log(Level.INFO, "Selected filters : " + selection.toString());

        return allFilters.stream().map(x -> (
                new Filter(
                        x.getFid(),
                        x.getName(),
                        x.getValues().stream().filter(y->selection.get(x.getName()).contains(y.getDescription())).collect(Collectors.toList())
                )
        )).collect(Collectors.toList());
    }


    @Override
    public int compareTo(Filter o) {
        this.values = FilterValue.sorted(this.values);
        o.setValues(FilterValue.sorted(o.getValues()));

        int filterOrder = this.fid.getIndex().compareTo(o.getFid().getIndex());

        if (filterOrder != 0)
            return filterOrder;
        else {
            List<FilterValue> thisValues = this.values;
            List<FilterValue> otherValues = o.getValues();

            int listsize = (new Integer(thisValues.size())).compareTo(otherValues.size());
            if (listsize != 0) {
                return listsize;
            } else {
                for (int i = 0; i < thisValues.size(); i++) {
                    int compValue = thisValues.get(i).compareTo(otherValues.get(i));
                    if (compValue != 0)
                        return compValue;
                }
            }
        }
        return 0;
    }

    public static List<Filter> sorted(List<Filter> filters) {
        return filters.stream().sorted().collect(Collectors.toList());
    }

    public static String filterListBase64encode(List<Filter> filters) {
        filters = sorted(filters);
        return Base64.encodeBase64String((new JSONArray(filters)).toString().getBytes());
    }

    public static Map<String, List<String>> mapFilterRepresentationFromBase64String(String base64String) {
        if (!base64String.isEmpty()) {
            String decodedString = new String(Base64.decodeBase64(base64String));
            JSONArray decodedObj = new JSONArray(decodedString);

            List<Filter> lf = decodedObj.toList().stream().map(x -> (HashMap<String, Object>) x).map(Filter::new).collect(Collectors.toList());
            lf = sorted(lf);
            return Filter.toMap(lf);
        }
        return new HashMap<>();
    }

    public static List<Filter> listFilterFromBase64String(String base64String) {
        if (!base64String.isEmpty()) {
            String decodedString = new String(Base64.decodeBase64(base64String));
            JSONArray decodedObj = new JSONArray(decodedString);

            List<Filter> lf = decodedObj.toList().stream().map(x-> (HashMap<String, Object>) x).map(Filter::new).collect(Collectors.toList());
            return sorted(lf);
        }
        return new ArrayList<>();
    }
}
