package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml.FilterIdsXMLResponseParser;
import org.json.JSONObject;
import org.w3c.dom.Document;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;

public class FilterId implements Serializable {

    private String id;
    private Integer index;

    public FilterId() {
    }

    public FilterId(String id, Integer index) {
        this.id = id;
        this.index = index;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "FilterId{" + "id='" + id + '\'' + ", index=" + index + '}';
    }

    public static Collection<FilterId> parseFilterIds(Document doc, JSONObject jsonObject) throws ParserConfigurationException, SAXException, IOException {

        Collection<FilterId> filterIds;

        if (jsonObject.getJSONObject("filter_id").getString("path").equals("")) {
            filterIds = new HashSet<>();
            FilterId fid = new FilterId(jsonObject.getJSONObject("filter_id").getString("parse"), 0);
            filterIds.add(fid);
        } else{
            FilterIdsXMLResponseParser parser = new FilterIdsXMLResponseParser(doc, jsonObject);
            filterIds = parser.parse();
        }

        return filterIds;

    }
}
