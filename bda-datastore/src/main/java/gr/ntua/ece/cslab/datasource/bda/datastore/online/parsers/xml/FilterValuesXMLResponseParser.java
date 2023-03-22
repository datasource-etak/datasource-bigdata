package gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml;

import gr.ntua.ece.cslab.datasource.bda.datastore.DatastoreException;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.beans.Filter;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.beans.FilterId;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.beans.FilterValue;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class FilterValuesXMLResponseParser implements XMLResponseParser {

    private Document xml;
    private JSONObject instructions;
    private FilterId fid;
    private String filterName;

    public FilterValuesXMLResponseParser(Document xml, JSONObject instructions, FilterId fid) {
        this.xml = xml;
        this.instructions = instructions;
        this.fid = fid;
    }

    @Override
    public Collection<FilterValue> parse() {
        Element root = this.xml.getDocumentElement();
        try {
            return parse_item(root, Arrays.asList(String.format(this.instructions.getString("path"), this.fid.getId()).split(",")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HashSet<>();
    }

    private Collection<FilterValue> parse_item(Element XML, List<String> token_path) throws Exception {
        if (XML.getTagName().equals(token_path.get(0))) {
            if (token_path.size() == 1) {
                Collection<FilterValue> filter_collection = new HashSet<FilterValue>();
                filter_collection.add(create_item(XML));
                return filter_collection;
            } else if (token_path.get(1).startsWith("[list]")) {
                return parse_list(XML.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size()));
            }
            else if (token_path.get(1).startsWith("[filter]")) {
                return filter_list(XML.getElementsByTagName(token_path.get(1).substring(8).split(">")[0]), token_path.get(1).substring(8).split(">"), token_path.subList(1, token_path.size()));
            }
            else {
                return parse_item((Element) XML.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size()));
            }
        }
        else {
            throw new Exception("Parsing error");
        }
    }

    private Collection<FilterValue> parse_list(NodeList XML, List<String> token_path) throws Exception {
        Collection<FilterValue> filter_collection = new HashSet<FilterValue>();


        for (int i = 0; i < XML.getLength(); i++) {
            Element xml_item = (Element) XML.item(i);
            if (xml_item.getTagName().equals(token_path.get(0).substring(6))) {
                if (token_path.size() == 1) {
                    filter_collection.add(create_item(xml_item));
                } else if (token_path.get(1).startsWith("[list]")) {
                    filter_collection.addAll(parse_list(xml_item.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size())));
                }
                else {
                    filter_collection.addAll(parse_item((Element) xml_item.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size())));
                }
            }
            else {
                throw new Exception("Parsing error");
            }
        }

        return filter_collection;
    }

    private Collection<FilterValue> filter_list(NodeList XML, String[] filters, List<String> token_path) throws Exception {
        Collection<FilterValue> filter_collection = new HashSet<FilterValue>();


        for (int i = 0; i < XML.getLength(); i++) {
            Element xml_item = (Element) XML.item(i);
            if (xml_item.getAttribute(filters[2]).equals(filters[3])) {
                if (token_path.size() == 1) {
                    filter_collection.add(create_item(xml_item));
                } else if (token_path.get(1).startsWith("[list]")) {
                    filter_collection.addAll(parse_list(xml_item.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size())));
                } else {
                    filter_collection.addAll(parse_item((Element) xml_item.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size())));
                }

            }
        }

        return filter_collection;
    }

    private String find_info_item(Element XML, List<String> info) throws DatastoreException {

        if (info.get(0).startsWith("[attrib]")) {
            return XML.getAttribute(info.get(0).substring(8));
        }
        else if (info.get(0).startsWith("[text]")) {
            return XML.getTextContent();
        }
        else if (info.get(0).startsWith("[filter]")) {
            String[] tokens = info.get(0).substring(8).split(">");
            NodeList nodes = XML.getElementsByTagName(tokens[0]);
            Element element = null;
            if (tokens[1].equals("attrib")) {
                for (int i = 0; i < nodes.getLength(); i++) {
                    element = (Element) nodes.item(i);
                    if (element.getAttribute(tokens[2]).equals(tokens[3])) break;
                }
                return find_info_item(element, info.subList(1, info.size()));
            }

            throw new DatastoreException("Parsing error");
        }
        else {
            return find_info_item((Element) XML.getElementsByTagName(info.get(0)).item(0), info.subList(1, info.size()));
        }
    }

    private FilterValue create_item(Element XML) throws DatastoreException {

        JSONObject parseinfo = this.instructions.getJSONObject("parse");
        List<String> value_id_location = Arrays.asList(parseinfo.getString("value_id").split(","));
        List<String> value_desc_location = Arrays.asList(parseinfo.getString("value_description").split(","));

        String value_id = find_info_item(XML, value_id_location);
        String value_desc = find_info_item(XML, value_desc_location);

        return new FilterValue(value_id, value_desc);

    }
}
