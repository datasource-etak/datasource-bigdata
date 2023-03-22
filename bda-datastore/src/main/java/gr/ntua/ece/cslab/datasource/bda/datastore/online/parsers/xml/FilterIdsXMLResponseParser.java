package gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml;

import gr.ntua.ece.cslab.datasource.bda.datastore.DatastoreException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


import gr.ntua.ece.cslab.datasource.bda.datastore.online.beans.FilterId;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class FilterIdsXMLResponseParser implements XMLResponseParser {

    private Document xml;
    private JSONObject instructions;

    public FilterIdsXMLResponseParser(Document xml, JSONObject instructions) {
        this.xml = xml;
        this.instructions = instructions;
    }

    @Override
    public Collection<FilterId> parse() {
        Element root = this.xml.getDocumentElement();
        try {
            return parse_item(root, Arrays.asList(this.instructions.getJSONObject("filter_id").getString("path").split(",")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HashSet<>();
    }

    private Collection<FilterId> parse_item(Element XML, List<String> token_path) throws Exception {
        if (XML.getTagName().equals(token_path.get(0))) {
            if (token_path.size() == 1) {
                Collection<FilterId> filterid_collection = new HashSet<FilterId>();
                filterid_collection.add(create_item(XML, 0));
                return filterid_collection;
            } else if (token_path.get(1).startsWith("[list]")) {
                return parse_list(XML.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size()));
            }
            else {
                return parse_item((Element) XML.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size()));
            }
        }
        else {
            throw new Exception("Parsing error");
        }
    }

    private Collection<FilterId> parse_list(NodeList XML, List<String> token_path) throws Exception {
        Collection<FilterId> filterid_collection = new HashSet<FilterId>();


        for (int i = 0; i < XML.getLength(); i++) {
            Element xml_item = (Element) XML.item(i);
            if (xml_item.getTagName().equals(token_path.get(0).substring(6))) {
                if (token_path.size() == 1) {
                    filterid_collection.add(create_item(xml_item, i));
                } else if (token_path.get(1).startsWith("[list]")) {
                    filterid_collection.addAll(parse_list(xml_item.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size())));
                }
                else {
                    filterid_collection.addAll(parse_item((Element) xml_item.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size())));
                }
            }
            else {
                throw new Exception("Parsing error");
            }
        }

        return filterid_collection;
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

    private FilterId create_item(Element XML, Integer order) throws DatastoreException {

        List<String> filter_id_location = Arrays.asList(this.instructions.getJSONObject("filter_id").getString("parse"
        ).split(","));

        String filter_id = find_info_item(XML, filter_id_location);

        return new FilterId(filter_id, order);
        
    }

}
