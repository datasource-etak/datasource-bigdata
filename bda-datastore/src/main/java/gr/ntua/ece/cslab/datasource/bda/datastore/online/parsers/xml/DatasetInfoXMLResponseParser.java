package gr.ntua.ece.cslab.datasource.bda.datastore.online.parsers.xml;

import gr.ntua.ece.cslab.datasource.bda.datastore.DatastoreException;
import gr.ntua.ece.cslab.datasource.bda.datastore.online.beans.Dataset;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.*;

public class DatasetInfoXMLResponseParser implements XMLResponseParser {


    private Document xml;
    private JSONObject instructions;
    private Integer source_id;

    public DatasetInfoXMLResponseParser(Document xml, JSONObject instructions, Integer source_id) {
        this.xml = xml;
        this.instructions = instructions;
        this.source_id = source_id;
    }

    @Override
    public Collection<Dataset> parse() {
        Element root = this.xml.getDocumentElement();
        try {
            return parse_item(root, Arrays.asList(this.instructions.getString("dataset_entity_path").split(",")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HashSet<>();
    }

    private Collection<Dataset> parse_item(Element XML, List<String> token_path) throws Exception {
        if (XML.getTagName().equals(token_path.get(0))) {
            if (token_path.size() == 1) {
                Collection<Dataset> dataset_collection = new HashSet<Dataset>();
                dataset_collection.add(create_item(XML));
                return dataset_collection;
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

    private Collection<Dataset> parse_list(NodeList XML, List<String> token_path) throws Exception {
        Collection<Dataset> dataset_collection = new HashSet<Dataset>();


        for (int i = 0; i < XML.getLength(); i++) {
            Element xml_item = (Element) XML.item(i);
            if (xml_item.getTagName().equals(token_path.get(0).substring(6))) {
                if (token_path.size() == 1) {
                    dataset_collection.add(create_item(xml_item));
                } else if (token_path.get(1).startsWith("[list]")) {
                    dataset_collection.addAll(parse_list(xml_item.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size())));
                }
                else {
                    dataset_collection.addAll(parse_item((Element) xml_item.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size())));
                }
            }
            else {
                throw new Exception("Parsing error");
            }
        }

        return dataset_collection;
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

    private Dataset create_item(Element XML) throws DatastoreException {
        JSONObject dataset_info_location = this.instructions.getJSONObject("dataset_info_location");

        String did = find_info_item(XML, Arrays.asList(dataset_info_location.getString("dataset_id").split(",")));
        String dname = find_info_item(XML, Arrays.asList(dataset_info_location.getString("dataset_name").split(",")));

        String ddesc_loc = dataset_info_location.getString("dataset_description");
        String ddesc = "N/A";
        if (!ddesc_loc.equals("")) {
            ddesc = find_info_item(XML, Arrays.asList(ddesc_loc.split(",")));
        }

        return new Dataset(did, dname, ddesc, Integer.toString(this.source_id));
    }
}
