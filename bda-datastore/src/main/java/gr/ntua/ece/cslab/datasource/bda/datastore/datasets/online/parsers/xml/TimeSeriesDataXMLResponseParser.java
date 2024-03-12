package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.parsers.xml;

import gr.ntua.ece.cslab.datasource.bda.datastore.DatastoreException;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.TimeSeriesPoint;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class TimeSeriesDataXMLResponseParser implements XMLResponseParser {


    private Document xml;
    private JSONObject instructions;

    public TimeSeriesDataXMLResponseParser(Document xml, JSONObject instructions) {
        this.xml = xml;
        this.instructions = instructions;
    }

    @Override
    public Collection<TimeSeriesPoint> parse() {
        Element root = this.xml.getDocumentElement();
        try {
            return parse_item(root, Arrays.asList(this.instructions.getString("path").split(",")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HashSet<>();
    }

    private Collection<TimeSeriesPoint> parse_item(Element XML, List<String> token_path) throws Exception {
        if (XML.getTagName().equals(token_path.get(0))) {
            if (token_path.size() == 1) {
                Collection<TimeSeriesPoint> timeSeriesCollection = new HashSet<TimeSeriesPoint>();
                timeSeriesCollection.add(create_item(XML));
                return timeSeriesCollection;
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

    private Collection<TimeSeriesPoint> parse_list(NodeList XML, List<String> token_path) throws Exception {
        Collection<TimeSeriesPoint> timeSeriesCollection = new HashSet<TimeSeriesPoint>();


        for (int i = 0; i < XML.getLength(); i++) {
            Element xml_item = (Element) XML.item(i);
            if (xml_item.getTagName().equals(token_path.get(0).substring(6))) {
                if (token_path.size() == 1) {
                    timeSeriesCollection.add(create_item(xml_item));
                } else if (token_path.get(1).startsWith("[list]")) {
                    timeSeriesCollection.addAll(parse_list(xml_item.getElementsByTagName(token_path.get(1).substring(6)), token_path.subList(1, token_path.size())));
                }
                else {
                    timeSeriesCollection.addAll(parse_item((Element) xml_item.getElementsByTagName(token_path.get(1)).item(0), token_path.subList(1, token_path.size())));
                }
            }
            else {
                throw new Exception("Parsing error");
            }
        }

        return timeSeriesCollection;
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

    private TimeSeriesPoint create_item(Element XML) throws DatastoreException {
        JSONObject location = this.instructions.getJSONObject("location");

        String time = find_info_item(XML, Arrays.asList(location.getString("time").split(",")));
        String strvalue = find_info_item(XML, Arrays.asList(location.getString("value").split(",")));
        Double value = strvalue.isEmpty() ? null : Double.parseDouble(strvalue);
        return new TimeSeriesPoint(time, value);
    }
}
