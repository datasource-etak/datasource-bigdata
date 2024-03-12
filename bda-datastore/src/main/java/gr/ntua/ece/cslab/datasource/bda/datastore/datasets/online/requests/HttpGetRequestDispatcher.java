package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.requests;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class HttpGetRequestDispatcher {
    private final static Logger LOGGER = Logger.getLogger(HttpGetRequestDispatcher.class.getCanonicalName());

    private String url;

    public HttpGetRequestDispatcher() {}

    public HttpGetRequestDispatcher(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Document dispatch() throws IOException, SAXException, ParserConfigurationException {
        URL obj = new URL(this.url);
        LOGGER.log(Level.INFO, "Attempting to fetch from url " + this.url);

        HttpURLConnection con = (HttpURLConnection) obj.openConnection();


        try {
            con.setRequestMethod("GET");
        } catch (ProtocolException e) {
            LOGGER.log(Level.WARNING, "Http GET Request method to " + this.url + " failed.");
            throw e;
        }

        int responseCode;
        try {
            responseCode = con.getResponseCode();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Response to " + this.url + " is not 200 OK.");
            throw e;
        }

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in;

            if ((con.getContentEncoding()!= null) && (con.getContentEncoding().equals("gzip"))) {
                in = new BufferedReader(new InputStreamReader(new GZIPInputStream(con.getInputStream())));
            }
            else {
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            }

            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            String responseText = response.toString();

            while (!responseText.startsWith("<"))
                responseText = responseText.substring(1);

            Document doc;
            try {
                doc = DocumentBuilderFactory.newInstance().
                        newDocumentBuilder().
                        parse(new InputSource(
                                new StringReader(responseText)
                        ));
            } catch (SAXException e) {
                LOGGER.log(Level.WARNING, "SAX exception in XML response from " + this.url);
                throw e;
            } catch (ParserConfigurationException e) {
                LOGGER.log(Level.WARNING, "Error in parsing XML response from " + this.url);
                throw e;
            }

            return doc;
        }

        return null;
    }
}
