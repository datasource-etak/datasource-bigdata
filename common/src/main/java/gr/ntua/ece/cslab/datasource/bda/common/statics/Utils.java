package gr.ntua.ece.cslab.datasource.bda.common.statics;

import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Utils {

    public static String getCurrentLocalTimestamp() {
        ZoneId athensTimeZone = ZoneId.of("Europe/Athens");

        // Get the current timestamp in the specified time zone
        LocalDateTime currentDateTime = LocalDateTime.now(athensTimeZone);

        // Define a formatter for the desired output format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Format the current timestamp as a string
        String currentTimestampAsString = currentDateTime.format(formatter);

        // Print the result
        return currentTimestampAsString;

    }


    public static List<Map<String, Object>> parseJSONArrayString(String aJSONArray) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        // Parse the JSON array string
        JSONArray jsonArray = new JSONArray(aJSONArray);

        // Iterate through the JSON array elements
        for (int i = 0; i < jsonArray.length(); i++) {
            // Get each JSON object from the array
            JSONObject jsonObject = jsonArray.getJSONObject(i);

            // Convert the JSON object to a Map
            Map<String, Object> map = jsonObject.toMap();

            // Add the map to the result list
            resultList.add(map);
        }

        return resultList;
    }

}
