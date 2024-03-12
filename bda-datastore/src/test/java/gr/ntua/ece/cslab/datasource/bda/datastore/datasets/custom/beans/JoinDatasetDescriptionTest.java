package gr.ntua.ece.cslab.datasource.bda.datastore.datasets.custom.beans;

import junit.framework.TestCase;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinDatasetDescriptionTest extends TestCase {

    public Map<String, String> newColumnSpecs(String new_name, String new_type) {
        return new HashMap<String, String>() {{
           put("new_name", new_name);
           put("cast_type", new_type);
        }};
    }

    public Map<String, Map<String, String>> generateNumColumns(int n) {
        Map<String, Map<String, String>> columns = new HashMap<>();

        for (int i = 0 ; i < n ; i++) {
            columns.put("col" + i, newColumnSpecs("newcol" + i, "string"));
        }

        return columns;
    }
    public void testCountInColumns() throws Exception {

        int n = 10;

        Map<String, Map<String, String>> columns = generateNumColumns(n);
        JoinDatasetDescription ds = new JoinDatasetDescription("", "id", "alias", columns);

        for (int i = 0; i < n ; i++)
            assertEquals(ds.timeContainingColumnRenaming("newcol" + i), 1);

        assertEquals(ds.timeContainingColumnRenaming("newcol"), 0);

        columns.put("col_error", newColumnSpecs("newcol1", "sometype"));

        assertEquals(ds.timeContainingColumnRenaming("newcol1"), 2);


        System.out.println(ds.parseJSONCastingString());
        System.out.println(ds.parseJSONRenamingString());


        List<String> keys = new ArrayList<String>() {{
            add("add");
                    add("deferfe");
        }};

        System.out.println(keys.toString());

        System.out.println(new JSONArray(keys).toString());


    }
}