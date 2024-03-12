package gr.ntua.ece.cslab.datasource.bda.common.storage.beans;

import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class OperatorParameterTest {

    OperatorParameter parameter1;

    OperatorParameter parameter2;

    OperatorParameter parameter3;
    @BeforeEach
    public void setUp() {
        parameter1 = new OperatorParameter(
                1,
                "parameter1",
                "string",
                1,
                "{\"type\": \"from_list\", \"evaluate\": {\"choice_list\": [\"a\", \"b\", \"c\", \"d\"]}}"
        );

        parameter2 = new OperatorParameter(
                2,
                "parameter2",
                "integer",
                1,
                "{\"type\": \"range\", \"evaluate\": {\"maximum\": 10, \"minimum\": 1}}"
        );

        parameter3 = new OperatorParameter(
                2,
                "parameter2",
                "double",
                1,
                "{\"type\": \"range\", \"evaluate\": {\"maximum\": 0.8, \"minimum\": 0.3}}"
        );

    }

    @Test
    public void testRestictionsToJsonObject() {
        Map<String, Object> result = parameter1.getRestrictions();

        Assertions.assertTrue(result.containsKey("type"));
        Assertions.assertTrue(result.containsKey("evaluate"));
        Assertions.assertEquals("from_list", result.get("type"));
        List<String> eval_list = ((HashMap<String, List<String>>) result.get("evaluate")).get("choice_list");
        Assertions.assertTrue(eval_list.containsAll(Arrays.asList(new String[] {"a", "b", "c", "d"})));

        result = parameter2.getRestrictions();

        Assertions.assertTrue(result.containsKey("type"));
        Assertions.assertTrue(result.containsKey("evaluate"));
        Assertions.assertEquals("range", result.get("type"));
        Map<String, Integer> eval_json = ((HashMap<String, Integer>) result.get("evaluate"));
        Assertions.assertEquals(1, eval_json.get("minimum"));
        Assertions.assertEquals(10, eval_json.get("maximum"));

        result = parameter3.getRestrictions();

        Assertions.assertTrue(result.containsKey("type"));
        Assertions.assertTrue(result.containsKey("evaluate"));
        Assertions.assertEquals("range", result.get("type"));
        Map<String, Double> eval_json2 = ((HashMap<String, Double>) result.get("evaluate"));
        Assertions.assertEquals(0.3, eval_json2.get("minimum"));
        Assertions.assertEquals(0.8, eval_json2.get("maximum"));


    }

    @AfterEach
    public void close() {
    }

}