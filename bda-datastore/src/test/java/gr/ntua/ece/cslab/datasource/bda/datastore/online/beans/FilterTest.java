package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.datasource.bda.datastore.enums.DatasetStatus;
import org.apache.commons.codec.binary.Base64;

import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.Filter;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.FilterId;
import gr.ntua.ece.cslab.datasource.bda.datastore.datasets.online.beans.FilterValue;
import junit.framework.TestCase;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

public class FilterTest extends TestCase {

    public void testPrepareFiltersForDownload() {

        List<FilterValue> c1 = new ArrayList<>();
        c1.add(new FilterValue("c11", "c11 value"));
        c1.add(new FilterValue("c12", "c12 value"));
        Filter f1 = new Filter(new FilterId("f1", 0), "Filter1", c1);

        List<FilterValue> c2 = new ArrayList<>();
        c2.add(new FilterValue("c21", "c21 value"));
        c2.add(new FilterValue("c22", "c22 value"));
        Filter f2 = new Filter(new FilterId("f2", 1), "Filter2", c2);

        List<Filter> fs = new ArrayList<Filter>();
        fs.add(f1);
        fs.add(f2);

        List<List<Filter>> lists = Filter.prepareFiltersForDownload(fs);

        for (List<Filter> l : lists) System.out.println(l.toString());
    }

    public void testToMap() {
        String jsonString = "{\n" +
                "    \"Filter1\": [\n" +
                "        \"c11 value\",\n" +
                "        \"c12 value\"\n" +
                "    ],\n" +
                "    \"Filter2\": [\n" +
                "        \"c21 value\",\n" +
                "        \"c22 value\"\n" +
                "    ],\n" +
                "    \"Filter3\": [\n" +
                "        \"c32 value\",\n" +
                "        \"c34 value\"\n" +
                "    ]\n" +
                "}";
        List<FilterValue> c1 = new ArrayList<>();
        c1.add(new FilterValue("c11", "c11 value"));
        c1.add(new FilterValue("c12", "c12 value"));
        c1.add(new FilterValue("c13", "c13 value"));
        c1.add(new FilterValue("c14", "c14 value"));
        c1.add(new FilterValue("c15", "c15 value"));
        c1.add(new FilterValue("c16", "c16 value"));
        c1.add(new FilterValue("c17", "c17 value"));
        c1.add(new FilterValue("c18", "c18 value"));
        Filter f1 = new Filter(new FilterId("f1", 0), "Filter1", c1);

        List<FilterValue> c2 = new ArrayList<>();
        c2.add(new FilterValue("c21", "c21 value"));
        c2.add(new FilterValue("c22", "c22 value"));
        c2.add(new FilterValue("c23", "c23 value"));
        c2.add(new FilterValue("c24", "c24 value"));
        c2.add(new FilterValue("c25", "c25 value"));
        c2.add(new FilterValue("c26", "c26 value"));


        Filter f2 = new Filter(new FilterId("f2", 1), "Filter2", c2);


        List<FilterValue> c3 = new ArrayList<>();
        c3.add(new FilterValue("c31", "c31 value"));
        c3.add(new FilterValue("c32", "c32 value"));
        c3.add(new FilterValue("c33", "c33 value"));
        c3.add(new FilterValue("c34", "c34 value"));
        c3.add(new FilterValue("c35", "c35 value"));
        c3.add(new FilterValue("c36", "c36 value"));


        Filter f3 = new Filter(new FilterId("f3", 2), "Filter3", c3);

        List<Filter> fs = new ArrayList<Filter>();
        fs.add(f1);
        fs.add(f2);
        fs.add(f3);

        Map<String, List<String>> res = Filter.toMap(fs);

        JSONObject json = new JSONObject(res);
        System.out.println(json.toString(4));

        Gson gson = new Gson();
        TypeToken<Map<String, List<String>>> token = new TypeToken<Map<String, List<String>>>() {};

        Map<String, List<String>> resultMap = gson.fromJson(jsonString, token.getType());

        System.out.println(resultMap);

        System.out.println(Filter.selectFilterValues(fs, resultMap));

    }


    public void testEncodeDecode() {
        List<FilterValue> c1 = new ArrayList<>();
        c1.add(new FilterValue("c14", "c14 value"));
        c1.add(new FilterValue("c15", "c15 value"));
        c1.add(new FilterValue("c16", "c16 value"));
        c1.add(new FilterValue("c17", "c17 value"));
        c1.add(new FilterValue("c18", "c18 value"));
        c1.add(new FilterValue("c11", "c11 value"));
        c1.add(new FilterValue("c12", "c12 value"));
        c1.add(new FilterValue("c13", "c13 value"));

        Filter f1 = new Filter(new FilterId("f1", 0), "Filter1", c1);

        List<FilterValue> c2 = new ArrayList<>();
        c2.add(new FilterValue("c24", "c24 value"));
        c2.add(new FilterValue("c25", "c25 value"));
        c2.add(new FilterValue("c26", "c26 value"));
        c2.add(new FilterValue("c21", "c21 value"));
        c2.add(new FilterValue("c22", "c22 value"));
        c2.add(new FilterValue("c23", "c23 value"));


        Filter f2 = new Filter(new FilterId("f2", 1), "Filter2", c2);


        List<FilterValue> c3 = new ArrayList<>();
        c3.add(new FilterValue("c31", "c31 value"));
        c3.add(new FilterValue("c32", "c32 value"));
        c3.add(new FilterValue("c33", "c33 value"));
        c3.add(new FilterValue("c34", "c34 value"));
        c3.add(new FilterValue("c35", "c35 value"));
        c3.add(new FilterValue("c36", "c36 value"));


        Filter f3 = new Filter(new FilterId("f3", 2), "Filter3", c3);

        List<Filter> fs = new ArrayList<Filter>();
        fs.add(f3);
        fs.add(f2);
        fs.add(f1);


        System.out.println(fs);

        fs = Filter.sorted(fs);
        System.out.println();

        System.out.println(new JSONObject(Filter.toMap(fs)).toString(3));



        String encodedString = Base64.encodeBase64String((new JSONArray(fs)).toString().getBytes());

        System.out.println(encodedString);
        String decodedString = new String(Base64.decodeBase64(encodedString));
        JSONArray decodedObj = new JSONArray(decodedString);

        List<Filter> lf = decodedObj.toList().stream().map(x-> (HashMap<String, Object>) x).map(Filter::new).collect(Collectors.toList());
        for (Filter as : lf)
            System.out.println(as.toString());


    }

    public void testFilterEquality() {

        List<FilterValue> c1 = new ArrayList<>();
        c1.add(new FilterValue("c11", "c11 value"));
        c1.add(new FilterValue("c12", "c12 value"));
        c1.add(new FilterValue("c13", "c13 value"));
        c1.add(new FilterValue("c14", "c14 value"));
        c1.add(new FilterValue("c15", "c15 value"));
        c1.add(new FilterValue("c16", "c16 value"));
        c1.add(new FilterValue("c17", "c17 value"));
        c1.add(new FilterValue("c18", "c18 value"));
        Filter f1 = new Filter(new FilterId("f1", 0), "Filter1", c1);

        List<FilterValue> c2 = new ArrayList<>();
        c2.add(new FilterValue("c21", "c21 value"));
        c2.add(new FilterValue("c22", "c22 value"));
        c2.add(new FilterValue("c23", "c23 value"));
        c2.add(new FilterValue("c24", "c24 value"));
        c2.add(new FilterValue("c25", "c25 value"));
        c2.add(new FilterValue("c26", "c26 value"));


        Filter f2 = new Filter(new FilterId("f2", 1), "Filter2", c2);


        List<FilterValue> c3 = new ArrayList<>();
        c3.add(new FilterValue("c31", "c31 value"));
        c3.add(new FilterValue("c32", "c32 value"));
        c3.add(new FilterValue("c33", "c33 value"));
        c3.add(new FilterValue("c34", "c34 value"));
        c3.add(new FilterValue("c35", "c35 value"));
        c3.add(new FilterValue("c36", "c36 value"));


        Filter f3 = new Filter(new FilterId("f3", 2), "Filter3", c3);

        List<Filter> fs = new ArrayList<Filter>();
        fs.add(f2);
        fs.add(f1);
        fs.add(f3);

        String jsonStringfilter1 = "{\n" +
                "    \"Filter1\": [\n" +
                "        \"c11 value\",\n" +
                "        \"c12 value\"\n" +
                "    ],\n" +
                "    \"Filter2\": [\n" +
                "        \"c21 value\",\n" +
                "        \"c22 value\"\n" +
                "    ],\n" +
                "    \"Filter3\": [\n" +
                "        \"c32 value\",\n" +
                "        \"c34 value\"\n" +
                "    ]\n" +
                "}";

        String jsonStringfilter2 = "{\n" +
                "    \"Filter2\": [\n" +
                "        \"c22 value\",\n" +
                "        \"c21 value\"\n" +
                "    ],\n" +
                "    \"Filter3\": [\n" +
                "        \"c34 value\",\n" +
                "        \"c32 value\"\n" +
                "    ],\n" +
                "    \"Filter1\": [\n" +
                "        \"c12 value\",\n" +
                "        \"c11 value\"\n" +
                "    ]\n" +
                "}";

        Gson gson = new Gson();
        TypeToken<Map<String, List<String>>> token = new TypeToken<Map<String, List<String>>>() {};

        Map<String, List<String>> selval1 = gson.fromJson(jsonStringfilter1, token.getType());
        Map<String, List<String>> selval2 = gson.fromJson(jsonStringfilter2, token.getType());

        List<Filter> sfs1 = Filter.selectFilterValues(fs, selval1);
        List<Filter> sfs2 = Filter.selectFilterValues(fs, selval2);

        String bs1 = Filter.filterListBase64encode(sfs1);

        String bs2 = Filter.filterListBase64encode(sfs2);

        System.out.println(bs1);

        System.out.println(bs2);

        assertEquals(bs1, bs2);

        System.out.println((Filter.mapFilterRepresentationFromBase64String(bs1)));
        System.out.println((Filter.mapFilterRepresentationFromBase64String(bs2)));

        System.out.println(Filter.listFilterFromBase64String(bs1));
        System.out.println(Filter.listFilterFromBase64String(bs2));

        List<KeyValue> data1 = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", "uuid1"),
                new KeyValue("alias", "alias1"),
                new KeyValue("dataset_id", "did1"),
                new KeyValue("dataset_name", "dn1"),
                new KeyValue("dataset_description", "ddsc1"),
                new KeyValue("source_name", "dsc1"),
                new KeyValue("associated_filter", bs1),
                new KeyValue("status", DatasetStatus.DOWNLOADING.toString())
        });

        List<KeyValue> data2 = Arrays.asList(new KeyValue[]{
                new KeyValue("uuid", "uuid2"),
                new KeyValue("alias", "alias2"),
                new KeyValue("dataset_id", "did2"),
                new KeyValue("dataset_name", "dn2"),
                new KeyValue("dataset_description", "ddsc2"),
                new KeyValue("source_name", "dsc2"),
                new KeyValue("associated_filter", bs2),
                new KeyValue("status", DatasetStatus.DOWNLOADING.toString())
        });
        List<Tuple> tuples = Arrays.asList(new Tuple[]{new Tuple(data1), new Tuple(data2)});

        List<HashMap<String, Object>> result = tuples.
                stream().
                map(Tuple::toMap).
                map(y -> y.entrySet().stream().collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        ((Map.Entry<String, Object>) entry).getKey().equals("associated_filter") ?
                                                Filter.mapFilterRepresentationFromBase64String((String) ((Map.Entry<String, Object>) entry).getValue()) :
                                                ((Map.Entry<?, ?>) entry).getValue(),
                                (a, b) -> b,
                                () -> new HashMap<String, Object>()
                        )
                )).
                collect(Collectors.toList());

        System.out.println(result);
    }





}