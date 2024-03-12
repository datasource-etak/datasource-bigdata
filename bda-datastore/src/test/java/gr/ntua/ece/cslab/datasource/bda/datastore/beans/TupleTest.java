package gr.ntua.ece.cslab.datasource.bda.datastore.beans;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TupleTest extends TestCase {

    public void testToMap() {
        List<Tuple> tuples = new ArrayList<>();
        List<KeyValue> tuple1 = new ArrayList<>();
        List<KeyValue> tuple2 = new ArrayList<>();

        for (int i = 0 ; i < 10; i++) {
            String key = String.valueOf(i);

            tuple1.add(new KeyValue(key, "1  " + key));
            tuple2.add(new KeyValue(key, "2  " + key));
        }

        tuples.add(new Tuple(tuple1));
        tuples.add(new Tuple(tuple2));


        System.out.println(tuples);

        //System.out.println(tuples.stream().map(Tuple::toMap).collect(Collectors.toList()));
    }
}