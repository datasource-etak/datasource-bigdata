package gr.ntua.ece.cslab.datasource.bda.datastore.online.beans;

import java.io.Serializable;

public class TimeSeriesPoint implements Serializable {

    private String time;
    private Double value;

    public TimeSeriesPoint() {
    }

    public TimeSeriesPoint(String time, Double value) {
        this.time = time;
        this.value = value;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TimeSeriesPoint{" + "time='" + time + '\'' + ", value=" + value + '}';
    }

}
