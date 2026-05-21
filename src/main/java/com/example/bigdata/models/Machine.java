package com.example.bigdata.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Machine {

    @JsonProperty("machineId")
    public String machineId;

    @JsonProperty("name")
    public String name;

    @JsonProperty("type")
    public String type;

    @JsonProperty("nominalRpm")
    public double nominalRpm;

    @JsonProperty("nominalCurrentA")
    public double nominalCurrentA;

    @JsonProperty("vibrationWarnMms")
    public double vibrationWarnMms;

    @JsonProperty("vibrationAlarmMms")
    public double vibrationAlarmMms;

    @JsonProperty("tempWarnC")
    public double tempWarnC;

    @JsonProperty("tempAlarmC")
    public double tempAlarmC;

    @JsonProperty("productionLine")
    public String productionLine;

    @JsonProperty("plannedMaintenanceDays")
    public int plannedMaintenanceDays;

    @Override
    public String toString() {
        return "Machine{" +
                "machineId='" + machineId + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", productionLine='" + productionLine + '\'' +
                ", vibrationAlarmMms=" + vibrationAlarmMms +
                ", tempAlarmC=" + tempAlarmC +
                '}';
    }
}

