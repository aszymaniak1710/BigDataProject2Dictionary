package com.example.bigdata.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Machine machine = (Machine) o;
        return Double.compare(machine.nominalRpm, nominalRpm) == 0 &&
                Double.compare(machine.nominalCurrentA, nominalCurrentA) == 0 &&
                Double.compare(machine.vibrationWarnMms, vibrationWarnMms) == 0 &&
                Double.compare(machine.vibrationAlarmMms, vibrationAlarmMms) == 0 &&
                Double.compare(machine.tempWarnC, tempWarnC) == 0 &&
                Double.compare(machine.tempAlarmC, tempAlarmC) == 0 &&
                machine.plannedMaintenanceDays == plannedMaintenanceDays &&
                Objects.equals(machineId, machine.machineId) &&
                Objects.equals(name, machine.name) &&
                Objects.equals(type, machine.type) &&
                Objects.equals(productionLine, machine.productionLine);
    }

    @Override
    public int hashCode() {
        return Objects.hash(machineId, name, type, nominalRpm, nominalCurrentA,
                vibrationWarnMms, vibrationAlarmMms, tempWarnC, tempAlarmC,
                productionLine, plannedMaintenanceDays);
    }
}

