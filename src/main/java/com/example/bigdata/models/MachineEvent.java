package com.example.bigdata.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class MachineEvent {

    @JsonProperty("machineId")
    public String machineId;

    @JsonProperty("timestamp")
    public Instant timestamp;

    @JsonProperty("machineState")
    public String machineState;  // RUNNING, IDLE, SETUP, FAULT, MAINTENANCE

    @JsonProperty("shift")
    public String shift;  // A, B, C

    // Parametry wrzeciona
    @JsonProperty("spindleRpm")
    public double spindleRpm;

    @JsonProperty("spindleLoadPercent")
    public double spindleLoadPercent;

    // Parametry posuwu
    @JsonProperty("feedRateMmMin")
    public double feedRateMmMin;

    // Parametry elektryczne
    @JsonProperty("motorCurrentA")
    public double motorCurrentA;

    @JsonProperty("motorVoltageV")
    public double motorVoltageV;

    @JsonProperty("powerKw")
    public double powerKw;

    // Energia
    @JsonProperty("energyKwhDelta")
    public double energyKwhDelta;

    // Temperatury
    @JsonProperty("bearingTempC")
    public double bearingTempC;

    @JsonProperty("motorTempC")
    public double motorTempC;

    @JsonProperty("coolantTempC")
    public double coolantTempC;

    // Drgania
    @JsonProperty("vibrationXMms")
    public double vibrationXMms;

    @JsonProperty("vibrationYMms")
    public double vibrationYMms;

    @JsonProperty("vibrationZMms")
    public double vibrationZMms;

    @JsonProperty("vibrationRmsMms")
    public double vibrationRmsMms;  // RMS ze wszystkich osi

    // Produkcja
    @JsonProperty("partsProducedDelta")
    public int partsProducedDelta;

    @JsonProperty("defectPartsDelta")
    public int defectPartsDelta;

    // Czas cyklu
    @JsonProperty("cycleTimeS")
    public double cycleTimeS;

    @Override
    public String toString() {
        return "MachineEvent{" +
                "machineId='" + machineId + '\'' +
                ", timestamp=" + timestamp +
                ", machineState='" + machineState + '\'' +
                ", shift='" + shift + '\'' +
                ", vibrationRmsMms=" + vibrationRmsMms +
                ", bearingTempC=" + bearingTempC +
                ", motorCurrentA=" + motorCurrentA +
                ", partsProduced=" + partsProducedDelta +
                '}';
    }
}

