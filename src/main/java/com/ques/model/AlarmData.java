package com.ques.model;

import lombok.Data;

@Data
public class AlarmData {
    String tagName;
    String rule;
    long dataTime;
    Double value;
}
