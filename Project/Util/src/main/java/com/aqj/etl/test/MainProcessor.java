package com.aqj.etl.test;

import etl.binlog.LogExecutor;

public class MainProcessor {


    public static void main(String[] args) {
        String[] topics = {"",""};
        new LogExecutor().doTask(topics);
    }

}
