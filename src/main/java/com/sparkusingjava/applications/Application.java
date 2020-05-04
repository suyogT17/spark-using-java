package com.sparkusingjava.applications;

import com.sparkusingjava.helper.ArrayToDataset;
import com.sparkusingjava.helper.CsvToDatasetHouseToDataframe;
import com.sparkusingjava.helper.WordCount;

public class Application {

    /*
    * Dataset vs DataFrame
    * DataFrames provides Tungsten Memory Optimization
    * Dataset provides Type Safety
    * */

    public static void main(String[] args) {
        /*
        ArrayToDataset arrayToDataset = new ArrayToDataset();
        arrayToDataset.start();
        */
        /*
        CsvToDatasetHouseToDataframe csvToDatasetHouseToDataframe = new CsvToDatasetHouseToDataframe();
        csvToDatasetHouseToDataframe.start();
        */

        WordCount wordCount = new WordCount();
        wordCount.start();
    }

}
