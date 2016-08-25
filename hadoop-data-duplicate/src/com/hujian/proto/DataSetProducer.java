package com.hujian.proto;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FilterWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by hujian on 16-8-24.
 * this is the data set producer,you can use the api of this class to get the
 * data set,but this class is not the running-part of this proto.
 */
public class DataSetProducer {
    //the tag of this class,just get the name of this class
    private final String TAG="DataSetProducer";
    //the logger
    private LogHandler logger=null;
    //the random-er
    private Random random=null;
    //th file name..
    private final String filename_base="data/data_";
    //the data set num,default produce 10 file
    private final int file_num=100;
    //each data file's size,the default is 10000
    private final int file_size=10000;
    //the max data
    private final int BiggestData=1000;

    public DataSetProducer(){
        logger=new LogHandler();
        //set up the logger
        logger.setLogger(true);
        //get the random
        random=new Random();
        //test the logger
        logger.info(this.TAG,"set up the logger done!");
    }

    //get the data
    public void producer(){
        //1、get the file name
        //2、create the file,get the file handler
        //3、output the data
        String filename=filename_base;
        BufferedWriter fileWriter=null;
        int data=0;
        StringBuilder out=new StringBuilder();
        for(int i=0;i<this.file_num;i++){
            filename=filename+i;
            filename=filename+".hujian";//just for *.hujian
            logger.info(this.TAG,"get the filename=>"+filename);
            try {
                fileWriter=new BufferedWriter(new FileWriter(filename));
                logger.info(this.TAG,"get the file handler...");
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(this.TAG,"can not open/create the file=>"+filename);
            }
            //producer data and output
            for(int j=0;j<this.file_size;j++){
                data=random.nextInt(this.BiggestData);
                out.append(data);
                try {
                    fileWriter.write(out.toString());
                    out=new StringBuilder();
                    fileWriter.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(this.TAG,"can not write to file the data...");
                }
            }
            //time to close the file handler
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(this.TAG,"can not close the file handler of file=>"+filename);
            }
            filename=filename_base;
        }
    }
    //run this class,you can get the date set
    //if you want to get the data file,just re-name the function to main
    //then choose this class ~
    public static void mainApp(String[] args){
        DataSetProducer producer=new DataSetProducer();
        producer.producer();
    }
}
