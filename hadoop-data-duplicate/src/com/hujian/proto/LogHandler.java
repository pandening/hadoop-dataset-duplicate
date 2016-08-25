package com.hujian.proto;

/**
 * Created by hujian on 16-8-24.
 */
public class LogHandler {
    private boolean isSetUp=false;
    //info
    public void info(String tag,String msg){
        if(this.isSetUp) {
            System.out.println("[Info_" + tag + "]" + msg);
        }
    }
    //debug
    public void debug(String tag,String msg){
        if(this.isSetUp) {
            System.out.println("[Debug_" + tag + "]" + msg);
        }
    }
    //error
    public void error(String tag,String msg){
        if(this.isSetUp) {
            System.out.println("[Error_" + tag + "]" + msg);
        }
    }
    //you can setup/shut down the logger
    public void setLogger(boolean is){
     this.isSetUp=is;
    }
}
