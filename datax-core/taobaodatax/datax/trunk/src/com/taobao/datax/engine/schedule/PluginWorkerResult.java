package com.taobao.datax.engine.schedule;

/**
 * Created by zhuhq on 2015/12/7.
 */
public class PluginWorkerResult {
    public boolean success = true;
    public String msg;
    public PluginWorkerResult() {}
    public PluginWorkerResult(boolean isSuccess,String msg){
        this.success = isSuccess;
        this.msg = msg;
    }
    public void errorMsg(String msg) {
        success = false;
        this.msg = msg;
    }
}
