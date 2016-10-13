package com.taobao.datax.engine.schedule;

import com.taobao.datax.engine.conf.JobConf;
import com.taobao.datax.engine.conf.ParseXMLUtil;

/**
 * Created by zhuhq on 2015/11/18.
 */
public class Task implements Runnable {
    private Engine engine;
    private String jobConfPath;
    public Task(final  Engine engine,final  String jobConfPath){
        this.engine = engine;
        this.jobConfPath = jobConfPath;
    }

    @Override
    public void run() {
        try {
            JobConf jobConf = ParseXMLUtil.loadJobConfig(jobConfPath);
            engine.start(jobConf);
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
