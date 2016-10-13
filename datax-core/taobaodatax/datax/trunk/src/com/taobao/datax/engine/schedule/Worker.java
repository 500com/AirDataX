package com.taobao.datax.engine.schedule;

import com.taobao.datax.engine.conf.EngineConf;
import com.taobao.datax.engine.conf.ParseXMLUtil;
import com.taobao.datax.engine.conf.PluginConf;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zhuhq on 2015/11/18.
 */
public class Worker {
    private static ExecutorService executor = Executors.newFixedThreadPool(10);
    public static void main(String args[]) {
        String jobDescFile = args[0];
        EngineConf engineConf = ParseXMLUtil.loadEngineConfig();
        Map<String, PluginConf> pluginConfs = ParseXMLUtil.loadPluginConfig();

        Engine engine = new Engine(engineConf, pluginConfs);
        executor.execute(new Task(engine,jobDescFile));
        executor.shutdown();
    }
}
