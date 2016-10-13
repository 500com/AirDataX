/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package com.taobao.datax.engine.schedule;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.taobao.datax.common.constants.ExitStatus;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.engine.conf.PluginConf;

/**
 * Represents executor of a {@link Writer}.</br>
 * 
 * <p>{@link Engine} use {@link WriterWorker} to dump data.</p>
 * 
 * @see ReaderWorker
 * 
 * */
public class WriterWorker extends PluginWorker implements Callable<PluginWorkerResult> {
	private LineReceiver receiver;

	private Method init;

	private Method connect;

	private Method startWrite;

	private Method commit;

	private Method finish;

	private static int globalIndex = 0;

	private static final Logger logger = Logger.getLogger(WriterWorker.class);

	/**
	 * Construct a {@link WriterWorker}.
	 * 
	 * @param	pluginConf
	 * 			PluginConf of {@link Writer}.
	 * 
	 * @param myClass
	 * 
	 * @throws DataExchangeException
	 * 
	 */
	public WriterWorker(PluginConf pluginConf, Class<?> myClass)  {
		super(pluginConf, myClass);
		try {
			init = myClass.getMethod("init", new Class[] {});
			connect = myClass.getMethod("connect", new Class[] {});
			startWrite = myClass
					.getMethod("startWrite", new Class[] { Class
							.forName("com.taobao.datax.common.plugin.LineReceiver") });
			commit = myClass.getMethod("commit", new Class[] {});
			finish = myClass.getMethod("finish", new Class[] {});
		} catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e);
		}
		this.setMyIndex(globalIndex++);
	}

	public void setLineReceiver(LineReceiver receiver) {
		this.receiver = receiver;
	}

	/**
	 * Write data, main execute logic code of {@link Writer} <br>
	 * NOTE: When catches exception, {@link WriterWorker} exit process immediately
	 * 
	 * */
	@Override
	public PluginWorkerResult call() {
        PluginWorkerResult result = new PluginWorkerResult();
        int iRetcode = 1;
		try {
			iRetcode = (Integer) init.invoke(myObject, new Object[] {});
			if (iRetcode != PluginStatus.SUCCESS.value()) {
                result.errorMsg("DataX Initialize failed.");
				logger.error(result.msg);
				//System.exit(ExitStatus.FAILED.value());
				return result;
			}
			iRetcode = (Integer) connect.invoke(myObject, new Object[] {});
			if (iRetcode != PluginStatus.SUCCESS.value()) {
                result.errorMsg("DataX connect to DataSink failed.");
				logger.error(result.msg);
				//System.exit(ExitStatus.FAILED.value());
				return result;
			}
			iRetcode = (Integer) startWrite.invoke(myObject,
					new Object[] { receiver });
			if (iRetcode != PluginStatus.SUCCESS.value()) {
                result.errorMsg("DataX starts writing data failed .");
				logger.error(result.msg);
				//System.exit(ExitStatus.FAILED.value());
				return result;
			}
			iRetcode = (Integer) commit.invoke(myObject, new Object[] {});
			if (iRetcode != PluginStatus.SUCCESS.value()) {
                result.errorMsg("DataX commits transaction failed.");
				logger.error(result.msg);
				//System.exit(ExitStatus.FAILED.value());
				return result;
			}

		} catch (Exception e) {
            iRetcode = PluginStatus.FAILURE.value();
            result.errorMsg(ExceptionTracker.trace(e));
            logger.error("error:",e);
		}finally {
            try {
                logger.info("begin execute finish in finally code:" + iRetcode);

                if(iRetcode == PluginStatus.FAILURE.value()) {
                    try {
                        finish.invoke(myObject, new Object[] {});
                    }catch (Exception ignore) {
                        logger.info("execute finish ignore error:",ignore);
                    }
                }else {
                    iRetcode = (Integer) finish.invoke(myObject, new Object[] {});
                    logger.info("execute finish in finally code:" + iRetcode);
                    if (iRetcode != 0) {
                        result.errorMsg("DataX do finish job failed");
                        logger.error(result.msg);
                        //System.exit(ExitStatus.FAILED.value());
                        //return;
                    }
                }

            } catch (Exception ex) {
                result.errorMsg("DataX do finish job failed" + ex.getMessage());
                logger.error("DataX do finish job failed", ex);
            }
            logger.info("call result msg:" + result.msg + " is success:" + result.success );
        }
        return  result;
	}

}
