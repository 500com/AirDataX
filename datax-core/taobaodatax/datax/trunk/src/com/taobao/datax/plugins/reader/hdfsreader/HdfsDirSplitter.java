/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package com.taobao.datax.plugins.reader.hdfsreader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Splitter;
import com.taobao.datax.common.util.SplitUtils;
import com.taobao.datax.plugins.common.DFSUtils;


public class HdfsDirSplitter extends Splitter {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(HdfsDirSplitter.class);
	private Path p = null;

	private FileSystem fs = null;

	@Override
	public int init() {
		String dir = param.getValue(ParamKey.dir);
        if(StringUtils.isEmpty(dir) || "hive".equalsIgnoreCase(dir)) {//根据hive表名称获取dir
            dir = HdfsReader.getDirFromHiveConf(param);

        }
		if (dir.endsWith("*")) {
			dir = dir.substring(0, dir.lastIndexOf("*"));
		}
        logger.info("splitter init dir:" + dir);
		String ugi = param.getValue(ParamKey.ugi, null);
		if (dir == null) {
			throw new DataExchangeException("Can't find the param ["
					+ ParamKey.dir + "] in hdfs-spliter-param.");
		}
		p = new Path(dir);
		
		String configure = param.getValue(ParamKey.hadoop_conf, "");
		try {
			fs = DFSUtils.createFileSystem(new URI(dir),
					DFSUtils.getConf(dir, ugi, configure));
			if (!fs.exists(p)) {
				IOUtils.closeStream(fs);
				throw new DataExchangeException("the path[" + dir
						+ "] does not exist.");
			}
		} catch (Exception e) {
			throw new DataExchangeException("Can't create the HDFS file system:"
					+ e);
		}
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public List<PluginParam> split() {

		List<PluginParam> v = new ArrayList<PluginParam>();
		try {
			/*FileStatus[] status = fs.listStatus(p);
			for (FileStatus state : status) {
				if (state.isFile()) {
					String file = state.getPath().toString();
					PluginParam oParams = SplitUtils.copyParam(param);
					oParams.putValue(ParamKey.dir, file);
					v.add(oParams);
				}

			}*/
            split(v,param,fs,p);
		} catch (Exception e) {
			throw new DataExchangeException(
					"some errors have happened in fetching the file-status:"
							+ e.getCause());
		} finally {
			IOUtils.closeStream(fs);
		}
		return v;
	}
    private void split(List<PluginParam> params,PluginParam sourceParam,FileSystem fs,Path p) throws  Exception{
        FileStatus[] status = fs.listStatus(p);
        for(FileStatus fileStatus : status) {
            if(!fileStatus.isDir()) {
                String file = fileStatus.getPath().toString();
                PluginParam pluginParam = SplitUtils.copyParam(sourceParam);

                int indexOfEqual = file.indexOf("=");
                if(indexOfEqual > 0) {
                    String dir = file.substring(0,file.lastIndexOf("/"));
                    String pre = file.substring(0,indexOfEqual);
                    int lastIndexOfPre = pre.lastIndexOf("/");
                    String partitionDir = dir.substring(lastIndexOfPre + 1);
                    String[] partitionKV = StringUtils.split(partitionDir,"/");
                    int partitionSize = partitionKV.length;
                    String[] partitionNames = new String[partitionSize];
                    String[] partitionValues = new String[partitionSize];
                    logger.info("path:" + file);
                    logger.info("partition:" + StringUtils.join(partitionKV,","));
                    for(int i = 0; i < partitionSize; i++) {
                        String[] nameValue = StringUtils.split(partitionKV[i],"=");
                        if(nameValue.length != 2) {
                            logger.error("partition must be name=value");
                            throw  new DataExchangeException("partition must be name=value");
                        }
                        partitionNames[i] = nameValue[0];
                        partitionValues[i] = nameValue[1];
                    }
                    if(partitionSize > 0) {
                        pluginParam.putValue(ParamKey.hivePartitionNames,StringUtils.join(partitionNames,","));
                        pluginParam.putValue(ParamKey.hivePartitionValues,StringUtils.join(partitionValues,","));
                    }
                }
                pluginParam.putValue(ParamKey.dir, file);
                params.add(pluginParam);
            }else {
                split(params,sourceParam,fs,p);
            }
        }
    }



}
