/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package com.taobao.datax.common.constants;

/**
 * DataX constant configuration.
 * 
 * <p>
 * DATAX_LOCATION : path of DataX<br>
 * ENGINEXML : path of engin's xml.<br>
 * JOBSXML : path of jobs' xml.<br>
 * JOBSXMLDIR : path of concrete job xml.<br>
 * PLUGINSXML : path of plugins' xml.<br>
 * GENJOBXML : path of name's xml.
 * </p>
 * 
 * */
public abstract class Constants {
	public static  String DATAX_LOCATION = System.getProperty("user.dir");
    public static  String ENGINEXML = DATAX_LOCATION + "/conf/engine.xml";
	public static  String JOBSXML = DATAX_LOCATION + "/conf/jobs.xml";
	public static  String JOBSXMLDIR = DATAX_LOCATION + "/jobs/{0}.xml";
    public static  String PLUGINSXML = DATAX_LOCATION + "/conf/plugins.xml";
    public static  String PARAMCONFIG = DATAX_LOCATION
			+ "/conf/ParamsKey.java";
}