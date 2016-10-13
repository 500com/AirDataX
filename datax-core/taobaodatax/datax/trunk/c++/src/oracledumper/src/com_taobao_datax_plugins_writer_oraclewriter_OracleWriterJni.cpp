/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#include "com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni.h"
#include "oradumper.h"

#include <iconv.h>

JNIEXPORT jlong JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1init(JNIEnv *env, jobject obj, jstring logon1, jstring table1, jstring sep1, jstring pre1, jstring post1, jstring dtfmt1, jstring encoding1, jstring colorder1, jstring limit1, jlong parallel1, jlong skipindex1)
{
	Dumper* DumperObj = Create();

	const char *logon = env->GetStringUTFChars(logon1, 0);
	const char *table = env->GetStringUTFChars(table1, 0);
	const char *sep = env->GetStringUTFChars(sep1, 0);
	const char *pre = env->GetStringUTFChars(pre1, 0);
	const char *post = env->GetStringUTFChars(post1, 0);
	const char *dtfmt = env->GetStringUTFChars(dtfmt1, 0);
	const char *encoding = env->GetStringUTFChars(encoding1, 0);
	const char *colorder = env->GetStringUTFChars(colorder1, 0);
	const char *limit = env->GetStringUTFChars(limit1, 0);

    DumperObj->SetLogon(logon);
	DumperObj->SetTable(table);
   	DumperObj->SetSepChar(*sep);
	DumperObj->SetPreSQL(pre);
   	DumperObj->SetPostSQL(post) ;
	DumperObj->SetDateFormat(dtfmt);
	DumperObj->SetEncodeType(encoding);
	DumperObj->SetColOrder(colorder);

    double tLimit = atof(limit);
    if (tLimit < 0) {
        tLimit = 0;
    }
	DumperObj->SetLimit(tLimit);
	DumperObj->SetLoadMode("", "");
    DumperObj->SetNeedParallel(parallel1 > 1 ? true:false);
	DumperObj->SetSkipIndex(skipindex1);

	env->ReleaseStringUTFChars(logon1, logon);
	env->ReleaseStringUTFChars(table1, table);
	env->ReleaseStringUTFChars(sep1, sep);
	env->ReleaseStringUTFChars(pre1, pre);
	env->ReleaseStringUTFChars(post1, post);
	env->ReleaseStringUTFChars(dtfmt1, dtfmt);
	env->ReleaseStringUTFChars(encoding1, encoding);
	env->ReleaseStringUTFChars(colorder1, colorder);
	env->ReleaseStringUTFChars(limit1, limit);

	return (long)DumperObj;
	
}

JNIEXPORT jint JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1connect(JNIEnv *env, jobject obj, jlong p)
{
	((Dumper *)p)->DumperInit(); 	

	return 0;
}

JNIEXPORT jint JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1predump(JNIEnv *env, jobject obj, jlong p, jlong flag)
{
	((Dumper *)p)->PreDump((long)flag);         		// execute preSQL, truncate and so on	

	return 0;
}

JNIEXPORT jint JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1dump(JNIEnv *env, jobject obj, jlong p, jstring lines1)
{
	const char *lines = env->GetStringUTFChars(lines1, 0);
	
	((Dumper *)p)->RunDump(lines);
	
	env->ReleaseStringUTFChars(lines1, (const char *)lines);
	
	return 0;
}

JNIEXPORT jint JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1commit(JNIEnv *env, jobject obj, jlong p)
{
	bool isSuccess = ((Dumper *)p)->QCCheck();
	((Dumper *)p)->CommitDump(isSuccess);	

	return isSuccess ? 0 : -1;
}

JNIEXPORT jint JNICALL Java_com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni_oracle_1dumper_1finish(JNIEnv *env, jobject obj, jlong p, jlong flag)
{

	int res = ((Dumper *)p)->PostDump((long)flag);        	// execute postSQL, run statistics and so on

	return res;
	//delete (Dumper *)p;
}

