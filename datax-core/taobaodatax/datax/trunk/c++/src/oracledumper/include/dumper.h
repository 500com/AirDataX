/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#ifndef __DUMPER_H__
#define __DUMPER_H__

#include <dlfcn.h>
#include <libgen.h>

#include "common.h"

using namespace std;

class Dumper
{
public:
    typedef enum { APPEND, TRUNCATE, TRUNCPART } emLoadMode;

    Dumper();
    virtual ~Dumper();
    
    //Set operation
    void SetLogon( const string strLogon )
    { m_strLogon = strLogon; }

    void SetTable( const string strTable )
    { m_strTable = strTable; }

    void SetPreSQL( const string strPreSQL )
    { m_strPreSQL = strPreSQL; }
    
	void SetPostSQL( const string strPostSQL )
    { m_strPostSQL = strPostSQL; }

	void SetSepChar( const char cSepChar )
    { m_chSep = cSepChar; }
    
	void SetLoadMode( const string strMode, const string strPart = "" )
    { m_strMode = strMode;    m_strPart = strPart;  }

	void SetDateFormat( const string strDateFormat )
        { 
			m_strDateFormat = strDateFormat;
	   	  	m_strTimeStamp = strDateFormat + ".ff";
		}
    
    void SetNeedParallel(bool needParallel) {
        m_bNeedParallel = needParallel;
        return;
    }

	void SetEncodeType( const string strEncodeType )
        { m_strEncodeType = strEncodeType;
	   	  if (strcmp(m_strEncodeType.c_str(), "UTF-8") == 0)	
			  m_bIsUTF8 = true;
		  else
			  m_bIsUTF8 = false;
		}

	void SetColOrder( const string strColOrder )
    	{ m_strColOrder = strColOrder; }
	
    // Quality control
    void SetLimit( const float fLimit )
    { m_fLimit = fLimit; }

	void SetSkipIndex(const long flags)
	{ m_nSkipIndex = flags; }

    virtual void DumperInit() = 0;
    virtual void PreDump(long flag)  = 0;    // execute pre sql
    virtual void RunDump(const char *line)  = 0;    // load data into db
	virtual int simple_load(char *line, size_t size) = 0;
    virtual void CommitDump(bool bCommit)  = 0; // if commit the loaded data
    virtual int PostDump(long flag) = 0;    // execute post sql

    void GetLoadResult( int *pResult );
    bool  QCCheck();    // get Quality Control result

protected:
    string m_strTable;
          
    string m_strLogon;
    string m_strDBName;
    string m_strUsername;
    string m_strPassword;
    
    // С��1ʱ��Ϊ�ٷֱ�,����1ʱ��Ϊ��������������
    float m_fLimit;

    string m_strMode;
    string m_strPart;

    string m_strOrder;
    string m_strPreSQL;
    string m_strPostSQL;
	string m_strColOrder;
    string m_strDateFormat;
	string m_strTimeStamp;
    string m_strEncodeType;
	bool m_bIsUTF8;
	long m_nSkipIndex;

	vector<string> m_vDateFmt;
	vector<string> m_vTsFmt;

    string m_strErrMsg;

    char m_chBreak;
    char m_chSep;
    bool m_bNeedParallel;

    // load result
    int m_nReadRows;
	int m_nGetrows;
    int m_nSkipRows;
    int m_nLoadRows;
    int m_nDeleteRows;
    int m_nRejRows;
    int m_nCommitRows;

	FILE *m_fBadLineLog;
	FILE *m_fErrorLog;
	FILE *output_fp;
	FILE *notencode;

	string m_Output;
	int m_nBadLines;

};

#endif


