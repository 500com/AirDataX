/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


/********************************************************************
  Copyright by TAOBAO 2009-05-05
  File Name: oraloader.cpp
  Created Date: 2009-05-05
Author: ALI
Version: 0.0.1
Last Version Date:
Version Date:
Description: dynamic library implementation for Oracle database
Modified history:
Date      Author    Description
--------------------------------------------------------------------
20090505  ALI       Created
20100712  TUHAI     Change line buffer from 1024 to 8192
Add print the first badline feature
 ********************************************************************/
#include <stdio.h>
#include <errno.h>
#include "oradumper.h"


Dumper *Create()
{
	return new OraDumper;
}

OraDumper::OraDumper()
{
	memset(colArray, 0, sizeof(colArray));
	memset(fldArray, 0, sizeof(fldArray));
	memset(&table, 0, sizeof(table));
	memset(&session, 0, sizeof(session));
	memset(&colptr, 0, sizeof(colptr));

	dschp = (OCIDescribe *)0;
	memset( &m_oCtl, 0x00, sizeof(m_oCtl));
	ctlp = &m_oCtl;

	output_fp 		= stdout;
	m_fErrorLog 	= stderr; 

	if (output_fp == NULL || m_fErrorLog == NULL) {
		printf("output_fp, m_fErrorLog NULL\n");
		exit(-1);
	}

	/*------------------------------
	  Chinese Encode Type
	  ------------------------------*/
	m_mEncode["GBK"] 	= "american_america.zhs16gbk";
	m_mEncode["UTF-8"] 	= "american_america.al32utf8";

	/* set simple_load initial state */
	input_recnum 	= 0;
	load_recnum 	= UB4MAXVAL;
	err_recnum 		= 0;
	state 			= RESET;
	fsetrv 			= FIELD_SET_COMPLETE;
	cvtrv 			= CONVERT_SUCCESS;
	ldrv 			= LOAD_SUCCESS;
	done 			= FALSE;
	cvtcontcnt 		= 0;
	dotCnt 			= 0;

	bFirstCall 		= true;
	
	m_lines 		= 0;
	m_inbuf 		= NULL;
	m_outbuf 		= NULL;
	m_cd 			= (iconv_t)-1;
}

OraDumper::~OraDumper()
{
	//free iconv resouce
	iconv_close(m_cd);
	free(m_inbuf);
	free(m_outbuf);

	//free(data_buff);

	cleanup(ctlp, (sb4)EXIT_SUCCEED, (sb4)0);
	/* NOTREACHED */

}

void OraDumper::DumperInit()
{
	// logon information
	char szDBName[64]   = {0}; 
	char szUsername[64] = {0}; 
	char szPassword[64] = {0}; 

	sscanf(m_strLogon.c_str(), "%[^/]/%[^@]@%s",
			szUsername, szPassword, szDBName);

	m_strDBName   = szDBName;
	m_strUsername = szUsername;
	m_strPassword = szPassword;

	// parse date format
	string::iterator sit = m_strDateFormat.begin();
	string::iterator eit = m_strDateFormat.begin();

	m_vDateFmt.clear();
	m_vTsFmt.clear();

	for (string::iterator it = m_strDateFormat.begin(); it != m_strDateFormat.end(); it++) {
		eit = it;

		if (*eit == ',' || (eit + 1) == m_strDateFormat.end()) {
			if (*eit != ',')
				eit++;
			string tmp_str(sit, eit);
			string tmp_ts = tmp_str + ".ff";
			m_vDateFmt.push_back(tmp_str);
			m_vTsFmt.push_back(tmp_ts);

			sit = eit + 1;
		}
	}

	if (m_vDateFmt.size() == 0) {
		string tmp_str("yyyy-mm-dd");
		m_vDateFmt.push_back(tmp_str);
		tmp_str += ".ff";
		m_vTsFmt.push_back(tmp_str);
	}

	// encode type
	if ( m_strEncodeType.length() > 0 ) {

		if( m_mEncode.count(m_strEncodeType) == 0 ) {
			Log(m_fErrorLog, "Error:Unknown Encode Type[%s]!\n", m_strEncodeType.c_str());
			exit(-1);
		} else {
			setenv("NLS_LANG", m_mEncode[m_strEncodeType].c_str(), 1 );
		}
	} else {
		setenv("NLS_LANG", m_mEncode["UTF-8"].c_str(), 0 );  // if nls_lang already defined, not overwrite
	}


	sword ociret;
	/**
	 * set up OCI environment and connect to the ORACLE server 
	 */

	OCI_CHECK((dvoid *)0, (ub4)0, ociret, ctlp,
			OCIInitialize((ub4) OCI_THREADED, (dvoid *)0,(dvoid * (*)(dvoid *, size_t)) 0,
							(dvoid * (*)(dvoid *, dvoid *, size_t))0,(void (*)(dvoid *, dvoid *)) 0 ));

	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIEnvInit((OCIEnv **)&ctlp->envhp_ctl, OCI_DEFAULT, (size_t)0,(dvoid **)0));

	/* allocate error handles */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->errhp_ctl, 
							OCI_HTYPE_ERROR,(size_t)0, (dvoid **)0));

	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->errhp2_ctl, 
							OCI_HTYPE_ERROR,(size_t)0, (dvoid **)0));

	/* server contexts */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->srvhp_ctl, 
							OCI_HTYPE_SERVER,(size_t)0, (dvoid **)0));

	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->svchp_ctl, 
							OCI_HTYPE_SVCCTX,(size_t)0, (dvoid **)0));

	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIServerAttach(ctlp->srvhp_ctl, ctlp->errhp_ctl,(text *)m_strDBName.c_str(),
							(sb4)m_strDBName.length(),OCI_DEFAULT));

	/* set attribute server context in the service context */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)ctlp->svchp_ctl, OCI_HTYPE_SVCCTX,(dvoid *)ctlp->srvhp_ctl, 
						(ub4)0, OCI_ATTR_SERVER,ctlp->errhp_ctl));

	// session handle
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->authp_ctl, 
							(ub4)OCI_HTYPE_SESSION,(size_t)0, (dvoid **)0));

	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)ctlp->authp_ctl, (ub4)OCI_HTYPE_SESSION, (dvoid *)m_strUsername.c_str(),
						(ub4)m_strUsername.length(),(ub4)OCI_ATTR_USERNAME, ctlp->errhp_ctl));

	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)ctlp->authp_ctl, (ub4)OCI_HTYPE_SESSION,(dvoid *)m_strPassword.c_str(),
						(ub4)m_strPassword.length(),(ub4)OCI_ATTR_PASSWORD, ctlp->errhp_ctl));

	/* begin a session */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCISessionBegin(ctlp->svchp_ctl, ctlp->errhp_ctl, ctlp->authp_ctl,
							OCI_CRED_RDBMS, (ub4)OCI_DEFAULT));

	/* set authentication context into service context */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)ctlp->svchp_ctl, (ub4)OCI_HTYPE_SVCCTX,(dvoid *)ctlp->authp_ctl, 
						(ub4)0, (ub4)OCI_ATTR_SESSION,ctlp->errhp_ctl));

	// stmt handle -- SQL
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->stmthp_ctl, (ub4)OCI_HTYPE_STMT,
							(size_t)0, (dvoid **)0));

}

void OraDumper::PreDump(long flag)
{
	sword ociret;
	vector<string> vSQL;

	if (flag != 1) {
		// Pre SQL before load
		if( m_strPreSQL.length() ) {
			string::iterator sit = m_strPreSQL.begin();
			string::iterator eit = m_strPreSQL.begin();

			vSQL.clear();

			for (string::iterator it = m_strPreSQL.begin(); it != m_strPreSQL.end(); it++) {
				eit = it;

				if (*eit == ';' || (eit + 1) == m_strPreSQL.end()) {
					if (*eit != ';')
						eit++;
					string tmp_str(sit, eit);
					vSQL.push_back(tmp_str);

					sit = eit + 1;
				}
			}
		}

		if( m_strMode == "TRUNCATE" ) {
			vSQL.push_back(Format("truncate table %s", m_strTable.c_str()));
		} else if( m_strMode == "TRUNCPART" ) {

			CStrSplit spt;
			if (spt.Init() == -1) {
				Log(m_fErrorLog, "Error:Spt init failed.\n");
				exit(1);
			}

			spt.Split(m_strPart.c_str(), ",");
			for(int i = 1; i <= spt.GetFieldCount(); i++) {
				vSQL.push_back(Format("alter table %s truncate partition %s",
							m_strTable.c_str(), spt[i]));
			}
		}


		// print all sqls
		if( vSQL.size() > 0 ) {
			Log(output_fp, "PreSQL:%s\n.", vSQL[0].c_str() );
			for( size_t i = 1; i < vSQL.size(); i++ )
				Log(output_fp, "        %s\n", vSQL[i].c_str() );
		}

		// execute sql
		for(vector<string>::iterator iter = vSQL.begin(); iter != vSQL.end(); iter++) {
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
					OCIStmtPrepare(ctlp->stmthp_ctl, ctlp->errhp_ctl, (text *)iter->c_str(), 
									iter->length(), OCI_NTV_SYNTAX, 0));

			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
					OCIStmtExecute(ctlp->svchp_ctl, ctlp->stmthp_ctl, ctlp->errhp_ctl, 1, 0, 0, 0, 0));
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
					OCITransCommit(ctlp->svchp_ctl, ctlp->errhp_ctl, (ub4)0));
		}

	} else {

		PreLoadFiles();
	}
}

void OraDumper::RunDump(const char *lines)
{
	
	const char *start = lines;
	char *end = NULL;
	size_t len;
	int ret;
	char *inbuf = m_inbuf;
	char *outbuf = m_outbuf;
	
	while (start != NULL && *start != '\0') {

		end = strchr(start, m_chBreak);

		if (end == NULL) return;

		len = end - start;

		if (len == 15 && strncasecmp(start, "1y9i8x7i0a3o2*5", 15) == 0 ) {
			ret = simple_load("", 0);
			while (ret == 0)
				ret = simple_load("", 0);
			break;
		} else if (len != 0) {
			memset(m_inbuf, 0, 8192);
			memset(m_outbuf, 0, 8192);
			strncpy(m_inbuf, start, len);
            
			if (m_bIsUTF8) {
				simple_load(m_inbuf, strlen(m_inbuf));
			} else {
				int res = iconv(m_cd, &inbuf, &len, &outbuf, &m_outlen);

				if (res == -1) {
					m_notconv++;	
					simple_load(m_inbuf, strlen(m_inbuf)); 				
					
				} else {
					simple_load(m_outbuf, strlen(m_outbuf));
				}
			}
			
			inbuf = m_inbuf;
			outbuf = m_outbuf;
			m_outlen = 8192;
			
			m_nGetrows++;
		}
			
		start = end + 1;
	}

}

void OraDumper::CommitDump(bool bCommit)
{
	finish_load(bCommit);
	PreLoadFiles();
	state = RESET;	
	done  = FALSE;
	bFirstCall = true;
}

int OraDumper::PostDump(long flag)
{
	sword ociret;
	vector<string> vSQL;

	if (flag == 1)
		goto DS;
	else if (flag == 2)
		goto POSTSQL; 

DS:
	// free connection to server
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
				OCIServerDetach(ctlp->srvhp_ctl, ctlp->errhp_ctl, OCI_DEFAULT));

	// free handlers
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIHandleFree(ctlp->stmthp_ctl, (ub4)OCI_HTYPE_STMT) );
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIHandleFree(ctlp->authp_ctl, (ub4)OCI_HTYPE_SESSION) );
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIHandleFree(ctlp->svchp_ctl, (ub4)OCI_HTYPE_SVCCTX) );
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIHandleFree(ctlp->srvhp_ctl, (ub4)OCI_HTYPE_SERVER) );

	OCIHandleFree(ctlp->errhp2_ctl, OCI_HTYPE_ERROR);
	OCIHandleFree(ctlp->errhp_ctl, OCI_HTYPE_ERROR);
	
	if (flag == 1)
		return m_nRejRows;

POSTSQL:
	DumperInit(); // reconnect db server

	if( m_strPostSQL.length() ) {
		string::iterator sit = m_strPostSQL.begin();
		string::iterator eit = m_strPostSQL.begin();

		vSQL.clear();

		for (string::iterator it = m_strPostSQL.begin(); it != m_strPostSQL.end(); it++) {
			eit = it;

			if (*eit == ';' || (eit + 1) == m_strPostSQL.end()) {
				if (*eit != ';')
					eit++;
				string tmp_str(sit, eit);
				vSQL.push_back(tmp_str);

				sit = eit + 1;
			}
		}
	}

	// execute sql
	for(vector<string>::iterator iter = vSQL.begin(); iter != vSQL.end(); iter++) {
		cout << "PostSql: " << *iter << endl;
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
					OCIStmtPrepare(ctlp->stmthp_ctl, ctlp->errhp_ctl, (text *)iter->c_str(), 
									iter->length(), OCI_NTV_SYNTAX, 0));

		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
				OCIStmtExecute(ctlp->svchp_ctl, ctlp->stmthp_ctl, ctlp->errhp_ctl, 1, 0, 0, 0, 0));
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp, 
					OCITransCommit(ctlp->svchp_ctl, ctlp->errhp_ctl, (ub4)0));
	}

	return 0;
}

void OraDumper::PreLoadFiles()
{
	size_t nPos = m_strTable.find(".");
	if( nPos == string::npos ) {
		Log(m_fErrorLog, "Error:Table must like owner.table!\n"); 
		exit(-1);
	}

	text tbl_owner[255] = {0};
	text tbl_table[255] = {0};

	strncpy( (char*)tbl_owner, m_strTable.substr(0, nPos).c_str(), 255 );
	strncpy( (char*)tbl_table, m_strTable.substr(nPos+1).c_str(),  255 );

    table.parallel_tbl = 1;
    table.skipindex = OCI_DIRPATH_INDEX_MAINT_DONT_SKIP_UNUSABLE; // don't skip unusable indexes.

    if (!m_bNeedParallel) {
        /* if oraclewriter set to single-thread, we need to maintain index. */
        table.parallel_tbl = 0;
    } else {
	    table.parallel_tbl = 1;
		table.skipindex = OCI_DIRPATH_INDEX_MAINT_SKIP_ALL; // skip all index maintenance.
//	    if (m_nSkipIndex == 1) {
//		    table.skipindex = OCI_DIRPATH_INDEX_MAINT_SKIP_UNUSABLE; // skip unusable indexes
//	    } else if (m_nSkipIndex == 2) {
//		    table.skipindex = OCI_DIRPATH_INDEX_MAINT_DONT_SKIP_UNUSABLE; // don't skip unusable indexes.
//	    } else {
//		    table.skipindex = OCI_DIRPATH_INDEX_MAINT_SKIP_ALL; // skip all index maintenance.
//	    }
    }

	table.nolog_tbl = 1;
	table.subname_tbl =0;
	ctlp->loadobjcol_ctl =false ;

	session.maxreclen_sess = 8192;

	ub2 numcols = describe_table(ctlp,(text*)m_strTable.c_str());

	table.owner_tbl = &tbl_owner[0];
	table.name_tbl  = &tbl_table[0];
	table.ncol_tbl  = numcols;


	table.col_tbl = &colArray[0];
	table.fld_tbl = &fldArray[0];
	table.xfrsz_tbl = (ub4)65536;

	// iconv init
	if (m_cd != (iconv_t)-1) {
		iconv_close(m_cd);	
		m_cd = (iconv_t)-1;
	}

	m_cd = iconv_open("GBK", "UTF-8");
	if (m_cd == (iconv_t)-1) {
		Log(stderr, "iconv_open failed.\n");
		exit(-1);
	}

	int one = 1;
	iconvctl(m_cd,ICONV_SET_DISCARD_ILSEQ,&one);

	m_outlen = 81920;
	m_notconv = 0;

	if (m_inbuf != NULL) {
		free(m_inbuf);
	}
	m_inbuf = (char *)malloc(81920);

	if (m_outbuf != NULL) {
		free(m_outbuf);
	}
	m_outbuf = (char *)malloc(81920);

	if (m_inbuf == NULL || m_outbuf == NULL) {
		Log(stderr, "%s malloc failed.\n", __FUNCTION__);
		exit(-1);	
	}

	init_load(ctlp, &table, &session); /* initialize the load */

	return;
}

ub4 OraDumper::describe_table (struct loadctl *ctlp, text *tablename)
{
	sword     retval;
	OCIParam *parmp, *collst;
	ub2       numcols;
	ub4       objid = 0;

	checkerr (ctlp->errhp_ctl, OCIHandleAlloc((dvoid *)ctlp->envhp_ctl, (dvoid **) &dschp,
				(ub4) OCI_HTYPE_DESCRIBE,
				(size_t) 0, (dvoid **) 0));

	if ((retval = OCIDescribeAny(ctlp->svchp_ctl, ctlp->errhp_ctl, (dvoid *)tablename,
					(ub4) strlen((char *) tablename),
					OCI_OTYPE_NAME, (ub1)1,
					OCI_PTYPE_TABLE, dschp)) != OCI_SUCCESS) {

		if (retval == OCI_NO_DATA) {
			Log(m_fErrorLog, "NO DATA: OCIDescribeAny on %s\n", tablename);
		} else {/* OCI_ERROR */
			Log(m_fErrorLog, "ERROR: OCIDescribeAny on %s\n", tablename);
			checkerr(ctlp->errhp_ctl, retval);
			return 0;
		}

	} else {
		/* get the parameter descriptor */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)dschp, (ub4)OCI_HTYPE_DESCRIBE,
					(dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
					(OCIError *)ctlp->errhp_ctl));

		/* Get the attributes of the table */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &objid, (ub4 *) 0,
					(ub4) OCI_ATTR_OBJID, (OCIError *)ctlp->errhp_ctl));

		/* column list of the table */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &collst, (ub4 *) 0,
					(ub4) OCI_ATTR_LIST_COLUMNS, (OCIError *)ctlp->errhp_ctl));

		/* number of columns */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &numcols, (ub4 *) 0,
					(ub4) OCI_ATTR_NUM_COLS, (OCIError *)ctlp->errhp_ctl));

		/* now describe each column */
		/* !!! numcols maybe changed !!! */
		describe_column(ctlp, collst, numcols);
	}
	/* free the describe handle */
	OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);

	return (ub4)numcols;
}

void OraDumper::describe_column(struct loadctl *ctlp,OCIParam *parmp, ub2 &parmcnt)
{
	char      colname1[64];
	text      colname2[NPOS][64], colname3[NPOS][64];
	text     *namep;
	ub4       sizep;
	ub2       coltyp[NPOS];
	OCIParam *parmdp;
	ub4       i, pos;
	ub2       collen;
	///sword     retval;
	ub1       precision[NPOS];
	sb1       scale[NPOS];

	for (pos = 1; pos <= parmcnt; pos++) {
		/* get the parameter descriptor for each column */
		checkerr (ctlp->errhp_ctl, OCIParamGet((dvoid *)parmp, (ub4)OCI_DTYPE_PARAM, ctlp->errhp_ctl,
					(dvoid **)&parmdp, (ub4) pos));
		/* column length */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &collen, (ub4 *) 0,
					(ub4) OCI_ATTR_DATA_SIZE, (OCIError *)ctlp->errhp_ctl));
		vColLen.push_back(collen);
		/* column name */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &namep, (ub4 *) &sizep,
					(ub4) OCI_ATTR_NAME, (OCIError *)ctlp->errhp_ctl));

		strncpy(colname1, (char *)namep, (size_t) sizep);
		colname1[sizep] = '\0';
		vColName.push_back(colname1);
		/* schema name */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &namep, (ub4 *) &sizep,
					(ub4) OCI_ATTR_SCHEMA_NAME, (OCIError *)ctlp->errhp_ctl));

		strncpy((char *)colname2[pos-1], (char *)namep, (size_t) sizep);
		colname2[pos-1][sizep] = '\0';
		/* type name */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &namep, (ub4 *) &sizep,
					(ub4) OCI_ATTR_TYPE_NAME, (OCIError *)ctlp->errhp_ctl));

		strncpy((char *)colname3[pos-1], (char *)namep, (size_t) sizep);
		colname3[pos-1][sizep] = '\0';
		/* data type */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &coltyp[pos-1], (ub4 *) 0,
					(ub4) OCI_ATTR_DATA_TYPE, (OCIError *)ctlp->errhp_ctl));
		/* precision */
		checkerr (ctlp->errhp_ctl, OCIAttrGet ((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &precision[pos-1], (ub4 *) 0,
					(ub4) OCI_ATTR_PRECISION, (OCIError *)ctlp->errhp_ctl));
		/* scale */
		checkerr (ctlp->errhp_ctl, OCIAttrGet ((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
					(dvoid*) &scale[pos-1], (ub4 *) 0,
					(ub4) OCI_ATTR_SCALE, (OCIError *)ctlp->errhp_ctl));

		/* if column or attribute is type OBJECT/COLLECTION, describe it by ref */
		if (coltyp[pos-1] == OCI_TYPECODE_OBJECT ||
				coltyp[pos-1] == OCI_TYPECODE_NAMEDCOLLECTION) {

			OCIDescribe *deshp;
			OCIParam    *parmhp;
			OCIRef      *typeref;

			/* get ref to attribute/column type */
			checkerr (ctlp->errhp_ctl, OCIAttrGet ((dvoid *)parmdp, (ub4)OCI_DTYPE_PARAM,
						(dvoid *)&typeref, (ub4 *)0,
						(ub4)OCI_ATTR_REF_TDO, (OCIError *)ctlp->errhp_ctl));
			/* describe it */
			checkerr (ctlp->errhp_ctl, OCIHandleAlloc((dvoid *)ctlp->envhp_ctl, (dvoid **)&deshp,
						(ub4)OCI_HTYPE_DESCRIBE, (size_t)0, (dvoid **)0));

			checkerr (ctlp->errhp_ctl, OCIDescribeAny(ctlp->svchp_ctl, ctlp->errhp_ctl, (dvoid *)typeref, (ub4)0,
						OCI_OTYPE_REF, (ub1)1, OCI_PTYPE_TYPE, deshp));
			/* get the parameter descriptor */
			checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)deshp, (ub4)OCI_HTYPE_DESCRIBE,
						(dvoid *)&parmhp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
						(OCIError *)ctlp->errhp_ctl));
			/* describe the type */
			describe_type (ctlp,parmhp);
			/* free describe handle */
			OCIHandleFree((dvoid *) deshp, (ub4) OCI_HTYPE_DESCRIBE);
		}
	}

	map<int, int> mColMap;
	vector<string>::iterator iter;
	if( 0 == strncasecmp(m_strColOrder.c_str(), "ascii", 5)) {

		vector<string> vSColName = vColName;
		sort( vSColName.begin(), vSColName.end());

		for(i = 0; i < parmcnt; i++) {
			for( iter = vSColName.begin(); iter != vSColName.end(); iter++) {
				if( 0 == strncasecmp( vColName[i].c_str(), iter->c_str(), iter->length()+1)) {
					mColMap[i] = iter - vSColName.begin();
					break;
				}
			}
		}
	} else if( m_strColOrder.length() > 0 ) {
		CStrSplit spt;
		if (spt.Init() == -1) {
			Log(m_fErrorLog, "Error:Spt init failed.\n");
			exit(-1);
		}
		
		spt.Split( m_strColOrder.c_str(), ",");

		parmcnt = spt.GetFieldCount();         //!!! change column count
		for(i = 0; i < parmcnt; i++) {
			for( iter = vColName.begin(); iter != vColName.end(); iter++) {
				if( 0 == strncasecmp( spt[i+1], iter->c_str(), iter->length()+1)) {
					mColMap[i] = iter - vColName.begin();
					break;
				}
			}
			
			if( iter == vColName.end() ) {
				Log(m_fErrorLog, "Error: Not Found Column[%s]!", spt[i + 1]); 
				cleanup(ctlp, (sb4)EXIT_FAILED);
			}
		}
	} else {
		for(i = 0; i < parmcnt; i++)
			mColMap[i] = i;
	}

	int date_count = 0;
	for (i = 0; i < mColMap.size(); i++) {
		colArray[i].name_col = (text *)vColName[mColMap[i]].c_str();
		colArray[i].exttyp_col = OCI_TYPECODE_VARCHAR;
		
		if( OCI_TYPECODE_DATE == coltyp[mColMap[i]]) {
			colArray[i].date_col   = 1;
			//colArray[i].datemask_col = (text *)m_strDateFormat.c_str();
			if (date_count < m_vDateFmt.size())
				colArray[i].datemask_col = (text *)(m_vDateFmt[date_count].c_str());
			else
				colArray[i].datemask_col = (text *)(m_vDateFmt[0].c_str());

			date_count++;
		} else if (OCI_TYPECODE_TIMESTAMP == coltyp[mColMap[i]]) {
			colArray[i].date_col = 1;
			//colArray[i].datemask_col = (text *)m_strTimeStamp.c_str();
			if (date_count < m_vTsFmt.size())
				colArray[i].datemask_col = (text *)(m_vTsFmt[date_count].c_str());
			else
				colArray[i].datemask_col = (text *)(m_vTsFmt[0].c_str());

			date_count++;
		}

		// OCI_TYPECODE_OBJECT
		colArray[i].prec_col   = precision[mColMap[i]];
		colArray[i].scale_col  = scale[mColMap[i]];
		fldArray[i].maxlen_fld = (ub2)vColLen[mColMap[i]];
	}
}

void OraDumper::describe_type(struct loadctl *ctlp,OCIParam *type_parmp)
{
	///sword         retval;
	OCITypeCode   typecode,
				  collection_typecode;
	text          schema_name[MAXLEN],
				  version_name[MAXLEN],
				  type_name[MAXLEN];
	text         *namep;
	ub4           size;                                           /* not used */
	OCIRef       *type_ref;                                       /* not used */
	ub2           num_attr,
				  num_method;
	ub1           is_incomplete,
				  has_table;
	OCIParam     *list_attr,
				 *list_method,
				 *map_method,
				 *order_method,
				 *collection_elem;

	Log(output_fp, "\n\n-----------------\n");
	Log(output_fp, "USED-DEFINED TYPE\n-----------------\n");

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &namep, (ub4 *) &size,
				(ub4) OCI_ATTR_SCHEMA_NAME, (OCIError *)ctlp->errhp_ctl));
	strncpy((char *)schema_name, (char *)namep, (size_t) size);
	schema_name[size] = '\0';
	Log(output_fp, "Schema:            %s\n", schema_name);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &namep, (ub4 *) &size,
				(ub4) OCI_ATTR_NAME, (OCIError *)ctlp->errhp_ctl));
	strncpy ((char *)type_name, (char *)namep, (size_t) size);
	type_name[size] = '\0';
	Log(output_fp, "Name:              %s\n", type_name);

	/* get ref of type, although we are not using it here */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&type_ref, (ub4 *)0,
				(ub4)OCI_ATTR_REF_TDO, (OCIError *)ctlp->errhp_ctl));

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &typecode, (ub4 *) 0,
				(ub4) OCI_ATTR_TYPECODE, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Oracle Typecode:   %d\n", typecode);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &namep, (ub4 *) &size,
				(ub4) OCI_ATTR_VERSION, (OCIError *)ctlp->errhp_ctl));
	strncpy ((char *)version_name, (char *)namep, (size_t) size);
	version_name[size] = '\0';
	Log(output_fp, "Version:           %s\n", version_name);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &is_incomplete, (ub4 *) 0,
				(ub4) OCI_ATTR_IS_INCOMPLETE_TYPE, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Is incomplete:     %d\n", is_incomplete);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &has_table, (ub4 *) 0,
				(ub4) OCI_ATTR_HAS_NESTED_TABLE, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Has nested table:  %d\n", has_table);

	/* describe type attributes if any */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &num_attr, (ub4 *) 0,
				(ub4) OCI_ATTR_NUM_TYPE_ATTRS, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Number of attrs:   %d\n", num_attr);
	if (num_attr > 0) {
		/* get the list of attributes and pass it on */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&list_attr, (ub4 *)0,
					(ub4)OCI_ATTR_LIST_TYPE_ATTRS, (OCIError *)ctlp->errhp_ctl));
		describe_typeattr(ctlp,list_attr, num_attr);
	}

	/* describe the collection element if this is a collection type */
	if (typecode == OCI_TYPECODE_NAMEDCOLLECTION) {

		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&collection_typecode, (ub4 *)0,
					(ub4)OCI_ATTR_COLLECTION_TYPECODE, (OCIError *)ctlp->errhp_ctl));
		Log(output_fp, "Collection typecode: %d\n", collection_typecode);

		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&collection_elem, (ub4 *)0,
					(ub4)OCI_ATTR_COLLECTION_ELEMENT, (OCIError *)ctlp->errhp_ctl));

		describe_typecoll(ctlp,collection_elem, collection_typecode);
	}

	/* describe the MAP method if any */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
				(dvoid*) &map_method, (ub4 *) 0,
				(ub4) OCI_ATTR_MAP_METHOD, (OCIError *)ctlp->errhp_ctl));
	if (map_method != (dvoid *)0)
		describe_typemethod(ctlp,map_method,(text *)"TYPE MAP METHOD\n---------------");

	/* describe the ORDER method if any; note that this is mutually exclusive */
	/* with MAP                                                               */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&order_method, (ub4 *)0,
				(ub4)OCI_ATTR_ORDER_METHOD, (OCIError *)ctlp->errhp_ctl));
	if (order_method != (dvoid *)0)
		describe_typemethod(ctlp,order_method,
				(text *)"TYPE ORDER METHOD\n-----------------");

	/* describe all methods (including MAP/ORDER) */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&num_method, (ub4 *)0,
				(ub4)OCI_ATTR_NUM_TYPE_METHODS, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Number of methods: %d\n", num_method);
	if (num_method > 0) {

		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&list_method, (ub4 *)0,
					(ub4)OCI_ATTR_LIST_TYPE_METHODS, (OCIError *)ctlp->errhp_ctl));

		describe_typemethodlist(ctlp,list_method, num_method,
				(text *)"TYPE METHOD\n-----------");
	}
}

void OraDumper::describe_typecoll(struct loadctl *ctlp,OCIParam  *collelem_parmp, sword coll_typecode)
	/* OCI_TYPECODE_VARRAY or OCI_TYPECODE_TABLE */
{
	///text         *attr_name,
	text		 *schema_name,
				 *type_name;
	ub4           size;
	ub2           datasize;
	ub4           num_elems;
	OCITypeCode   typecode;
	ub2           datatype;
	///sword         retval;

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&schema_name, (ub4 *)&size,
				(ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)ctlp->errhp_ctl));
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&type_name, (ub4 *)&size,
				(ub4)OCI_ATTR_TYPE_NAME, (OCIError *)ctlp->errhp_ctl));
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&datasize, (ub4 *)0,
				(ub4)OCI_ATTR_DATA_SIZE, (OCIError *)ctlp->errhp_ctl));
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&typecode, (ub4 *)0,
				(ub4)OCI_ATTR_TYPECODE, (OCIError *)ctlp->errhp_ctl));
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&datatype, (ub4 *)0,
				(ub4)OCI_ATTR_DATA_TYPE, (OCIError *)ctlp->errhp_ctl));
	if (coll_typecode == OCI_TYPECODE_VARRAY)
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&num_elems, (ub4 *)0,
					(ub4)OCI_ATTR_NUM_ELEMS, (OCIError *)ctlp->errhp_ctl));
	else num_elems = 0;

	Log(output_fp, "Schema    Type            Length   Type Datatype Elements\n");
	Log(output_fp, "_________________________________________________________\n");

	Log(output_fp, "%10s%16s%9d%5d%9d%8d\n", schema_name, type_name,
			datasize, typecode, datatype, num_elems);
}

void OraDumper::describe_typemethodlist(struct loadctl *ctlp,OCIParam *methodlist_parmp, ub4 num_method, text *comment)
{
	///sword      retval;
	OCIParam  *method_parmp;
	ub4        pos;
	/* traverse the method list */
	for (pos = 1; pos <= num_method; pos++) {
		checkerr (ctlp->errhp_ctl, OCIParamGet((dvoid *)methodlist_parmp,
					(ub4)OCI_DTYPE_PARAM, ctlp->errhp_ctl,
					(dvoid **)&method_parmp, (ub4)pos));

		describe_typemethod(ctlp,method_parmp, comment);
	}
}

void OraDumper::describe_typemethod(struct loadctl *ctlp,OCIParam *method_parmp, text *comment)
{
	///sword          retval;
	text          *method_name;
	ub4            size;
	ub2            ovrid;
	ub4            num_arg;
	ub1            has_result,
				   is_map,
				   is_order;
	OCITypeEncap   encap;
	OCIParam      *list_arg;

	/* print header */
	Log(output_fp, "\n%s\n", comment);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&method_name, (ub4 *)&size,
				(ub4)OCI_ATTR_NAME, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Method Name:       %s\n", method_name);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&ovrid, (ub4 *)0,
				(ub4)OCI_ATTR_OVERLOAD_ID, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Overload ID:       %d\n", ovrid);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&encap, (ub4 *)0,
				(ub4)OCI_ATTR_ENCAPSULATION, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Encapsulation:     %s\n",
			(encap == OCI_TYPEENCAP_PUBLIC) ? "public" : "private");

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&is_map, (ub4 *)0,
				(ub4)OCI_ATTR_IS_MAP, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Is map:            %d\n", is_map);

	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&is_order, (ub4 *)0,
				(ub4)OCI_ATTR_IS_ORDER, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Is order:          %d\n", is_order);

	/* retrieve the argument list, includes result */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&list_arg, (ub4 *)0,
				(ub4)OCI_ATTR_LIST_ARGUMENTS, (OCIError *)ctlp->errhp_ctl));

	/* if this is a function (has results, then describe results */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&has_result, (ub4 *)0,
				(ub4)OCI_ATTR_HAS_RESULT, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Has result:        %d\n", has_result);
	if (has_result) {
		describe_typearg(ctlp,list_arg, OCI_PTYPE_TYPE_RESULT, 0, 1);
	}

	/* describe each argument */
	checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
				(dvoid *)&num_arg, (ub4 *)0,
				(ub4)OCI_ATTR_NUM_ARGS, (OCIError *)ctlp->errhp_ctl));
	Log(output_fp, "Number of args:    %d\n", num_arg);

	if (num_arg > 0) {
		describe_typearg(ctlp,list_arg, OCI_PTYPE_TYPE_ARG, 1, num_arg+1);
	}
}

void OraDumper::describe_typearg (struct loadctl *ctlp,OCIParam *arglist_parmp, ub1 type, ub4 start,ub4 end)
{
	OCIParam          *arg_parmp;
	///sword              retval;
	text              *arg_name,
					  *schema_name,
					  *type_name;
	ub2                position;
	ub2                level;
	ub1                has_default;
	OCITypeParamMode   iomode;
	ub4                size;
	OCITypeCode        typecode;
	ub2                datatype;
	///ub4                i,
	ub4				   pos;

	/* print header */
	Log(output_fp, "Name    Pos   Type Datatype Lvl Def Iomode SchName TypeName\n");
	Log(output_fp, "________________________________________________________________\n");

	for (pos = start; pos < end; pos++) {
		/* get the attribute's describe handle from the parameter */
		checkerr (ctlp->errhp_ctl, OCIParamGet((dvoid *)arglist_parmp, (ub4)OCI_DTYPE_PARAM,
					ctlp->errhp_ctl, (dvoid **)&arg_parmp, (ub4)pos));

		/* obtain attribute values for the type's attributes */
		/* if this is a result, it has no name, so we give it one */
		if (type == OCI_PTYPE_TYPE_RESULT) {
			arg_name = (text *)"RESULT";
		} else if (type == OCI_PTYPE_TYPE_ARG) {
			checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
						(dvoid *)&arg_name, (ub4 *)&size,
						(ub4)OCI_ATTR_NAME, (OCIError *)ctlp->errhp_ctl));
		}

		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&position, (ub4 *)0,
					(ub4)OCI_ATTR_POSITION, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&typecode, (ub4 *)0,
					(ub4)OCI_ATTR_TYPECODE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&datatype, (ub4 *)0,
					(ub4)OCI_ATTR_DATA_TYPE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&level, (ub4 *)0,
					(ub4)OCI_ATTR_LEVEL, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&has_default, (ub4 *)0,
					(ub4)OCI_ATTR_HAS_DEFAULT, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&iomode, (ub4 *)0,
					(ub4)OCI_ATTR_IOMODE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&schema_name, (ub4 *)&size,
					(ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&type_name, (ub4 *)&size,
					(ub4)OCI_ATTR_TYPE_NAME, (OCIError *)ctlp->errhp_ctl));

		/* if typecode == OCI_TYPECODE_OBJECT, you can proceed to describe it
		   recursively by calling describe_type() with its name; or you can
		   obtain its OCIRef by using OCI_ATTR_REF_TDO, and then describing the
		   type by REF                                                          */

		/* print values for the argument */
		printf ("%8s%5d%5d%9d%4d%3c%7d%8s%14s\n", arg_name, position,
				typecode, datatype, level, has_default ? 'y' : 'n',
				iomode, schema_name, type_name);
	}
}

void OraDumper::describe_typeattr(struct loadctl *ctlp,OCIParam *attrlist_parmp, ub4 num_attr)
{
	OCIParam     *attr_parmp;
	///sword         retval;
	text         *attr_name,
				 *schema_name,
				 *type_name;
	ub4           namesize, snamesize, tnamesize;
	///ub4           size;
	ub2           datasize;
	OCITypeCode   typecode;
	ub2           datatype;
	ub1           precision;
	sb1           scale;
	///ub4           i;
	ub4			  pos;

	Log(output_fp, "\nAttr Name      Schema      Type        Length Typ Datatyp Pre Scal\n");
	Log(output_fp, "____________________________________________________________________\n");

	for (pos = 1; pos <= num_attr; pos++) {
		/* get the attribute's describe handle from the parameter */
		checkerr (ctlp->errhp_ctl, OCIParamGet((dvoid *)attrlist_parmp, (ub4)OCI_DTYPE_PARAM,
					ctlp->errhp_ctl, (dvoid **)&attr_parmp, (ub4)pos));

		/* obtain attribute values for the type's attributes */
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&attr_name, (ub4 *)&namesize,
					(ub4)OCI_ATTR_NAME, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&schema_name, (ub4 *)&snamesize,
					(ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&type_name, (ub4 *)&tnamesize,
					(ub4)OCI_ATTR_TYPE_NAME, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&datasize, (ub4 *)0,
					(ub4)OCI_ATTR_DATA_SIZE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&typecode, (ub4 *)0,
					(ub4)OCI_ATTR_TYPECODE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&datatype, (ub4 *)0,
					(ub4)OCI_ATTR_DATA_TYPE, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&precision, (ub4 *)0,
					(ub4)OCI_ATTR_PRECISION, (OCIError *)ctlp->errhp_ctl));
		checkerr (ctlp->errhp_ctl, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
					(dvoid *)&scale, (ub4 *)0,
					(ub4)OCI_ATTR_SCALE, (OCIError *)ctlp->errhp_ctl));

		/* if typecode == OCI_TYPECODE_OBJECT, you can proceed to describe it
		   recursively by calling describe_type() with its name; or you can
		   obtain its OCIRef by using OCI_ATTR_REF_TDO, and then describing the
		   type by REF                                                          */

		/* print values for the attribute */
		Log(output_fp, "%10.*s%10.*s%16.*s%8d%4d%8d%4d%5d\n", namesize, attr_name,
				snamesize, schema_name, tnamesize, type_name, datasize,
				typecode, datatype, precision, scale);
	}

	Log(output_fp, "\n");
}

void OraDumper::checkerr(OCIError *errhp, sword status)
{
	text errbuf[512];
	sb4 errcode = 0;

	switch (status)
	{
		case OCI_SUCCESS:
			break;
		case OCI_SUCCESS_WITH_INFO:
			(void) Log(m_fErrorLog, "Error - OCI_SUCCESS_WITH_INFO\n");
			break;
		case OCI_NEED_DATA:
			(void) Log(m_fErrorLog, "Error - OCI_NEED_DATA\n");
			break;
		case OCI_NO_DATA:
			(void) Log(m_fErrorLog, "Error - OCI_NODATA\n");
			break;
		case OCI_ERROR:
			(void) OCIErrorGet ((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
					errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
			(void) Log(m_fErrorLog, "Error - %.*s\n", 512, errbuf);
			break;
		case OCI_INVALID_HANDLE:
			(void) Log(m_fErrorLog, "Error - OCI_INVALID_HANDLE\n");
			break;
		case OCI_STILL_EXECUTING:
			(void) Log(m_fErrorLog, "Error - OCI_STILL_EXECUTE\n");
			break;
		case OCI_CONTINUE:
			(void) Log(m_fErrorLog, "Error - OCI_CONTINUE\n");
			break;
		default:
			break;
	}
}
/*
 **++++++++++++++++++++++++++++++ alloc_obj_ca +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Function allocates the column arrays for any objects or nested object columns.
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** tblp table pointer 
 ** objp object pointer 
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::alloc_obj_ca(struct loadctl *ctlp,struct tbl *tblp,struct obj *objp)
	/* load control structure pointer */ /* table pointer */ /* object pointer */
{
	struct col *colp;
	sword ociret; /* return code from OCI calls*/
	ub2 i;

	/*
	 * Allocate a separate column array for the column object
	 */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)(objp->ctx_obj),(dvoid **)&(objp->ca_obj),
							(ub4)OCI_HTYPE_DIRPATH_FN_COL_ARRAY,(size_t)0, (dvoid **)0));

	/* get number of rows in the column array just allocated */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((CONST dvoid *)(objp->ca_obj),OCI_HTYPE_DIRPATH_FN_COL_ARRAY,
					(dvoid *)(&objp->nrows_obj), (ub4 *)0,OCI_ATTR_NUM_ROWS, ctlp->errhp_ctl));

	/* get number of columns in the column array just allocated */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((CONST dvoid *)(objp->ca_obj),OCI_HTYPE_DIRPATH_FN_COL_ARRAY,
						(dvoid *)(&objp->ncol_obj), (ub4 *)0,OCI_ATTR_NUM_COLS, ctlp->errhp_ctl));

	/*
	 * If there are fewer rows in the object column array than in them top-level,
	 * one, only use as many rows in the other column array. This will happen
	 * when the object requires more space than all of the other columns inits
	 * parent table. This simplifies the loop for loading the column arrays
	 * so that we only have to worry about when we've filled the top-level
	 * column array.
	 */
	if (objp->nrows_obj < ctlp->nrow_ctl) {
		ctlp->nrow_ctl = objp->nrows_obj;
	}

	/* check each column to see if it is an object, opaque or ref */
	/* and if so, recurse */
	for (i = 0, colp = objp->col_obj; i < objp->ncol_obj; i++, colp++) {
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF) {
			alloc_obj_ca(ctlp, tblp, colp->obj_col);
		}
	}

}


/*
 **++++++++++++++++++++++++++++++ init_obj_load +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Function which prepares the load of an object column. This should only
 ** be called from init_load or recursively.
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** tblp table pointer 
 ** objp object pointer 
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::init_obj_load(struct loadctl *ctlp,struct tbl *tblp,struct obj *objp)
{
	struct col *colp;
	struct fld *fldp;
	sword ociret; /* return code from OCI calls*/
	ub2 i;
	ub4 pos;
	ub2 numCols;
	///ub4 len;
	///ub2 type;
	ub1 exprtype;
	ub1 parmtyp;
	OCIParam *colDesc; /* column parameter descriptor*/
	OCIParam *objColLstDesc; /* obj col's list param handle*/

	/*
	 * create a context for this object type and describe the attributes
	 * that will be loaded for this object.
	 */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->dpctx_ctl,(dvoid **)&(objp->ctx_obj),
						(ub4)OCI_HTYPE_DIRPATH_FN_CTX,(size_t)0, (dvoid **)0));

	/* If col is an obj, then its constructor is the type name. (req.)
	 * If col is an opaque/sql function, then use the expression given. (req.)
	 * If col is a ref, then can set a fixed tbl name. (optional) 
	 */

	if (objp->name_obj) /* if expression is available */
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)(objp->ctx_obj),(ub4)OCI_HTYPE_DIRPATH_FN_CTX,(dvoid *) objp->name_obj,(ub4)strlen((const char *) objp->name_obj),(ub4)OCI_ATTR_NAME, ctlp->errhp_ctl));
		/* Set the expression type to obj constructor, opaque/sql function, or ref
		 * table name.
		 */
		if (bit(objp->flag_obj, OBJ_OBJ))
			exprtype = OCI_DIRPATH_EXPR_OBJ_CONSTR; /* expr is an obj constructor */
		else if (bit(objp->flag_obj, OBJ_OPQ))
			exprtype = OCI_DIRPATH_EXPR_SQL; /* expr is an opaque/sql func */
		else if (bit(objp->flag_obj, OBJ_REF))
			exprtype = OCI_DIRPATH_EXPR_REF_TBLNAME; /* expr is a ref table name */

		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)(objp->ctx_obj),(ub4)OCI_HTYPE_DIRPATH_FN_CTX,(dvoid *) &exprtype,(ub4) 0,(ub4)OCI_ATTR_DIRPATH_EXPR_TYPE, ctlp->errhp_ctl));
	}
	/* set number of columns to be loaded */
	numCols = objp->ncol_obj;
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)(objp->ctx_obj),(ub4)OCI_HTYPE_DIRPATH_FN_CTX,(dvoid *)&numCols,(ub4)0, (ub4)OCI_ATTR_NUM_COLS, ctlp->errhp_ctl));

	/* get the column parameter list */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrGet((dvoid *)(objp->ctx_obj),OCI_HTYPE_DIRPATH_FN_CTX,(dvoid *)&objColLstDesc, (ub4 *)0,OCI_ATTR_LIST_COLUMNS, ctlp->errhp_ctl));

	/* get attributes of the column parameter list */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrGet((CONST dvoid *)objColLstDesc,OCI_DTYPE_PARAM,(dvoid *)&parmtyp, (ub4 *)0,OCI_ATTR_PTYPE, ctlp->errhp_ctl));

	if (parmtyp != OCI_PTYPE_LIST)
	{
		Log(m_fErrorLog,"ERROR: expected parmtyp of OCI_PTYPE_LIST, got %d\n",(int)parmtyp);
	}

	/* Now set the attributes of each column by getting a parameter
	 * handle on each column, then setting attributes on the parameter
	 * handle for the column.
	 * Note that positions within a column list descriptor are 1-based.
	 */
	for (i = 0, pos = 1, colp = objp->col_obj, fldp = objp->fld_obj;i < objp->ncol_obj;i++, pos++, colp++, fldp++)
	{
		/* get parameter handle on the column */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIParamGet((CONST dvoid *)objColLstDesc,(ub4)OCI_DTYPE_PARAM, ctlp->errhp_ctl,(dvoid **)&colDesc, pos));

		colp->id_col = i; /* position in column array*/

		/* set external attributes on the column */

		/* column name */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)colp->name_col,(ub4)strlen((const char *)colp->name_col),(ub4)OCI_ATTR_NAME, ctlp->errhp_ctl));

		/* column type */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->exttyp_col, (ub4)0,(ub4)OCI_ATTR_DATA_TYPE, ctlp->errhp_ctl));

		/* max data size */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&fldp->maxlen_fld, (ub4)0,(ub4)OCI_ATTR_DATA_SIZE, ctlp->errhp_ctl));

		/* If column is chrdate or date, set column (input field) date mask
		 * to trigger client library to check string for a valid date.
		 * Note: OCIAttrSet() may be called here w/ a null ptr or null string.
		 */

		if (colp->date_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)colp->datemask_col,(colp->datemask_col) ?(ub4)strlen((const char *)colp->datemask_col) :0,(ub4)OCI_ATTR_DATEFORMAT, ctlp->errhp_ctl));
		}

		if (colp->prec_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->prec_col, (ub4)0,(ub4)OCI_ATTR_PRECISION, ctlp->errhp_ctl));
		}

		if (colp->scale_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->scale_col, (ub4)0,(ub4)OCI_ATTR_SCALE, ctlp->errhp_ctl));
		}

		if (colp->csid_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->csid_col, (ub4)0,(ub4)OCI_ATTR_CHARSET_ID, ctlp->errhp_ctl));
		}

		/* If this is an object, opaque or ref then recurse */
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
		{
			init_obj_load(ctlp, tblp, colp->obj_col);

			/* set the object function context into the param handle */
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)(colp->obj_col->ctx_obj), (ub4)0,(ub4)OCI_ATTR_DIRPATH_FN_CTX, ctlp->errhp_ctl));
		}

		/* free the parameter handle to the column descriptor */
		OCI_CHECK((dvoid *)0, 0, ociret, ctlp,OCIDescriptorFree((dvoid *)colDesc, OCI_DTYPE_PARAM));
	}

}


/*
 **++++++++++++++++++++++++++++++ init_load +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Function which prepares for a direct path load using the direct
 ** path API on the table described by 'tblp'.
 **
 ** Assumptions:
 **
 ** The loadctl structure given by 'ctlp' has an appropriately initialized
 ** environment, and service context handles (already connected to
 ** the server) prior to calling this function.
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer
 ** tblp table pointer 
 ** sessp session pointer
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::init_load(struct loadctl *ctlp, struct tbl *tblp, struct sess *sessp)
{
	struct col 		*colp;
	struct fld 		*fldp;
	sword 			ociret; /* return code from OCI calls */
	OCIDirPathCtx 	*dpctx; /* direct path context */
	///OCIParam 		*objAttrDesc; /* attribute parameter descriptor */
	OCIParam 		*colDesc; /* column parameter descriptor */
	ub1 			parmtyp;
	///ub1 			*timestamp = (ub1 *)0;
	///ub4 			size;
	ub2 			i;
	ub4 			pos;
	ub1 			dirpathinput = OCI_DIRPATH_INPUT_TEXT;
	///int 			retval;

	/* allocate and initialize a direct path context */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->envhp_ctl,(dvoid **)&ctlp->dpctx_ctl,(ub4)OCI_HTYPE_DIRPATH_CTX,(size_t)0, (dvoid **)0));

	dpctx = ctlp->dpctx_ctl; /* shorthand*/

	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)tblp->name_tbl,(ub4)strlen((const char *)tblp->name_tbl),(ub4)OCI_ATTR_NAME, ctlp->errhp_ctl));

	if (tblp->subname_tbl && *tblp->subname_tbl) /* set (sub)partition name*/
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)tblp->subname_tbl,(ub4)strlen((const char *)tblp->subname_tbl),(ub4)OCI_ATTR_SUB_NAME, ctlp->errhp_ctl));
	}
	if (tblp->owner_tbl) /* set schema (owner) name*/
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)tblp->owner_tbl,(ub4)strlen((const char *)tblp->owner_tbl),(ub4)OCI_ATTR_SCHEMA_NAME, ctlp->errhp_ctl));
	}
	/* Note: setting tbl default datemask will not trigger client library
	 * to check strings for dates - only setting column datemask will.
	 */
	if (tblp->dfltdatemask_tbl)
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)tblp->dfltdatemask_tbl,(ub4)strlen((const char *)tblp->dfltdatemask_tbl),(ub4)OCI_ATTR_DATEFORMAT, ctlp->errhp_ctl));
	}
	/* set the data input type to be text */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)dpctx, OCI_HTYPE_DIRPATH_CTX,(dvoid *)&dirpathinput, (ub4)0, OCI_ATTR_DIRPATH_INPUT, ctlp->errhp_ctl));

	if (tblp->parallel_tbl) /* set table level parallel option*/
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)&tblp->parallel_tbl,(ub4)0, (ub4)OCI_ATTR_DIRPATH_PARALLEL,ctlp->errhp_ctl));
    }

	if (tblp->skipindex) 
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)&tblp->skipindex,
							(ub4)0, (ub4)OCI_ATTR_DIRPATH_SKIPINDEX_METHOD, ctlp->errhp_ctl));
	}

	if (tblp->nolog_tbl) /* set table level nolog option*/
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)&tblp->nolog_tbl, (ub4)0,(ub4)OCI_ATTR_DIRPATH_NOLOG, ctlp->errhp_ctl));
	}

	if (tblp->objconstr_tbl) /* set obj type of tbl to load if exists */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)dpctx,(ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *) tblp->objconstr_tbl,(ub4)strlen((const char *) tblp->objconstr_tbl),(ub4)OCI_ATTR_DIRPATH_OBJ_CONSTR,ctlp->errhp_ctl));

	/* set number of columns to be loaded */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)&tblp->ncol_tbl,(ub4)0, (ub4)OCI_ATTR_NUM_COLS, ctlp->errhp_ctl));

	/* get the column parameter list */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((dvoid *)dpctx,OCI_HTYPE_DIRPATH_CTX,(dvoid *)&ctlp->colLstDesc_ctl, (ub4 *)0,OCI_ATTR_LIST_COLUMNS, ctlp->errhp_ctl));

	/* get attributes of the column parameter list */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((CONST dvoid *)ctlp->colLstDesc_ctl,OCI_DTYPE_PARAM,(dvoid *)&parmtyp, (ub4 *)0,OCI_ATTR_PTYPE, ctlp->errhp_ctl));

	if (parmtyp != OCI_PTYPE_LIST)
	{
		Log(m_fErrorLog, "ERROR: expected parmtyp of OCI_PTYPE_LIST, got%d\n",(int)parmtyp);
	}

	/* Now set the attributes of each column by getting a parameter
	 * handle on each column, then setting attributes on the parameter
	 * handle for the column.
	 * Note that positions within a column list descriptor are 1-based.
	 */
	for (i = 0, pos = 1, colp = tblp->col_tbl, fldp = tblp->fld_tbl;i < tblp->ncol_tbl;i++, pos++, colp++, fldp++)
	{
		/* get parameter handle on the column */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIParamGet((CONST dvoid *)ctlp->colLstDesc_ctl,(ub4)OCI_DTYPE_PARAM, ctlp->errhp_ctl,(dvoid **)&colDesc, pos));

		colp->id_col = i; /* position in column array*/

		/* set external attributes on the column */
		/* column name */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)colp->name_col,(ub4)strlen((const char *)colp->name_col),(ub4)OCI_ATTR_NAME, ctlp->errhp_ctl));

		/* column type */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->exttyp_col, (ub4)0,(ub4)OCI_ATTR_DATA_TYPE, ctlp->errhp_ctl));

		/* max data size */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&fldp->maxlen_fld, (ub4)0,(ub4)OCI_ATTR_DATA_SIZE, ctlp->errhp_ctl));

		/* If column is chrdate or date, set column (input field) date mask
		 * to trigger client library to check string for a valid date.
		 * Note: OCIAttrSet() may be called here w/ a null ptr or null string.
		 */        

		if (colp->date_col)
		{
			ub2 col_width = (colp->datemask_col) ? strlen((const char *)colp->datemask_col) : 0;
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&col_width, (ub4)sizeof(col_width),(ub4)OCI_ATTR_DATA_SIZE, ctlp->errhp_ctl));
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)colp->datemask_col, (ub4)col_width, (ub4)OCI_ATTR_DATEFORMAT, ctlp->errhp_ctl));
		}


		if (colp->prec_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->prec_col, (ub4)0,(ub4)OCI_ATTR_PRECISION, ctlp->errhp_ctl));
		}

		if (colp->scale_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->scale_col, (ub4)0,(ub4)OCI_ATTR_SCALE, ctlp->errhp_ctl));
		}

		if (colp->csid_col)
		{
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&colp->csid_col, (ub4)0,(ub4)OCI_ATTR_CHARSET_ID, ctlp->errhp_ctl));
		}

		if (bit(colp->flag_col, COL_OID))
		{
			ub1 flg = 1;
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)&flg, (ub4)0,(ub4)OCI_ATTR_DIRPATH_OID, ctlp->errhp_ctl));
		}

		/* If this is an object, opaque or ref then call init_obj_load */
		/* to handle it. */
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
		{
			init_obj_load(ctlp, tblp, colp->obj_col);
			/* set the object function context into the param handle */
			
			OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
					OCIAttrSet((dvoid *)colDesc, (ub4)OCI_DTYPE_PARAM,(dvoid *)(colp->obj_col->ctx_obj), (ub4)0,(ub4)OCI_ATTR_DIRPATH_FN_CTX, ctlp->errhp_ctl));
		}
		/* free the parameter handle to the column descriptor */
        OCI_CHECK((dvoid *)0, 0, ociret, ctlp,
				OCIDescriptorFree((dvoid *)colDesc, OCI_DTYPE_PARAM));
    }
	
#if 0
    /* read back some of the attributes for purpose of illustration */
    for (i = 0, pos = 1, colp = tblp->col_tbl, fldp = tblp->fld_tbl;i < tblp->ncol_tbl;i++, pos++, colp++, fldp++)
    {
        text *s;
        ub4 slen;    
        ub4 maxdsz;
        ub2 dty;    
        /* get parameter handle on the column */
        OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIParamGet((CONST dvoid *)ctlp->colLstDesc_ctl,(ub4)OCI_DTYPE_PARAM, ctlp->errhp_ctl,(dvoid **)&colDesc, pos));
        
        /* get column name */
        OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrGet((dvoid *)colDesc, OCI_DTYPE_PARAM,(dvoid *)&s, (ub4 *)&slen,OCI_ATTR_NAME, ctlp->errhp_ctl));
        /* check string length */
        if (slen != (ub4)strlen((const char *)colp->name_col))
        {
            Log(output_fp,"*** ERROR *** bad col name len in column parameter\n");
            Log(output_fp, "\texpected %d, got %d\n",(int)strlen((const char *)colp->name_col), (int)slen);
        }
        if (strncmp((const char *)s, (const char *)colp->name_col, (size_t)slen))
        {
            Log(output_fp,"*** ERROR *** bad column name in column parameter\n");
            Log(output_fp, "\texpected %s, got %s\n",(char *)colp->name_col, (char *)s);
        }
        /* get column type */
        OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrGet((dvoid *)colDesc, OCI_DTYPE_PARAM,(dvoid *)&dty, (ub4 *)0,OCI_ATTR_DATA_TYPE, ctlp->errhp_ctl));
        if (dty != colp->exttyp_col)
        {
            Log(output_fp, "*** ERROR *** bad OCI_ATTR_DATA_TYPE in col param\n");
            Log(output_fp, "\tColumn name %s\n", colp->name_col);
            Log(output_fp, "\t\texpected %d, got %d\n",(int)colp->exttyp_col, (int)dty);
        }
        /* get the max data size */
        OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIAttrGet((dvoid *)colDesc, OCI_DTYPE_PARAM,(dvoid *)&maxdsz, (ub4 *)0,OCI_ATTR_DATA_SIZE, ctlp->errhp_ctl));
        if (maxdsz != fldp->maxlen_fld)
        {
            Log(output_fp, "*** ERROR *** bad OCI_ATTR_DATA_SIZE in col param\n");
            Log(output_fp, "\tColumn name %s\n", colp->name_col);
            Log(output_fp, "\t\texpected %d, got %d\n",(int)fldp->maxlen_fld, (int)maxdsz);
        }
        /* free the parameter handle to the column descriptor */
        OCI_CHECK((dvoid *)0, 0, ociret, ctlp,OCIDescriptorFree((dvoid *)colDesc, OCI_DTYPE_PARAM));	
	}
#endif
OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
		OCIAttrSet((dvoid *)dpctx, (ub4)OCI_HTYPE_DIRPATH_CTX,(dvoid *)&tblp->xfrsz_tbl,(ub4)0, (ub4)OCI_ATTR_BUF_SIZE, ctlp->errhp_ctl));

	/* prepare the load */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIDirPathPrepare(dpctx, ctlp->svchp_ctl, ctlp->errhp_ctl));

	/* Allocate column array and stream handles.
	 * Note that for the column array and stream handles
	 * the parent handle is the direct path context.
	 * Also note that Oracle errors are returned via the
	 * environment handle associated with the direct path context.
	 */
	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->dpctx_ctl, (dvoid**)&ctlp->dpca_ctl,(ub4)OCI_HTYPE_DIRPATH_COLUMN_ARRAY,(size_t)0, (dvoid **)0));

	OCI_CHECK(ctlp->envhp_ctl, OCI_HTYPE_ENV, ociret, ctlp,
			OCIHandleAlloc((dvoid *)ctlp->dpctx_ctl, (dvoid**)&ctlp->dpstr_ctl,(ub4)OCI_HTYPE_DIRPATH_STREAM,(size_t)0, (dvoid **)0));

	/* get number of rows in the column array just allocated */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((CONST dvoid *)(ctlp->dpca_ctl),OCI_HTYPE_DIRPATH_COLUMN_ARRAY,(dvoid *)(&ctlp->nrow_ctl), (ub4 *)0,OCI_ATTR_NUM_ROWS, ctlp->errhp_ctl));

	/* get number of columns in the column array just allocated */
	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIAttrGet((CONST dvoid *)(ctlp->dpca_ctl),OCI_HTYPE_DIRPATH_COLUMN_ARRAY,(dvoid *)(&ctlp->ncol_ctl), (ub4 *)0,OCI_ATTR_NUM_COLS, ctlp->errhp_ctl));

	/* allocate the column arrays for any column objects, opaques or refs */
	for (i = 0, colp = tblp->col_tbl; i < tblp->ncol_tbl; i++, colp++)
	{
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
		{
			alloc_obj_ca(ctlp, tblp, colp->obj_col);
		}
	}

	/* allocate buffer for input records */
	if (ctlp->inbuf_ctl != (ub1 *)0)
		free((void *)ctlp->inbuf_ctl);

	ctlp->inbuf_ctl = (ub1 *)malloc(ctlp->nrow_ctl * sessp->maxreclen_sess);
	if (ctlp->inbuf_ctl == (ub1 *)0)
	{
		FATAL("init_load:malloc:inbuf_ctl alloc failure", ctlp->nrow_ctl * sessp->maxreclen_sess);
	}

	/* allocate Offset-TO-Record number mapping array */
	if (ctlp->otor_ctl != (ub4 *)0)
		free((void *)ctlp->otor_ctl);

	ctlp->otor_ctl = (ub4 *)malloc(ctlp->nrow_ctl * sizeof(ub4));
	if (ctlp->otor_ctl == (ub4 *)0)
	{
		FATAL("init_load:malloc:otor_ctl alloc failure",ctlp->nrow_ctl * sizeof(ub4));
	}

	CLEAR_PCTX(ctlp->pctx_ctl); /* initialize partial context*/
	/*
	   Log(output_fp, "init_load: %ld column array rows\n",(long)ctlp->nrow_ctl);*/
	return;

}


/*
 **++++++++++++++++++++++++++++++ simple_load +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** This function reads input records from 'file', parses the input
 ** records into fields according to the field description given by
 ** tblp->fld_tbl, and loads the data into the database.
 **
 ** LOBs can be loaded with this function in a piecewise manner. This
 ** function is written as a state machine, which cycles through the
 ** following states:
 ** RESET, GET_RECORD, FIELD_SET, DO_CONVERT, DO_LOAD, END_OF_INPUT
 **
 ** The normal case of all scalar data, where multiple records fit
 ** entirely in memory, cycles through the following states:
 ** RESET, [[GET_RECORD, FIELD_SET]+, DO_CONVERT, DO_LOAD]+, RESET
 **
 ** The case of loading one or more LOB columns, which do not fit entirely
 ** in memory, has the following state transitions:
 ** RESET, GET_RECORD, [FIELD_SET, DO_CONVERT, DO_LOAD]+, RESET
 ** Note: The second and subsequent transitions to the FIELD_SET
 ** state have a partial record context.
 **
 ** A mapping of column array offset to input record number (otor_ctl[])
 ** is maintained by this function for error reporting and recovery.
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** tblp table pointer 
 ** sessp session pointer 
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

int OraDumper::simple_load(char *line, size_t size)
{
	struct tbl *tblp = &table;
 	struct sess *sessp = &session;
	int ncols = (int)tblp->ncol_tbl;

	while (!done)
	{
		switch (state)
		{
			case RESET: /* Reset column array and direct stream state to be empty*/
				{
					startoff 	= 0; /* reset starting offset into column array*/
					lastoff 	= 0; /* last entry set of column array*/
					rowCnt 		= 0; /* count of rows partial and complete*/
					cvtCnt 		= 0; /* count of converted rows*/
					nxtLoadOff 	= 0;
					/* Reset column array state in case a previous conversion needed
					 * to be continued, or a row is expecting more data.
					 */

					(void) OCIDirPathColArrayReset(ctlp->dpca_ctl, ctlp->errhp_ctl);
					(void) OCIDirPathColArrayReset(ctlp->dpobjca_ctl, ctlp->errhp_ctl);
					(void) OCIDirPathColArrayReset(ctlp->dpnestedobjca_ctl,ctlp->errhp_ctl);
					/* Reset the stream state since we are starting a new stream
					 * (i.e. don't want to append to existing data in the stream.)
					 */
					(void) OCIDirPathStreamReset(ctlp->dpstr_ctl, ctlp->errhp_ctl);
					state = GET_RECORD; /* get some more input records*/

					if (bFirstCall == false)
						return 0;
					else
						bFirstCall = false;
					/* FALLTHROUGH */
				}
			case GET_RECORD:
				{
					assert(lastoff < ctlp->nrow_ctl); /* array bounds check*/
					recp = (text *)(ctlp->inbuf_ctl + (lastoff * sessp->maxreclen_sess));

					if (size == 0) 
					{
						if (lastoff != 0)
						{
							lastoff--;
						}

						state = END_OF_INPUT;
						break;
					}
					//Get line 
					memcpy( (char *)recp, line, size); //optimize
					
					// oracle columns can more then hdfs, so pad n nulls
					int i = 0;
					for (; i < ncols; ++i){
						recp[i + size] = m_chSep;
					}
					size = size + ncols;
					recp[size] = '\0';
	 				
					// compat for sequence, ignore the first tab char
					if( strlen((char *)recp ) > 0 && recp[0] == '\t' )
						recp += 1;

					m_nReadRows++;
					/* set column array offset to input record number map */
					ctlp->otor_ctl[lastoff] = ++input_recnum;
					if ((input_recnum % 500000) == 0)
					{
						sword ociret; 
						//Log(output_fp, "record number: %d\n", (int)input_recnum); 
						OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIDirPathDataSave(ctlp->dpctx_ctl, ctlp->errhp_ctl,(ub4)OCI_DIRPATH_DATASAVE_SAVEONLY));
					}

					state = FIELD_SET;
					/* FALLTHROUGH */
				}
			case FIELD_SET:
				{
					/* map input data fields to DB columns, set column array entries */
					fsetrv = field_set(ctlp, tblp, (struct obj *) 0, recp, lastoff, 0);
					rowCnt = lastoff + 1;
					if (rowCnt == ctlp->nrow_ctl || fsetrv != FIELD_SET_COMPLETE)
					{
							/* array is full, or have a large partial column, or the
							 * secondary buffer is in use by an OUTOFLINE field.
							 */
						state = DO_CONVERT;
						/* FALLTHROUGH */
					}
					else
					{
						lastoff++; /* use next column array slot*/
						state = GET_RECORD; /* get next record*/
						return 0;
					}
				}
			case DO_CONVERT:
				{
					/* Either one of the following is true:
					 * - the column array is full
					 * - there is a large partial column
					 * - the secondary buffer used by field_set() is in use
					 * - previous conversion returned CONVERT_CONTINUE and
					 * now the conversion is being resumed.
					 *
					 * In any case, convert and load the data.
					 */
					ub4 cvtBadRoff; /* bad row offset from conversion*/
					ub2 cvtBadCoff; /* bad column offset from conversion*/
					while (startoff <= lastoff)
					{
						ub4 cvtCntPerCall = 0; /* rows converted in one call to do_convert*/
						/* note that each call to do_convert() will convert all contiguousrows
						 * in the colarray until it hit a row in error while converting.
						 */
						    cvtrv = do_convert(ctlp, startoff, rowCnt, &cvtCntPerCall,&cvtBadCoff);
							cvtCnt += cvtCntPerCall; /* sum of rows converted so far in colarray*/
							if (cvtrv == CONVERT_SUCCESS)
							{
								/* One or more rows converted successfully, break
								 * out of the conversion loop and load the rows.
								 */
								assert(cvtCntPerCall > 0);
								state = DO_LOAD;
								break;
							}
							else if (cvtrv == CONVERT_ERROR)
							{
								/* Conversion error. Reject the bad record and
								 * continue on with the next record (if any).
								 * cvtBadRoff is the 0-based index of the bad row in
								 * the column array. cvtBadCoff is the 0-based index
								 * of the bad column (of the bad row) in the column
								 * array.
								 */
								assert(cvtCntPerCall >= 0);
								cvtBadRoff = startoff + cvtCntPerCall;
								err_recnum = ctlp->otor_ctl[cvtBadRoff]; /* map to input_recnum*/
								m_nRejRows++;
									
								//char szErrLine[8192] = {0};
								//strncpy(szErrLine, (char *)(ctlp->inbuf_ctl + (cvtBadRoff * sessp->maxreclen_sess)), sizeof(szErrLine));
								//if (m_nBadLines < 100)
								//{
								//	Log(m_fBadLineLog, "[%d,%d]: %s\n", (int)err_recnum, (int)cvtBadCoff + 1, szErrLine);
								//	m_nBadLines++;
								//}
								
								sb4 nErrCode = 0;
								(void) OCIErrorGet((dvoid *)ctlp->errhp_ctl, (ub4) 1, (text *) NULL, &nErrCode, NULL, 0, OCI_HTYPE_ERROR);
								// optimze error message
								if( m_oLoadErr.nLastErrCode == nErrCode )
								{
                                    /*
									if( dotCnt < 320 )
										Log(output_fp, ".");
									else if( dotCnt == 320 )
										Log(output_fp, "more dots hidden");
                                    */
									dotCnt++;
									m_oLoadErr.mErrInfo[nErrCode].nErrCount++;
								}
								else
								{
									if( m_oLoadErr.nLastErrCode != 0 )
									{
										//Log(m_fErrorLog, "Error[%d]: %d:%s\n", 
										//		m_oLoadErr.nLastErrCode, 
										//		m_oLoadErr.mErrInfo[m_oLoadErr.nLastErrCode].nErrCount,
										//		m_oLoadErr.mErrInfo[m_oLoadErr.nLastErrCode].strErrMsg.c_str());
									}
									Log(m_fErrorLog, "Conversion Error on record %d, column %d\n",(int)err_recnum, (int)cvtBadCoff + 1);
									/* print err msg txt */
									errprint((dvoid *)(ctlp->errhp_ctl), OCI_HTYPE_ERROR,(sb4 *)0);

									/* print err file context */
									char szErrLine[8192] = {0};
                            		strncpy( szErrLine, (char *)(ctlp->inbuf_ctl + (cvtBadRoff * sessp->maxreclen_sess)), sizeof(szErrLine));
                            		Log(output_fp, "BadLine: %s\n", szErrLine );
									m_oLoadErr.nLastErrCode = nErrCode;
									if( m_oLoadErr.mErrInfo.count(nErrCode) == 0 ) /* a new error code */
										m_oLoadErr.mErrInfo[nErrCode].nErrCount = 1;
								}
								/* Check to see if the conversion error occurred on a
								 * continuation of a partially loaded row.
								 * If so, either (a) flush the partial row from the server, or
								 * (b) mark the column as being 0 length and complete.
								 * In the latter case (b), any data already loaded into the column
								 * from a previous LoadStream call remains, and we can continue
								 * field setting, conversion and loading with the next column.
								 * Here, we implement (a), and flush the row from the server.
								 */
								if (err_recnum == load_recnum)
								{
									/* Conversion error occurred on record which has been
									 * partially loaded (by a previous stream).
									 * XXX May be better to have an attribute of the direct path
									 * XXX context which indicates that the last row loaded was
									 * XXX partial.
									 *
									 * Flush the output pipe. Note that on conversion error,
									 * no part of the row data for the row in error makes it
									 * into the stream buffer.
									 * Here we flush the partial row from the server. The
									 * stream state is reset if no rows are successfully
									 * converted.
									 */
									/* flush partial row from server */
									(void) OCIDirPathFlushRow(ctlp->dpctx_ctl, ctlp->errhp_ctl);
								}
								if (cvtBadRoff == lastoff)
								{
									/* Conversion error occurred on the last populated slot
									 * of the column array.
									 * Flush the input stream of any data for this row,
									 * and re-use this slot for another input record.
									 */
									field_flush(ctlp, lastoff);
									state = GET_RECORD;
									startoff = cvtBadRoff; /* only convert the last row*/
									rowCnt = 0; /* already tried converting all rows in col array*/
									assert(startoff <= lastoff);
									return 0;
								}
								else
								{
									/* Skip over bad row and continue conversion with next row.
									 * We don't attempt to fill in this slot with another record.
									 */
									startoff = cvtBadRoff + 1;
									assert(startoff <= lastoff);
									continue;
								}
							}
							else if (cvtrv == CONVERT_NEED_DATA) /* partial col encountered*/
							{
								/* Partial (large) column encountered, load the piece
								 * and loop back up to field_set to get the rest of
								 * the partial column.
								 * startoff is set to the offset into the column array where
								 * we need to resume conversion from, which should be the
								 * last entry that we set (lastoff).
								 */
								state = DO_LOAD;
								/* Set our row position in column array to resume
								 * conversion at when DO_LOAD transitions to DO_CONVERT.
								 */
								assert(cvtCntPerCall >= 0);
								startoff = startoff + cvtCntPerCall;
								/* assert(startoff == lastoff); */
								break;
							}
							else if (cvtrv == CONVERT_CONTINUE)
							{
								/* The stream buffer is full and there is more data in
								 * the column array which needs to be converted.
								 * Load the stream (DO_LOAD) and transition back to
								 * DO_CONVERT to convert the remainder of the column array,
								 * without calling the field setting function in between.
								 * The sequence {DO_CONVERT, DO_LOAD} may occur many times
								 * for a long row or column.
								 * Note that startoff becomes the offset into the column array
								 * where we need to resume conversion from.
								 */
								cvtcontcnt++;
								state = DO_LOAD;

								/* Set our row position in column array (startoff) to
								 * resume conversion at when we transition from the
								 * DO_LOAD state back to DO_CONVERT.
								 */
								assert(cvtCntPerCall >= 0);
								startoff = startoff + cvtCntPerCall;
								assert(startoff <= lastoff);
								break;
							}
						} /* end while*/
						break;
					}
				case DO_LOAD:
					{
						ub4 loadCnt; /* count of rows loaded by do_load*/
						ldrv = do_load(ctlp, &loadCnt);
						nxtLoadOff = nxtLoadOff + loadCnt;
						switch (ldrv)
						{
							case LOAD_SUCCESS:
								{
									/* The stream has been loaded successfully. What we do next
									 * depends on the result of the previous conversion step.
									 */
									load_recnum = ctlp->otor_ctl[nxtLoadOff - 1];
									if (cvtrv == CONVERT_SUCCESS || cvtrv == CONVERT_ERROR)
									{
										/* The column array was successfully converted (or the
										 * last row was in error).
										 * Fill up another array with more input records.
										 */
										state = RESET;
									}
									else if (cvtrv == CONVERT_CONTINUE)
									{
										/* There is more data in column array to convert and load. */
										state = DO_CONVERT;
										/* Note that when do_convert returns CONVERT_CONTINUE that
										 * startoff was set to the row offset into the column array
										 * of where to resume conversion. The loadCnt returned by
										 * OCIDirPathLoadStream is the number of rows successfully
										 * loaded.
										 * Do a sanity check on the attributes here.
										 */
										if (startoff != nxtLoadOff) /* sanity*/
											Log(m_fErrorLog, "LOAD_SUCCESS/CONVERT_CONTINUE: %ld:%d\n",(long)nxtLoadOff, startoff);
										/* Reset the direct stream state so conversion starts at
										 * the beginning of the stream.
										 */
										(void) OCIDirPathStreamReset(ctlp->dpstr_ctl, ctlp->errhp_ctl);
									}
									else
									{
										/* Note that if the previous conversion step returned
										 * CONVERT_NEED_DATA then the load step would have returned
										 * LOAD_NEED_DATA too (not LOAD_SUCCESS).
										 */
										FATAL("DO_LOAD:LOAD_SUCCESS: unexpected cvtrv", cvtrv);
									}
									break;
								}
							case LOAD_ERROR:
								{
									sb4 oraerr;
									ub4 badRowOff;

									badRowOff = nxtLoadOff;
									nxtLoadOff += 1; /* account for bad row*/
									err_recnum = ctlp->otor_ctl[badRowOff]; /* map to input_recnum*/
									m_nRejRows++;
									Log(m_fErrorLog, "Error on record %ld\n", (long)err_recnum);
									/* print err msg txt */
									errprint((dvoid *)(ctlp->errhp_ctl), OCI_HTYPE_ERROR, &oraerr);
									/* On a load error, all rows up to the row in error are loaded.
									 * account for that here by setting load_recnum only when some
									 * rows have been loaded.
									 */
									if (loadCnt != 0)
										load_recnum = err_recnum - 1;

									if (oraerr == OER(600))
										FATAL("DO_LOAD:LOAD_ERROR: server internal error", oraerr);
									if (err_recnum == input_recnum)
									{
										/* Error occurred on last input row, which may or may not
										 * be in a partial state. Flush any remaining input for
										 * the bad row.
										 */
										field_flush(ctlp, badRowOff);
									}
									if (err_recnum == load_recnum)
									{
										/* Server has part of this row already, flush it */
										(void) OCIDirPathFlushRow(ctlp->dpctx_ctl, ctlp->errhp_ctl);
									}
									if (badRowOff == lastoff)
									{
										/* Error occurred on the last entry in the column array,
										 * go process more input records and set up another array.
										 */
										state = RESET;
									}
									else
									{
										/* Otherwise, continue loading this stream. Note that the
										 * stream positions itself to the next row on error.
										 */
										state = DO_LOAD;
									}
									break;
								}
							case LOAD_NEED_DATA:
								{
									load_recnum = ctlp->otor_ctl[nxtLoadOff];
									if (cvtrv == CONVERT_NEED_DATA)
										state = FIELD_SET; /* need more input data*/
									else if (cvtrv == CONVERT_CONTINUE)
										state = DO_CONVERT; /* have input data, continue with conversion*/
									else
										FATAL("DO_LOAD:LOAD_NEED_DATA: unexpected cvtrv", cvtrv);
									/* Reset the direct stream state so conversion starts at
									 * the beginning of the stream.
									 */
									(void) OCIDirPathStreamReset(ctlp->dpstr_ctl, ctlp->errhp_ctl);
									break;
								}
							case LOAD_NO_DATA:
								{
									/* Attempt to either load an empty stream, or a stream
									 * which has been completely processed.
									 */
									if (cvtrv == CONVERT_CONTINUE)
									{
										/* Reset stream state so we convert into an empty stream buffer.*/
										(void) OCIDirPathStreamReset(ctlp->dpstr_ctl, ctlp->errhp_ctl);
										state = DO_CONVERT; /* convert remainder of column array*/
									}
									else
										state = RESET; /* get some more input records*/
									break;
								}
							default:
								FATAL("DO_LOAD: unexpected return value", ldrv);
								break;
						}
						break;
					}
				case END_OF_INPUT:
					{
						if (cvtCnt)
							state = DO_LOAD; /* deal with data already converted, but not loaded*/
						else if (rowCnt)
							state = DO_CONVERT; /* deal with a partially populated column array*/
						else
							done = TRUE;
						break;
					}
				default:
					FATAL("SIMPLE_LOAD: unexpected state", state);
					break;
			} /* end switch (state)*/
		}
	/*
	   Log(output_fp, "do_convert returned CONVERT_CONTINUE %ld times\n",
	   (long)cvtcontcnt);*/
	if( m_oLoadErr.nLastErrCode != 0 )
	{
		//Log(m_fErrorLog, "Error[%d]: %d:%s\n", 
		//		m_oLoadErr.nLastErrCode, 
		//		m_oLoadErr.mErrInfo[m_oLoadErr.nLastErrCode].nErrCount,
		//		m_oLoadErr.mErrInfo[m_oLoadErr.nLastErrCode].strErrMsg.c_str());
	}
	m_nLoadRows = m_nReadRows - m_nRejRows;  /* calc loaded rows */
	
	m_nReadRows += m_notconv; //Total read
	m_nRejRows += m_notconv;  // Total bad lines
	
	//Log(output_fp, "\n----------------------------------------------------------\n");
	//Log(output_fp, "Number of input records processed = %ld\n", (long)m_nReadRows);
	//Log(output_fp, "Number of load records processed  = %ld\n\n", (long)m_nLoadRows);
	if (m_notconv != 0)
		Log(output_fp, "Number of not conv encode = %ld\n\n", m_notconv);

	return 1;
}


/*
 **++++++++++++++++++++++++++++++ finish_load +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Completes the loading procedure.
 **
 ** Assumptions:
 **
 ** Does not free server data structures related to the load.
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::finish_load(bool bSave /* = 1 */)
{
	sword ociret; /* return code from OCI call */
	/* Execute load finishing logic without freeing server data structures
	 * related to the load.
	 */
	if( bSave )
	{
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIDirPathDataSave(ctlp->dpctx_ctl, ctlp->errhp_ctl,(ub4)OCI_DIRPATH_DATASAVE_FINISH));
		/* free up server data structures for the load. */
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
				OCIDirPathFinish(ctlp->dpctx_ctl, ctlp->errhp_ctl));
	} else {
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIDirPathAbort(ctlp->dpctx_ctl, ctlp->errhp_ctl));
	}

	OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
			OCIHandleFree(ctlp->dpctx_ctl, (ub4)OCI_HTYPE_DIRPATH_CTX) );

	//OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
	//		OCIHandleFree(ctlp->dpca_ctl,(ub4)OCI_HTYPE_DIRPATH_COLUMN_ARRAY) );
	
	//OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,
	//		OCIHandleFree(ctlp->dpstr_ctl,(ub4)OCI_HTYPE_DIRPATH_STREAM) );
}


/*
 **++++++++++++++++++++++++++++++ do_convert +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Convert the data in the column array to stream format.
 **
 ** Assumptions:
 **
 **
 ** Parameters:
 ** 
 ** ctlp pointer to control structure (IN/OUT) 
 ** rowcnt number of rows in column array (IN) 
 ** startoff starting row offset into column array (IN) 
 ** cvtCntp count of rows successfully converted (OUT) 
 ** badcoffp column offset into col array of bad col (OUT) 
 **
 ** Returns:
 **
 ** CONVERT_SUCCESS:
 ** All data in the column array has been successfully converted.
 ** *cvtCntp is the number of rows successfully converted.
 ** CONVERT_ERROR:
 ** Conversion error occurred on the row after the last successfully
 ** converted row.
 ** Client Action:
 ** Continue converting the column array by calling this function
 ** again with startoff adjusted to skip over the row in error.
 ** CONVERT_NEED_DATA:
 ** All data in the column array has been converted, but the last
 ** column processed was marked as a partial.
 ** CONVERT_CONTINUE:
 ** Not all of the data in the column array has been converted due to
 ** lack of space in the stream buffer.
 ** Client Action:
 ** Load the converted stream data, reset the stream, and call this
 ** function again without modifying the column array and setting
 ** startoff to the appropriate position in the array.
 **
 **-------------------------------------------------------------------------
 */

sword OraDumper::do_convert(struct loadctl *ctlp, ub4 startoff, ub4 rowcnt, ub4 *cvtCntp, ub2 *badcoffp)
	/* pointer to control structure (IN/OUT) *//* number of rows in column array (IN) *//* starting row offset into column array (IN) *//* count of rows successfully converted (OUT) *//* column offset into col array of bad col (OUT) */
{
	sword retval = CONVERT_SUCCESS;
	sword ocierr, ocierr2;
	ub2 badcol = 0;
	*cvtCntp = 0;

	if (startoff >= rowcnt)
		FATAL("DO_CONVERT: bad startoff", startoff);

	if (rowcnt)
	{
		/* convert array to stream, filter out bad records */
		ocierr = OCIDirPathColArrayToStream(ctlp->dpca_ctl, ctlp->dpctx_ctl,ctlp->dpstr_ctl, ctlp->errhp_ctl,rowcnt, startoff);
		switch (ocierr)
		{
			case OCI_SUCCESS: /* everything succesfully converted to stream */
				retval = CONVERT_SUCCESS;
				break;
			case OCI_ERROR: 
				/* some error, most likely a conversion error */
				/* Tell the caller that a conversion error occurred along
				 * with the number of rows successfully converted (*cvtCntp).
				 * Note that the caller is responsible for adjusting startoff
				 * accordingly and calling us again to resume conversion of
				 * the remaining rows.
				 */
				retval = CONVERT_ERROR; /* conversion error */
				break;
			case OCI_CONTINUE: /* stream buffer is full */
				/* The stream buffer could not contain all of the data in
				 * the column array.
				 * The client should load the converted data, and loop
				 * back to convert the remaining data in the column array.
				 */
				retval = CONVERT_CONTINUE;
				break;
			case OCI_NEED_DATA: /* partial column encountered */
				/* Everything converted, but have a partial column.
				 * Load this stream, and return to caller for next piece.
				 */
				retval = CONVERT_NEED_DATA;
				break;
			default: /* unexpected OCI return value! */
				FATAL("do_convert:OCIDirPathColArrayToStream:Unexpected OCI return code",ocierr);
				/* NOTREACHED */
				break;
		}

		OCI_CHECK(ctlp->errhp2_ctl, OCI_HTYPE_ERROR, ocierr2, ctlp,OCIAttrGet((CONST dvoid *)ctlp->dpca_ctl, OCI_HTYPE_DIRPATH_COLUMN_ARRAY,(dvoid *)(cvtCntp), (ub4 *)0, OCI_ATTR_ROW_COUNT, ctlp->errhp2_ctl));
		OCI_CHECK(ctlp->errhp2_ctl, OCI_HTYPE_ERROR, ocierr2, ctlp,OCIAttrGet((CONST dvoid *)ctlp->dpca_ctl, OCI_HTYPE_DIRPATH_COLUMN_ARRAY,(dvoid *)(&badcol), (ub4 *)0, OCI_ATTR_COL_COUNT, ctlp->errhp2_ctl));
	}

	*badcoffp = badcol;

	return retval;
}


/*
 **++++++++++++++++++++++++++++++ do_load +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Load a direct path stream.
 **
 ** Assumptions:
 **
 ** errhp_ctl contains error information when an error is returned
 ** from this function.
 **
 ** Parameters:
 **
 ** ctlp: Pointer to control structure. 
 ** loadCntp: Count of rows loaded on this call.
 **
 ** Returns:
 **
 ** LOAD_SUCCESS:
 ** All data loaded succesfully.
 ** Client Action:
 ** Supply another stream and call again, or finish the load.
 **
 ** LOAD_ERROR:
 ** Error while loading occured. *loadCntp is the number of
 ** rows successfully loaded this call.
 ** Client Action:
 ** Use *loadCntp to compute current column array position and
 ** map the column array position to the input record and reject
 ** the record.
 **
 ** if (this is a continuation of a row)
 ** {
 ** /o server has data for this row buffered o/
 ** flush the row data
 ** }
 **
 ** if (end-of-stream has not been reached)
 ** {
 ** call this function again,
 ** stream loading will resume with the next row in the stream.
 ** }
 ** else if (end-of-stream has been reached)
 ** {
 ** build another stream and call this function again,
 ** or finish the load.
 ** }
 **
 ** LOAD_NEED_DATA:
 ** Last row was not complete.
 ** Client Action:
 ** Caller needs to supply more data for the row (a column is
 ** being pieced.) Note that the row offset can be determined
 ** by either the cvtCnt returned from do_convert, or from the
 ** loadCntp returned by do_load. The column offset for the
 ** column being pieced is available as an attribute of
 ** the column array.
 **
 **-------------------------------------------------------------------------
 */

sword OraDumper::do_load(struct loadctl *ctlp, ub4 *loadCntp)
	/* pointer to control structure (IN/OUT) *//* number of rows loaded (OUT) */
{
	sword ocierr; /* OCI return value */
	sword retval; /* return value from this function */
	sword getRowCnt = FALSE; /* return row count if TRUE */

	if (loadCntp != (ub4 *)0)
	{
		*loadCntp = 0;
		getRowCnt = TRUE;
	}
	/* Load the stream.
	 * Note that the position in the stream is maintained internally to
	 * the stream handle, along with offset information for the column
	 * array which produced the stream. When the conversion to stream
	 * format is done, the data is appended to the stream. It is the
	 * responsibility of the caller to reset the stream when appropriate.
	 * On errors, the position is moved to the next row, or the end of
	 * the stream if the error occurs on the last row. The next LoadStream
	 * call will start on the next row, if any.
	 * If a LoadStream call is made, and end of stream has been reached,
	 * OCI_NO_DATA is returned.
	 */
#if 1
	ocierr = OCIDirPathLoadStream(ctlp->dpctx_ctl, ctlp->dpstr_ctl,ctlp->errhp_ctl);
#else
	{
		ub1 *bufaddr;
		ub4 buflen;

		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ocierr, ctlp,OCIAttrGet((CONST dvoid *)(ctlp->dpstr_ctl), OCI_HTYPE_DIRPATH_STREAM,(dvoid *)&bufaddr, (ub4 *)0, OCI_ATTR_BUF_ADDR, ctlp->errhp_ctl));
		OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ocierr, ctlp,OCIAttrGet((CONST dvoid *)(ctlp->dpstr_ctl), OCI_HTYPE_DIRPATH_STREAM,(dvoid *)&buflen, (ub4 *)0, OCI_ATTR_BUF_SIZE, ctlp->errhp_ctl));

		write(1, (char *)bufaddr, (int)buflen);
		Log(output_fp, "Wrote %d bytes from stream\n", (int)buflen);
		getRowCnt = FALSE;
	}
#endif

	switch (ocierr)
	{
		case OCI_SUCCESS:
			/* all data succcesfully loaded */
			retval = LOAD_SUCCESS;
			break;

		case OCI_ERROR:
			/* Error occurred while loading: could be a partition mapping
			 * error, null constraint violation, or an out of space
			 * condition. In any case, we return the number of rows
			 * processed (successfully loaded).
			 */
			retval = LOAD_ERROR;
			break;
		case OCI_NEED_DATA:
			/* Last row was not complete.
			 * The caller needs to supply another piece.
			 */
			retval = LOAD_NEED_DATA;
			break;
		case OCI_NO_DATA:
			/* the stream was empty */
			retval = LOAD_NO_DATA;
			break;

		default:
			FATAL("do_load:OCIDirPathLoadStream:Unexpected OCI return code", ocierr);
			/* NOTREACHED */
			break;
	}

	if (getRowCnt)
	{
		sword ocierr2;
		OCI_CHECK(ctlp->errhp2_ctl, OCI_HTYPE_ERROR, ocierr2, ctlp,OCIAttrGet((CONST dvoid *)(ctlp->dpstr_ctl), OCI_HTYPE_DIRPATH_STREAM,(dvoid *)loadCntp, (ub4 *)0, OCI_ATTR_ROW_COUNT, ctlp->errhp2_ctl));
	}

	return retval;
}


/*
 **++++++++++++++++++++++++++++++ field_flush +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Helper function which cleans up the partial context state, and clears it.
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** rowoff column array row offset 
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::field_flush(struct loadctl *ctlp, ub4 rowoff)
	/* load control structure pointer */ /* column array row offset */ 
{
	if (ctlp->pctx_ctl.valid_pctx)
	{
		/* Partial context is valid; make sure the request is
		 * for the context corresponding to the current row.
		 */
		assert(rowoff == ctlp->pctx_ctl.row_pctx);
		(void) close(ctlp->pctx_ctl.fd_pctx);
		free((void *)ctlp->pctx_ctl.fnm_pctx);
	}
	CLEAR_PCTX(ctlp->pctx_ctl);
}


/*
 **++++++++++++++++++++++++++++++ field_set +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Sets the input data fields to their corresponding data columns.
 **
 ** Simple field setting.
 ** Computes address and length of fields in the input record,
 ** and sets the corresponding column array entry for each input field.
 **
 ** This function only deals with positional fields.
 **
 ** Leading white space is trimmed from the field if FLD_STRIP_LEAD_BLANK
 ** is set.
 **
 ** Trailing white space is trimmed from the field if FLD_STRIP_TRAIL_BLANK
 ** is set.
 **
 ** Fields which consist of all spaces are loaded as null columns.
 **
 ** Fields which are marked as FLD_OUTOFLINE are interpreted
 ** as being file names which can be passed directly to an
 ** open system call.
 **
 ** NOTES: Discuss how partials are handled.
 ** 
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure 
 ** tblp table descriptor 
 ** recp input record 
 ** rowoff column array row offset 
 ** bufflg buffer in use flag 
 **
 ** Returns:
 **
 ** FIELD_SET_COMPLETE:
 ** All fields are complete, the partial context is not valid.
 **
 ** FIELD_SET_BUF
 ** All fields are complete, the partial context is not valid, but
 ** data is buffered in a secondary buffer and the column array has
 ** one or more pointers into the secondary buffer. The caller
 ** must convert the column array to stream format before calling
 ** this function again.
 **
 ** FIELD_SET_PARTIAL:
 ** A field is in the partial state, the partial context is valid
 ** and is required to continue processing the field. Note that
 ** when a field is partial, the row which contains the column
 ** corresponding to the field is partial also.
 **
 ** FIELD_SET_ERROR:
 ** A read error occured on a secondary (out-of-line) data file.
 **
 **-------------------------------------------------------------------------
 */

/* Deal with WIN32 CR-LF weirdness */
#if defined(WIN32COMMON) || defined(WIN32) || defined(_WIN32)
#define TKPIDRV_OPEN_MODE (O_RDONLY | O_BINARY)
#else
#define TKPIDRV_OPEN_MODE (O_RDONLY)
#endif

sword OraDumper::field_set(struct loadctl *ctlp, struct tbl *tblp, struct obj *objp, text *recp, ub4 rowoff, ub1 bufflg)
{
	ub1 *cval;
	ub4 ncols;
	ub4 thiscol;
	ub4 clen, j; /* column length */
	ub1 cflg;
	sword ociret;
	///sword done = FALSE;
	///sword f = FALSE;
	int fd; /* file descriptor for out-of-line data */
	char *filename; /* filename for out-of-line data */
	sword partial;
	///static int count = 0;
	struct col *cols;
	struct fld *flds;
	OCIDirPathColArray * ca;
	ub4 recsz = 0; /* Current size of record read in */
	ub1 prntflg = 0; /* Print warning message flag */
	///ub4 i = 0;
	recsz = strlen ((const char *)recp); 
	/* strlen won't work for binary numbers in record. */
	/*while (!done)
	  {
	  for (i = 0; *recp != '\n' ; i++)
	  {
	  recsz = recsz + 1;
	  }
	  done = TRUE;
	  }*/
	/* Reset the buffer offset if not recursing */
	if (!bufflg)
		ctlp->bufoff_ctl = 0;

	if ((partial = (sword)ctlp->pctx_ctl.valid_pctx) == TRUE)
	{
		/* partial context is valid; resume where we left off */
		assert(rowoff == ctlp->pctx_ctl.row_pctx);
		thiscol = ctlp->pctx_ctl.col_pctx;
	}
	else
		thiscol = 0;

	if (objp != 0)
	{
		cols = objp->col_obj;
		flds = objp->fld_obj;
		ncols = objp->ncol_obj;
		ca = objp->ca_obj;
	}
	else
	{
		cols = tblp->col_tbl;
		flds = tblp->fld_tbl;
		ncols = tblp->ncol_tbl;
		ca = ctlp->dpca_ctl;
	}

	ub4 nacts = 0;
	GetPos((char*)recp, flds, ncols, nacts);
	if( nacts < ncols )
	{
		// cout << "Column is not enough!" << endl;
	}

	for (/* empty */; thiscol < ncols; thiscol++)
	{
		struct col *colp = &cols[thiscol];
		struct fld *fldp = &flds[thiscol];
		fldp->flag_fld = FLD_INLINE;///test

		if (partial)
		{
			/* partials are always from a secondary file */
			fd = ctlp->pctx_ctl.fd_pctx;
			filename = ctlp->pctx_ctl.fnm_pctx;
		}
		else /* !partial*/
		{
			fd = -1;
			filename = (char *)0;
			cval = (ub1 *)recp + fldp->begpos_fld - 1;
			/*
			 ** Check the field length is not longer than the current record length.
			 ** If it is, issue a warning and set the clen to the record length -
			 ** the beginning field position.
			 */
			if (fldp->endpos_fld > recsz )
			{
				if (!prntflg)
				{
					Log(m_fErrorLog, "Warning: Max field length, %d for record %d, is greater than the current record size %d.\n",fldp->endpos_fld, (rowoff+1), recsz);
					//printf("%s\n", recp);
					//sleep(2);
					prntflg = 1;
				}
				clen = recsz - fldp->begpos_fld + 1;
			} 
			else 
				clen = fldp->endpos_fld - fldp->begpos_fld + 1;

			j = 0;
			if (bit(fldp->flag_fld, FLD_STRIP_LEAD_BLANK))
			{
				/* trim leading white space */
				for (/*empty*/; j < clen; j++)
					if (!isspace((int)cval[j]))
						break;
			}
			if (j >= clen)
				clen = 0; /* null column, handled below*/
			else
			{
				if (bit(fldp->flag_fld, FLD_STRIP_TRAIL_BLANK))
				{
					/* trim trailing white space or new line char within field length. */
					while ((clen && isspace((int)cval[clen - 1]))||(clen && ((int)cval[clen - 1]== '\n')))
						clen--;
				}
				cval = cval + j;
				clen = clen - j;
			}
			if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
				goto obj;

			if (clen)
			{
				if (bit(fldp->flag_fld, FLD_INLINE))
				{
					cflg = OCI_DIRPATH_COL_COMPLETE;
				}
				else if (bit(fldp->flag_fld, FLD_OUTOFLINE))
				{
					filename = (char *)malloc((size_t)clen+1);
					if (!filename)
					{
						FATAL("field_set: cannot malloc buf for filename", (clen + 1));
					}
					(void) memcpy((dvoid *)filename, (dvoid *)cval, (size_t)clen);
					filename[clen] = 0;
					fd = open(filename, TKPIDRV_OPEN_MODE);
					SET_PCTX(ctlp->pctx_ctl, rowoff, thiscol, (ub4)0, fd, filename);
					LEN_PCTX(ctlp->pctx_ctl) = 0;
				}
				else
				{
					FATAL("field_set: unknown field type", fldp->flag_fld);
				}
			}
			else
			{
				cflg = OCI_DIRPATH_COL_NULL; /* all spaces become null*/
				cval = (ub1 *)0;
			}
		}


		if (bit(fldp->flag_fld, FLD_OUTOFLINE))
		{
			char *buf;
			ub4 bufsz;
			int cnt;

			if (!ctlp->buf_ctl)
			{
				ctlp->buf_ctl = (ub1 *)malloc((size_t)SECONDARY_BUF_SIZE);
				ctlp->bufsz_ctl = SECONDARY_BUF_SIZE;
			}
			if ((ctlp->bufsz_ctl - ctlp->bufoff_ctl) > SECONDARY_BUF_SLOP)
			{
				buf = (char *)ctlp->buf_ctl + ctlp->bufoff_ctl; /* buffer pointer*/
				bufsz = (int)ctlp->bufsz_ctl - ctlp->bufoff_ctl; /* buffer size*/
				if (fd == -1)
					cnt = 0;
				else
					cnt = read(fd, buf, bufsz);

				if (cnt != -1)
				{
					cval = (ub1 *)buf;
					clen = (ub4)cnt;

					if (cnt < bufsz) /* all file data has been read*/
					{
						/* mark column as null or complete */
						if (cnt == 0 && LEN_PCTX(ctlp->pctx_ctl) == 0)
							cflg = OCI_DIRPATH_COL_NULL;
						else
							cflg = OCI_DIRPATH_COL_COMPLETE;

						field_flush(ctlp, rowoff); /* close file, free filename*/
						/* adjust offset into buffer for use by next field */
						ctlp->bufoff_ctl += cnt;
					}
					else
						cflg = OCI_DIRPATH_COL_PARTIAL;
				}
				else
				{
					/* XXX: do something on read failure, like return an error context*/
					field_flush(ctlp, rowoff); /* close file, free filename*/
					return FIELD_SET_ERROR;
				}
			}
			else
			{
				/* no room in secondary buffer, return a 0 length partial
				 * and pick it up next time.
				 */
				cflg = OCI_DIRPATH_COL_PARTIAL;
				clen = 0;
				cval = (ub1 *)NULL;
			}
		}
OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIDirPathColArrayEntrySet(ca, ctlp->errhp_ctl, rowoff, colp->id_col,cval, clen, cflg));
		if (cflg == OCI_DIRPATH_COL_PARTIAL)
		{
			/* Partials only occur for OutOfLine data
			 * remember the row offset, column offset,
			 * total length of the column so far,
			 * and file descriptor to get data from on
			 * subsequent calls to this function.
			 */
			LEN_PCTX(ctlp->pctx_ctl) += clen;
			return FIELD_SET_PARTIAL;
		}

obj:
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
		{
			/* check for NULL for the whole object. If clen is not empty, then the
			 * object is not null.
			 */
			objp = colp->obj_col;
			if (clen)
			{
				field_set(ctlp, tblp, colp->obj_col, recp, objp->rowoff_obj, 1);
				objp->rowoff_obj++;
				/* Set the entry in the parent column array to be the column array
				 * for the object/
				 */
				cflg = OCI_DIRPATH_COL_COMPLETE;
				clen = sizeof(objp->ca_obj);
				cval = (ub1 *) objp->ca_obj;
				OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIDirPathColArrayEntrySet(ca, ctlp->errhp_ctl,rowoff, colp->id_col,cval, clen, cflg));
			}
			else
			{
				/* set the entry in the column array to be NULL flag */
				cflg = OCI_DIRPATH_COL_NULL;
				clen = 0;
				cval = (ub1 *)NULL;
					OCI_CHECK(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret, ctlp,OCIDirPathColArrayEntrySet(ca, ctlp->errhp_ctl,rowoff, colp->id_col,cval, clen, cflg));			
			}
		}
	} /* end of setting attr values in col array */

	CLEAR_PCTX(ctlp->pctx_ctl);
	if (ctlp->bufoff_ctl) 
		/* data in secondary buffer for this row*/
		return FIELD_SET_BUF;
	else
		return FIELD_SET_COMPLETE;
}

void OraDumper::GetPos(char *str, struct fld *flds, ub4 ncols, ub4& nacts)
{//str = "12,ad,45,ef";
	int col = 0;
	int nchlen = 1; // length of seperator
	char *pcur = strchr(str, m_chSep);
	while(pcur && col < ncols)
	{
		flds[col].begpos_fld =  ( 0 == col ? 1 : flds[col-1].endpos_fld + nchlen + 1);
		flds[col].endpos_fld =  pcur - str;

		pcur = strchr(pcur + nchlen, m_chSep);
		col++;
	}
	flds[col].begpos_fld =  ( 0 == col ? 1 : flds[col-1].endpos_fld + nchlen + 1);
	flds[col].endpos_fld =  strlen(str);

	nacts = col;
	return;
}

/*
 **++++++++++++++++++++++++++++++ errprint +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** errhp
 ** htype
 ** errcodep
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::errprint(dvoid *errhp, ub4 htype, sb4 *errcodep)
{
	text errbuf[512];

	if (errhp)
	{
		sb4 errcode;
		if (errcodep == (sb4 *)0)
			errcodep = &errcode;

		(void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, errcodep,errbuf, (ub4) sizeof(errbuf), htype);
		(void) Log(m_fErrorLog, "Error[%d] - %.*s\n", errcode, 512, errbuf);
	}
}


/*
 **++++++++++++++++++++++++++++++ checkerr +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** errhp
 ** htype
 ** status
 ** note
 ** code
 ** file
 ** line
 **
 ** Returns:
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::checkerr(dvoid *errhp, ub4 htype, sword status, text *note, sb4 code, text *file, sb4 line)
{
	sb4 errcode = 0;

	if ((status != OCI_SUCCESS))
		(void) Log(m_fErrorLog, "OCI Error %ld occurred at File %s:%ld\n",(long)status, (char *)file, (long)line);
	if (note)
		(void) Log(m_fErrorLog, "File %s:%ld (code=%ld) %s\n",(char *)file, (long)line, (long)code, (char *)note);

	switch (status)
	{
		case OCI_SUCCESS:
			break;
		case OCI_SUCCESS_WITH_INFO:
			(void) Log(m_fErrorLog, "Error - OCI_SUCCESS_WITH_INFO\n");
			errprint(errhp, htype, &errcode);
			break;
		case OCI_NEED_DATA:
			(void) Log(m_fErrorLog, "Error - OCI_NEED_DATA\n");
			break;
		case OCI_NO_DATA:
			(void) Log(m_fErrorLog, "Error - OCI_NODATA\n");
			break;
		case OCI_ERROR:
			errprint(errhp, htype, &errcode);
			break;
		case OCI_INVALID_HANDLE:
			(void) Log(m_fErrorLog, "Error - OCI_INVALID_HANDLE\n");
			break;
		case OCI_STILL_EXECUTING:
			(void) Log(m_fErrorLog, "Error - OCI_STILL_EXECUTE\n");
			break;
		case OCI_CONTINUE:
			(void) Log(m_fErrorLog, "Error - OCI_CONTINUE\n");
			break;
		default:
			break;
	}
}


/*
 **++++++++++++++++++++++++++++++ cleanup +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Frees up handles and exit with the supplied exit status code.
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** ex_status
 **
 ** Returns:
 ** ex_status
 **
 **-------------------------------------------------------------------------
 */

void OraDumper::cleanup(struct loadctl *ctlp, sb4 ex_status, sb4 ex_flag /* = 1 */)
	/* load control structure pointer */
{
	sword ociret;
	/* Free the column array and stream handles if they have been
	 * allocated. We don't need to do this since freeing the direct
	 * path context will free the heap which these child handles have
	 * been allocated from. I'm doing this just to exercise the code
	 * path to free these handles.
	 */
	if (ctlp->dpca_ctl)
	{
		ociret = OCIHandleFree((dvoid *)ctlp->dpca_ctl,
				OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
		if (ociret != OCI_SUCCESS)
			CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
	}
	if (ctlp->dpstr_ctl)
	{
		ociret = OCIHandleFree((dvoid *)ctlp->dpstr_ctl,OCI_HTYPE_DIRPATH_STREAM);
		if (ociret != OCI_SUCCESS)
			CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
	}
	/* free object-related dpapi handles if loading to obj/opq/ref cols */
	if (ctlp->loadobjcol_ctl) 
	{
		ub2 i;
		struct col *colp;
		struct tbl *tblp = &table;

		for (i = 0, colp = tblp->col_tbl; i < tblp->ncol_tbl; i++, colp++)
		{
			if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
			{
				free_obj_hndls(ctlp, colp->obj_col);
				if (colp->obj_col->ca_obj)
				{
					ociret = OCIHandleFree((dvoid *)(colp->obj_col->ca_obj),OCI_HTYPE_DIRPATH_FN_COL_ARRAY);
					if (ociret != OCI_SUCCESS)
						CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
				}
				if (colp->obj_col->ctx_obj)
				{
					ociret = OCIHandleFree((dvoid *)(colp->obj_col->ctx_obj), OCI_HTYPE_DIRPATH_FN_CTX);
					if (ociret != OCI_SUCCESS)
						CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
				}
			}
		}
	}

	if (ctlp->dpctx_ctl)
	{
		ociret = OCIHandleFree((dvoid *)ctlp->dpctx_ctl, OCI_HTYPE_DIRPATH_CTX);
		if (ociret != OCI_SUCCESS)
			CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
	}
	if (ctlp->errhp_ctl && ctlp->srvhp_ctl)
	{
		(void) OCIServerDetach(ctlp->srvhp_ctl, ctlp->errhp_ctl, OCI_DEFAULT );
		ociret = OCIHandleFree((dvoid *)ctlp->srvhp_ctl, OCI_HTYPE_SERVER);
		if (ociret != OCI_SUCCESS)
			CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
	}

	if (ctlp->svchp_ctl)
		(void) OCIHandleFree((dvoid *) ctlp->svchp_ctl, OCI_HTYPE_SVCCTX);
	if (ctlp->errhp_ctl)
		(void) OCIHandleFree((dvoid *) ctlp->errhp_ctl, OCI_HTYPE_ERROR);
	if ((output_fp != stdout) && (output_fp != stderr))
		fclose(output_fp);

	if(ex_flag)
		exit((int)ex_status);
}
/*
 **++++++++++++++++++++++++++++ free_obj_hndls +++++++++++++++++++++++++++++++++
 **
 ** Description:
 **
 ** Frees up dpapi object handles (function column array & function context).
 **
 ** Assumptions:
 **
 ** Parameters:
 **
 ** ctlp load control structure pointer 
 ** objp object structure pointer
 **
 ** Returns:
 ** Nothing.
 **
 **-------------------------------------------------------------------------
 */
void OraDumper::free_obj_hndls(struct loadctl *ctlp, struct obj *objp)
{
	ub2 i;
	struct col *colp; /* column pointer */
	sword ociret;

	for (i = 0, colp = objp->col_obj; i < objp->ncol_obj; i++, colp++)
	{
		if (colp->exttyp_col == SQLT_NTY || colp->exttyp_col == SQLT_REF)
		{
			free_obj_hndls(ctlp, colp->obj_col);
			if (colp->obj_col->ca_obj)
			{
				ociret = OCIHandleFree((dvoid *)(colp->obj_col->ca_obj),OCI_HTYPE_DIRPATH_FN_COL_ARRAY);
				if (ociret != OCI_SUCCESS)
					CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
			}

			if (colp->obj_col->ctx_obj)
			{
				ociret = OCIHandleFree((dvoid *)(colp->obj_col->ctx_obj), OCI_HTYPE_DIRPATH_FN_CTX);
				if (ociret != OCI_SUCCESS)
					CHECKERR(ctlp->errhp_ctl, OCI_HTYPE_ERROR, ociret);
			}
		}
	}
}



