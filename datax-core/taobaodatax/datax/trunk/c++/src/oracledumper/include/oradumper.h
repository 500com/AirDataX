/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#ifndef __ORADUMPER_H__
#define __ORADUMPER_H__

#include <oratypes.h>
#include <oci.h>
#include <iconv.h>

#include "cdemodp0.h"
#include "dumper.h"
#include "strsplit.h"

#define COLLEN 1000
using namespace std;   


#ifndef bit
# define bit(x, y) ((x) & (y))
#endif

#ifndef OER
# define OER(x) (x)
#endif


#define NPOS   255                            /* max number of columns */
#define MAXLEN 128                            /* max length of column names */

/* below are used by cleanup */
#define EXIT_SUCCEED    0                       /* oraclewriter exit successfully */
#define EXIT_FAILED     1                       /* oraclewriter failed */
#define EXIT_RETRY      2                       /* oraclewriter error, retry it */

/* In order to retry in almost situation */
#define OCI_CHECK(errhp, htype, status, ctlp, OCIfunc) \
	if (OCI_SUCCESS != ((status) = (OCIfunc))) \
{ \
	printf("status: %d, %d, %s, %s\n", status, __LINE__, __FILE__, __FUNCTION__); \
	checkerr((dvoid *)(errhp), (ub4)(htype), (sword)(status), (text *)0, \
(sb4)0, (text *)__FILE__, (sb4)__LINE__); \
	printf("%d, %s, %s\n", __LINE__, __FILE__, __FUNCTION__);	\
	if ((status) != OCI_SUCCESS_WITH_INFO) \
	cleanup((struct loadctl *)ctlp, (sb4)EXIT_RETRY); \
} else

#define CHECKERR(errhp, htype, status) \
	checkerr((dvoid *)errhp, (ub4)(htype), (sword)(status), (text *)0, \
(sb4)0, (text *)__FILE__, (sb4)__LINE__);

#define FATAL(note, state) \
	do \
{ \
	checkerr((dvoid *)0, (ub4)OCI_HTYPE_ERROR, (sword)OCI_SUCCESS, \
	(text *)(note), (sb4)(state), (text *)__FILE__, (sb4)__LINE__); \
	cleanup((ctlp), (sb4)EXIT_FAILED); \
} while (0)


#ifndef bit
# define bit(x, y) ((x) & (y))
#endif

#ifndef OER
# define OER(x) (x)
#endif

/* External column attributes */
struct col
{
	text *name_col; /* column name */
	ub2 id_col;     /* column load id */
	ub2 exttyp_col; /* external type */
	text *datemask_col; /* datemask, if applicable */
	ub1 prec_col;   /* precision, if applicable */
	sb1 scale_col;  /* scale, if applicable */
	ub2 csid_col;   /* character set id */
	ub1 date_col;   /* is column a chrdate or date? 1=TRUE. 0=FALSE */
	struct obj * obj_col; /* description of object, if applicable */
#define COL_OID 0x1 /* col is an OID */
	ub4 flag_col;
};

/* Input field descriptor
* For this example (and simplicity),
* fields are strictly positional.
*/
struct fld
{
	ub4 begpos_fld; /* 1-based beginning position */
	ub4 endpos_fld; /* 1-based ending position */
	ub4 maxlen_fld; /* max length for out of line field */
	ub4 flag_fld;
#define FLD_INLINE 0x1
#define FLD_OUTOFLINE 0x2
#define FLD_STRIP_LEAD_BLANK 0x4
#define FLD_STRIP_TRAIL_BLANK 0x8
};

struct obj
{
	text *name_obj; /* type name*/
	ub2 ncol_obj; /* number of columns in col_obj*/
	struct col *col_obj; /* column attributes*/
	struct fld *fld_obj; /* field descriptor*/
	ub4 rowoff_obj; /* current row offset in the column array*/
	ub4 nrows_obj; /* number of rows in col array*/
	OCIDirPathFuncCtx *ctx_obj; /* Function context for this obj column*/
	OCIDirPathColArray *ca_obj; /* column array for this obj column*/
	ub4 flag_obj; /* type of obj */
#define OBJ_OBJ 0x1 /* obj col */
#define OBJ_OPQ 0x2 /* opaque/sql str col */
#define OBJ_REF 0x4 /* ref col */
};

struct tbl
{
	text *owner_tbl; /* table owner */
	text *name_tbl; /* table name */
	text *subname_tbl; /* subname, if applicable */
	ub2 ncol_tbl; /* number of columns in col_tbl */
	text *dfltdatemask_tbl; /* table level default date mask */
	struct col *col_tbl; /* column attributes */
	struct fld *fld_tbl; /* field descriptor */
	ub1 parallel_tbl; /* parallel: 1 for true */
	ub1 skipindex;
	ub1 nolog_tbl; /* no logging: 1 for true */
	ub4 xfrsz_tbl; /* transfer buffer size in bytes */
	text *objconstr_tbl; /* obj constr/type if loading a derived obj */
};

struct sess /* options for a direct path load session */
{
	text *username_sess; /* user */
	text *password_sess; /* password */
	text *inst_sess; /* remote instance name */
	text *outfn_sess; /* output filename */
	ub4 maxreclen_sess; /* max size of input record in bytes */
};

struct loadctl
{
	ub4 nrow_ctl; /* number of rows in column array */
	ub2 ncol_ctl; /* number of columns in column array */
	OCIEnv *envhp_ctl; /* environment handle */
	OCIServer *srvhp_ctl; /* server handle */
	OCIError *errhp_ctl; /* error handle */
	OCIError *errhp2_ctl; /* yet another error handle */
	OCISvcCtx *svchp_ctl; /* service context */
	OCIStmt    *stmthp_ctl;/* statement context */
	OCISession *authp_ctl; /* authentication context */
	OCIParam *colLstDesc_ctl; /* column list parameter handle */
	OCIDirPathCtx *dpctx_ctl; /* direct path context */
	OCIDirPathColArray *dpca_ctl; /* direct path column array handle */
	OCIDirPathColArray *dpobjca_ctl; /* dp column array handle for obj*/
	OCIDirPathColArray *dpnestedobjca_ctl; /* dp col array hndl for nested obj*/
	OCIDirPathStream *dpstr_ctl; /* direct path stream handle */
	ub1 *buf_ctl; /* pre-alloc'd buffer for out-of-line data */
	ub4 bufsz_ctl; /* size of buf_ctl in bytes */
	ub4 bufoff_ctl; /* offset into buf_ctl */
	ub4 *otor_ctl; /* Offset to Recnum mapping */
	ub1 *inbuf_ctl; /* buffer for input records */
	struct pctx pctx_ctl; /* partial field context */
	boolean loadobjcol_ctl; /* load to obj col(s)? T/F */
};

struct loaderr
{
    struct errinfo
    {
        string strErrMsg;      // load error message
        int    nErrCount;      // error occured count
    };

    map<int, struct errinfo> mErrInfo; // all error record
    int nLastErrCode;          // lastest error code
};

class OraDumper : public Dumper
{
public:
	OraDumper();
	~OraDumper();
	virtual void	DumperInit();
	virtual void	PreDump(long flag);
	virtual void	RunDump(const char *lines);
	virtual void	CommitDump(bool bCommit);
	virtual int 	PostDump(long flag);

	virtual int simple_load(char *line, size_t size);

private:
	void PreLoadFiles();
	void GetPos(char *str, struct fld *flds, ub4 ncols, ub4 &nacts);

	/* Forward references: */
	void field_flush(struct loadctl *ctlp, ub4 rowoff);
	sword field_set( struct loadctl *ctlp, struct tbl *tblp,struct obj *objp, text *recp, ub4 rowoff, ub1 bufflg );
	
	void init_obj_load( struct loadctl *ctlp, struct tbl *tblp,struct obj *objp );
	void alloc_obj_ca( struct loadctl *ctlp, struct tbl *tblp,struct obj *objp );
	void init_load( struct loadctl *ctl, struct tbl *table,struct sess *session );
	
	void finish_load( bool bSave );
	void errprint( dvoid *errhp, ub4 htype, sb4 *errcodep );
	
	void checkerr( dvoid *errhp, ub4 htype, sword status,text *note, sb4 state, text *file, sb4 line );
	void cleanup( struct loadctl *ctlp, sb4 ex_status, sb4 ex_flag = 1 );
	sword do_convert( struct loadctl *ctlp, ub4 startoff, ub4 rowcnt, ub4 *cvtCntp, ub2 *badcoffp );
	sword do_load( struct loadctl *ctlp, ub4 *loadCntp );
	void free_obj_hndls(struct loadctl *ctlp, struct obj *objp);
	
	ub4    describe_table(struct loadctl *ctlp,text *tablename );
	void   describe_column(struct loadctl *ctlp, OCIParam *parmp, ub2 &parmcnt );
	void   describe_type(struct loadctl *ctlp, OCIParam *parmp );
	void   describe_typeattr(struct loadctl *ctlp, OCIParam *parmp, ub4 num_attr );
	void   describe_typecoll(struct loadctl *ctlp, OCIParam *parmp, sword typecode );
	void   describe_typemethodlist(struct loadctl *ctlp, OCIParam *parmp, ub4 num_meth,
                              text *comment );
	void   describe_typemethod(struct loadctl *ctlp, OCIParam *parmp, text *comment );
	void   describe_typearg(struct loadctl *ctlp, OCIParam *parmp, ub1 type, ub4 start,
                                                                ub4 end );

	void   checkerr( OCIError *errhp, sword status );

private:
	vector<string> vColName;
	vector<int>    vColLen;

    struct loadctl 	m_oCtl;
    struct loadctl 	*ctlp;
    struct loaderr 	m_oLoadErr;

    map<string, string> m_mEncode;

	//simple_load的所有变釄1?7 -- yixiao
	sword fsetrv; /* return value from field_set */
	sword cvtrv; /* return value from do_convert */
	sword ldrv; /* return value from do_load */
	ub4 startoff; /* starting row offset for conversion */
	ub4 nxtLoadOff; /* column array offset to be loaded next */
	ub4 rowCnt; /* count of rows populated in column array */
	ub4 cvtCnt; /* count of rows converted */
	ub4 lastoff; /* last row offset used in column array */
	sword state; /* current state machine state */
	sword done; /* set to TRUE when load is complete */
	ub4 input_recnum; /* current input record number */
	ub4 load_recnum; /* record number corresponding to last record loaded */
	ub4 err_recnum; /* record number corresponding to error */
	text *recp;
	ub4 cvtcontcnt; /* # of times CONVERT_CONTINUE returned */
	ub4 dotCnt; /* count of dots displayed */

	bool bFirstCall;

	OCIDescribe  *dschp ;
	struct fld fldArray[COLLEN];
	struct tbl table;
	struct sess session;
	struct col  colptr;
	struct col colArray[COLLEN];

	//add buffer
	char *data_buff;

	//add iconv var
	iconv_t m_cd;
	char *m_inbuf;
	char *m_outbuf;
	size_t m_outlen;
	long m_notconv;
	int m_lines; //3 is end

	FILE *m_fConvLog;
};

Dumper *Create();

#endif /* __ORADUMPER_H__ */

