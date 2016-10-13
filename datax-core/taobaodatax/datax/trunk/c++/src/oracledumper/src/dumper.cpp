/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#include <stdarg.h>

#include "dumper.h"
#include "common.h"

//Constructor
Dumper::Dumper()
{
	// load result initial
	m_nReadRows = 0;
	m_nGetrows = 0;
	m_nSkipRows = 0;
	m_nLoadRows = 0;
	m_nDeleteRows = 0;
	m_nRejRows = 0;
	m_nCommitRows = 0;
	
	m_nBadLines = 0;

	m_bIsUTF8 = false;

    /* line break */
    m_chBreak = '\002';

    /* seperator between field */
    m_chSep = '\001';

    return;
}

Dumper::~Dumper()
{
}

bool Dumper::QCCheck()
{
    if ( 0 == m_nReadRows ) // empty file, ignore limit and check ok, 20100706
        return true;
	
    // Quality control
    if ( m_fLimit < 1e-6 && m_fLimit > -1e-6)     // limit = 0
    { // limit = 0
        return true; /* finish the load */
    }
    else if( m_fLimit < 1 )
    { // percent
        if( m_nRejRows/(float)m_nReadRows <= m_fLimit )
            return true;
        else
            return false;
    }
    else if (m_fLimit >= 1)
    { // abs rows
        if( m_nRejRows <= m_fLimit )
            return true;
        else
            return false;
    }
}

void Dumper::GetLoadResult( int *pResult )
{
	pResult[0] = m_nReadRows;
	pResult[1] = m_nSkipRows;
	pResult[2] = m_nLoadRows;
	pResult[3] = m_nDeleteRows;
	pResult[4] = m_nRejRows;
	pResult[5] = m_nCommitRows;
}

