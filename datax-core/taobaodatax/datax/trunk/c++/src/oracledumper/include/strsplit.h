/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#ifndef __STRSPLIT_H__
#define __STRSPLIT_H__

#include "common.h"
//----------------------------------------------
// CStrSplit
// ��������:
//     �ַ����,���ַ�ָ���ָ�����,���±�����ط��Ѻ���ֶ�
//----------------------------------------------
#define MAX_FIELD_CNT   512
#define MAX_FIELD_SIZE  1024

class CStrSplit
{
public:
     //default���캯��
     CStrSplit();
     
     //default��������
     virtual ~CStrSplit();
          
	 virtual int Init();

     //�а��ض��ָ�������
     //    pSourStr ��Դ�ַ�
     //    del ���ָ���
     virtual int Split(const char* pSourStr, const char* del);
     
     //�����ֶ���
     virtual int GetFieldCount();
     
     //�����ֶ�ֵ
     //    n ����n�ֶ�ֵ
     virtual const char* GetField(int n);
     
     //�����ֶ�ֵ
     //    n ����n�ֶ�ֵ
     virtual const char* operator[] (int n);
//attribute
protected:
     //����fieldָ��
     char** m_ppFields;
      
     //�ֶ���
     int m_nFieldCnt;
     
     const char* m_pLine;

};

#endif /* __STRSPLIT_H__ */

