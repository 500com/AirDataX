/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#include "common.h"

static bool NotSpace(char c)
{
	return ( isspace(c) == 0 );
}

void Trim(string& strStr)
{
	LTrim(strStr);
	RTrim(strStr);
}

void LTrim(string& strStr)
{
	IterStr iterStr = find_if( strStr.begin(), strStr.end(), NotSpace);
	strStr.erase(strStr.begin(), iterStr);
}

void RTrim(string& strStr)
{
	IterStr iterStr = find_if( strStr.rbegin(), strStr.rend(), NotSpace).base();
	strStr.erase(iterStr, strStr.end());
}

string Format(const char* fmt, ... )
{
       char szStr[MAX_BUFF_SIZE];
       va_list vl;
       va_start(vl, fmt);
       vsprintf(szStr, fmt, vl);
       
       va_end(vl);
       
       return string(szStr);
}

int Log(FILE* fp, const char* fmt, ...) {
    va_list args;
    int ret = 0;

    va_start(args, fmt);
    ret = vfprintf(fp, fmt, args);
    va_end(args);
    
    fflush(fp);
    return ret;
}

int FileType( const char* pFileName )
{
       const int mNum = 2;
       size_t magic[mNum][4] = 
       {
       	    {0,    2,     8093,     2}, 
       	    {0,    4,     529205256,3}  
       };

       char szHDFS[16] = {0};
       if( sscanf(pFileName, "%4s", szHDFS) > 0 && 0 == strncmp("hdfs", szHDFS, 4))
           return 4;	// hdfs file
       
       FILE* pFile = fopen(pFileName,"r+b");
       if ( pFile == NULL )
       {
             return -1;
       }
       char buff[10];
       int n = -1;

       while ( ++n < mNum )
       {
             fseek( pFile, magic[n][0], SEEK_SET );
             if( fread(buff,sizeof(char),magic[n][1],pFile) )
             {
                   buff[magic[n][1]] = '\0';
                   if ( GetMagic(buff) == magic[n][2] )
                   {
                         break;
                   }
             }
       }
       fclose(pFile);
       
       if ( n < mNum)
       {
             return magic[n][3];
       }
       else  return 1; 
}

int GetMagic( const char* pStr )
{
       int value = 0;
       unsigned int tmp = 0;
       const char* pCh = pStr;
       
       int mask = 0x000000FF;

       while ( *pCh != '\0' )
       {
            tmp = *pCh;
            tmp &= mask;
            value = value << 8;
            value |= tmp;
            pCh++;
       }
       return value;
}

void DelEndLine(char *line)
{
	char *p = strrchr(line, '\n');
	
	if (p != NULL && *(p + 1) == '\0') {
		*(p - 1) = '\0';
	}
}

