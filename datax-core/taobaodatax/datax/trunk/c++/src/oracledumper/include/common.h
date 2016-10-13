/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


#ifndef __COMMON_H__
#define __COMMON_H__

#include <iostream>
#include <sstream>
#include <fstream>
#include <fstream>
#include <cassert>
#include <algorithm>

#include <vector>
#include <map>

#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glob.h>
#include <libgen.h>
#include <fnmatch.h>

using namespace std;

const int MAX_BUFF_SIZE = 1024;

typedef string::iterator	IterStr;

void Trim(string& strStr);
void LTrim(string& strStr);
void RTrim(string& strStr);
void DelEndLine(char *line);

int Log(FILE* fp, const char* fmt, ...);
string Format(const char *fmt, ...);

int FileType(const char *pFileName );
int GetMagic(const char *pStr);

#endif // __COMMON_H__

