#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */
 
"""

import os
import sys
import time
import signal
import subprocess
import os.path
import urllib2

from optparse import OptionParser
from string import Template

engineCmd='''
java -Xmx800m -Djava.ext.dirs=${libs} -Djava.library.path=${share_library} ${params} -jar ${jar} ${jobdescpath}
'''
editCmd='''
java -jar -Djava.ext.dirs=${libs} -jar ${jar}
'''

childProcess = None

def getCopyRight():
    copyright = """
DataX V 1.0, Taobao Data Platform
Copyright (C) 2010-2011, Alibaba Group. All Rights Reserved.
"""
    return copyright

def getUsage():
    usage = '''Usage: datax.py [-e] [-p params] job.xml'''
    return usage

def showUsage():
    print getUsage()
    return

def initOptionParser():
    op = OptionParser()
    op.add_option('-e', '--edit', action="store_true", dest="edit", help='edit job config file .')
    op.add_option('-p', '--params', default="", help='add DataX runtime parameters .')
    op.set_usage(getUsage())
    return op

def registerSignal(process):
    global childProcess
    childProcess = process
    signal.signal(2, suicide)
    signal.signal(3, suicide)
    signal.signal(15, suicide)
    return

def suicide(signum, e):
    print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide ." % (signum)
    if childProcess is not None:
        childProcess.send_signal(signal.SIGQUIT)
        time.sleep(1)
        childProcess.kill()
    return

def getJobName(urlStr):
    name = urlStr[urlStr.find(r'=') + 1:]
    return name

def isUrl(arg):
    return arg.strip().lower().find('http') == 0

def isJobMsg(jobMsg):
    sflag = jobMsg.find('''<?xml version="1.0" encoding="UTF-8"?>''')
    eflag = jobMsg.find('''</job></jobs>''')
    return sflag != -1 and eflag != -1

def genJobXml(jobMsg,jobName):
    fileLocation = os.path.abspath('jobs/' + jobName + '.xml')
    with open(fileLocation, r'w') as fp:
        fp.write(jobMsg)
    return

def setSharePath():
    if r'LD_LIBRARY_PATH' not in os.environ:
        os.environ['LD_LIBRARY_PATH'] = ''
    os.environ['LD_LIBRARY_PATH'] = r"/home/taobao/datax/libs:" + os.environ['LD_LIBRARY_PATH']
    return

if __name__ == '__main__':
    if len(sys.argv) == 1:
        showUsage()
        sys.exit(0)

    os.chdir(sys.path[0]+"/..")
    ctxt={}
    ctxt['jar'] = "engine/engine-1.0.0.jar"
    ctxt['libs'] = "libs:common"
    ctxt['share_library'] = r"plugins/reader/hdfsreader:plugins/writer/hdfswriter"

    options, args = initOptionParser().parse_args(sys.argv[1:])

    if options.edit:
        cmd = Template(editCmd).substitute(**ctxt)
        sys.exit(os.system(cmd))

    print(getCopyRight())
    sys.stdout.flush()

    ctxt['params'] = options.params
    if not isUrl(args[0]):
        ctxt['jobdescpath'] = os.path.abspath(args[0])
    else:
        counter = -1
        response = None
        while counter < 3:
            counter += 1
            try:
            #try to fetch job.xml from skynet
                response = urllib2.urlopen(args[0])
                jobMsg = response.read()
                if isJobMsg(jobMsg):
                    genJobXml(jobMsg,getJobName(args[0]))
                    ctxt['jobdescpath'] = os.path.abspath("jobs/" + getJobName(args[0]) +".xml")
                    break
                else:
                    print >>sys.stderr, r"[Warning] DataX querying Job config file failed, sleep %d sec and try again." % (2**counter)
                    time.sleep(2**counter)
                    continue

            except Exception, ex:
                print >>sys.stderr, str(ex)
                print >>sys.stderr, r"[Warning] DataX querying Job config file failed, sleep %d sec and try again." % (2**counter)
                time.sleep(2**counter)

            finally:
                if response is not None:
                    response.close()

        if counter >= 3 and \
            ctxt.get(r'jobdescpath', None) is None:
            print >>sys.stderr, r"[Error] DataX querying Job config file failed!"
            sys.exit(2)

    try:
        from interceptor import InterceptorRegister
        interceptors = InterceptorRegister.instance()
        interceptors.process(ctxt)
    except Exception, ex:
        print("[INFO] Mysql Swither function disable : " + str(ex))
        sys.stdout.flush()

    cmd = Template(engineCmd).substitute(**ctxt)
    p = subprocess.Popen(cmd, shell=True)
    registerSignal(p)
    (stdo, stde) = p.communicate()

    retCode = p.returncode
    if 0 != retCode:
        sys.exit(2)
    else:
        sys.exit(0)

