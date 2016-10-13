#!/usr/bin/env python2.7
# -*- coding: UTF-8 -*-

from __future__ import print_function
import os, os.path, re, signal, subprocess, sys, time

from datetime import date, datetime, timedelta;
from glob import glob
from optparse import OptionParser
from string import Template
from traceback import print_exc

import convert

SCRIPT_PATH = os.path.abspath (os.path.dirname(sys.argv[0]));
DATAX_HOME = os.path.abspath (os.path.join (SCRIPT_PATH, '../datax'));
CONF_FILE_BASE_PATH = os.path.abspath (os.path.join (SCRIPT_PATH, '../config'));
JAVA_HOME = os.environ.get ('JAVA_HOME', '/usr/java/jdk');
LD_LIBRARY_PATH = os.environ.get ('LD_LIBRARY_PATH', '');
TIMEOUT = 3600;
RETRY = 2;
LEGACY_MODE = False;
PREPROCESS_ONLY = False;
XML_TEMP_DIR = '/tmp/datax-xml';
EXTRA_PARAMS = "";
WORK_DATE = date.today () - timedelta(days=1);

# these are relative path to CONF_FILE_BASE_PATH
BLUEPRINT_CONF_FILE = "auxiliary/template.xml"
ENTRY_CONF_FILE = "job_config/datax_job_config.xml";
DB_CONF_DIR = "db_config";
PARAM_CONF_DIR = "param_config";

COMMAND =   ['${JAVA_HOME}/bin/java',
             '-Xmx512m',
             '-XX:+HeapDumpOnOutOfMemoryError',
             '-XX:HeapDumpPath=/tmp/datax',
             '-Djavam',
             '-Djava.ext.dirs=${JAVA_HOME}/jre/lib/ext',
             '-Djava.library.path=${LD_LIBRARY_PATH}',
             '-cp',
             '${DATAX_HOME}/lib/*:${DATAX_HOME}/common/*:${DATAX_HOME}/engine/*:${DATAX_HOME}/common/datax-server-1.0-SNAPSHOT.jar:${DATAX_HOME}/conf:.',
             '${PARAMS}',
             'com.taobao.datax.engine.schedule.Engine',
            ];
            
CP = None;

def kill_gracefully (cp):
    if cp:
        cp.send_signal (signal.SIGQUIT);
        time.sleep (1);
        try:
            cp.kill ();
        except:
            pass

def sig_handler (signum, frame):
    print ("[WARNING] DataX is receiveing signal %d and going to die ." % (signum), file=sys.stderr);
    kill_gracefully (CP);
    exit (2);
    
# damn to python2 there is no timeout for Popen.wait, use alarm to mimic similar feature
class Alarm(Exception):
    pass
def alarm_handler(signum, frame):
    raise Alarm
    
def parse_opt (argv=sys.argv[1:]):
    global CONF_FILE_BASE_PATH, DATAX_HOME, EXTRA_PARAMS, LD_LIBRARY_PATH, LEGACY_MODE, JAVA_HOME, PREPROCESS_ONLY, RETRY, TIMEOUT, WORK_DATE, XML_TEMP_DIR;

    op = OptionParser (usage="Usage: %prog [options] job [job1 job2 ...]\n\t\t job can be either a XML file or 'type_id:job_id' pair");
    op.add_option ('-C', '--configure-home', dest='chome', action='store', default=None, help="Set configure XML base dir, instead of basing on script path, currently is '%s'" % CONF_FILE_BASE_PATH);
    op.add_option ('-D', '--datax-home', dest='dhome', action='store', default=None, help="Set datax home, this will override DATAX_HOME env value, currently is '%s'" % DATAX_HOME);
    op.add_option ('-J', '--java-home', dest='jhome', action='store', default=None, help="Set java home, this will override JAVA_HOME env value, currently is '%s'" % JAVA_HOME);
    op.add_option ('-L', '--lib-paths', dest='lpaths', action='append', default=[], help='Add dir to the list of dirs to be searched for libs, i.e. LD_LIBRARY_PATH env value');
    op.add_option ('-O', '--output-dir', dest='tmpdir', action='store', default=None, help="Store generated temp XML files in this dir, default dir '%s'" % XML_TEMP_DIR);
    op.add_option ('-R', '--retry', dest='retry', action='store', default=None, type="int", help='Retry times of each single job, default value is %d' % RETRY);
    op.add_option ('-T', '--timeout', dest='timeout', action='store', default=None, type="int", help='Timeout for single job, default value is %d' % TIMEOUT);
    op.add_option ('-X', '--xml-preprocess', dest='preprocess', action='store_true', default=False, help='Stop after generate the XML files.');

    # backward compatable with old script
    op.add_option ('-l', '--legacy-mode', dest='legacy', action='store_true', default=False, help='switch to legacy mode, job config passed from -j & -n');
    op.add_option ('-j', '--job', dest='_job', action='store', default=None, type="int", help="execute the chosen job on datax_job_config.xml which taged as type $JOB. This option implys '-l'");
    op.add_option ("-n", "--no", dest='_no', action='store', default=None, type="int", help="execute the chosen number item on  the chosen job which taged as type $NO. This option implys '-l'")
    op.add_option ('-p', '--params', dest='params', action='store', default="", help='add DataX runtime parameters .');
    op.add_option ('-t', '--work-date', dest='wdate', action='store', default="", help="work date, formation is 'YYYYMMDD' or 'YYYY-MM-DD', default is yesterday(%s)" % WORK_DATE);

    # parse options and set up environment
    opt, args = op.parse_args (argv);
    if opt.legacy or opt._job or opt._no:
        LEGACY_MODE = True;
    if opt.chome:
        CONF_FILE_BASE_PATH = os.path.abspath (opt.chome);
    if opt.dhome:
        DATAX_HOME = os.path.abspath (opt.dhome);
    if opt.jhome:
        JAVA_HOME = os.path.abspath (opt.jhome);
    if opt.tmpdir:
        XML_TEMP_DIR = opt.tmpdir;
    if opt.timeout > 0:
        TIMEOUT = opt.timeout;
    if opt.retry >= 0:
        RETRY = opt.retry;
    if opt.preprocess:
        PREPROCESS_ONLY = True;
    if opt.params:
        EXTRA_PARAMS = opt.params;
    if opt.wdate:
        try:
            dt = datetime.strptime (opt.wdate, '%Y-%m-%d')
        except:
            try:
                dt = datetime.strptime (opt.wdate, '%Y%m%d')
            except:
                print ("[ERROR] Invalid date represent '%s'" % opt.wdate);
                exit (1);
        WORK_DATE = dt.date ();
    lpaths = [os.path.abspath (p) for p in  opt.lpaths] + [DATAX_HOME + '/plugins/reader/hdfsreader', DATAX_HOME + '/plugins/writer/hdfswriter', LD_LIBRARY_PATH];
    LD_LIBRARY_PATH = ':'.join (lpaths);

    # validate integrity of arguments
    if LEGACY_MODE:
        if opt._job == None or opt._no == None:
            op.parse_args (['-h']);
            exit (1);
        # All extra arguments are ignored in legacy mode.
        # The real work is transfered to compatable form as standard and replaces the original arguments,
        # so it can be processed as usual.
        args = ['%d:%d' % (opt._job, opt._no)];
    elif not args:
        op.parse_args (['-h']);
        exit (1);

    return args;

def preprocess (targets):
    if not os.path.isdir (XML_TEMP_DIR):
        os.makedirs (XML_TEMP_DIR, mode=0o700);
    xmls = [];
    bpfile = os.path.join (CONF_FILE_BASE_PATH, BLUEPRINT_CONF_FILE);
    etfile = os.path.join (CONF_FILE_BASE_PATH, ENTRY_CONF_FILE);
    job_conf_path = os.path.join (CONF_FILE_BASE_PATH, PARAM_CONF_DIR);
    dbfiles= glob (os.path.join (CONF_FILE_BASE_PATH, DB_CONF_DIR, '*.xml'));
    for target in targets:
        m = re.search (r'^(\d+|\*):(\d+|\*)$', target);
        if not m:
        # treat target as file
            xmls.append (os.path.abspath (target));
            continue;
    
        jid, wid = m.groups();
        jid = (0 if jid == '*' else int (jid));
        wid = (0 if wid == '*' else int (wid));
        files = convert.generate_files_with_config (jid, wid, WORK_DATE, XML_TEMP_DIR, bpfile, etfile, job_conf_path, *dbfiles);
        if files:
            xmls += files;
    return xmls;

def run_work_aux (cmd, target, e):
    cmd = cmd + [target];
    #print (' '.join (cmd));
    
    stime = time.time ();
    global CP;
    try:
        # I KNOW this code looks like shit
        # However, there is no way to make it work at any CWD without invoke SHELL
        CP = subprocess.Popen (' '.join(cmd), cwd=DATAX_HOME, env=e, shell=1);
    except Exception:
        print_exc ();
        CP = None;
        return -1, 0;
        
    signal.alarm (TIMEOUT);
    try:
        CP.communicate();
        signal.alarm(0);  # reset the alarm
    except Alarm:
        kill_gracefully (CP);
        print ("[ERROR] Run time out for task '%s', task killed." % target)
        CP = None;
        return -2, TIMEOUT;
    
    rc = (0 if CP.returncode == 0 else -3);
    CP = None;
    return rc, time.time () - stime;

   
def run_work (cmd, target):
    if not os.path.isfile (target):
        print ("[ERROR] File '%s' does not exists." % target, file=sys.stderr);
        return -1, 0;

    env = {k:v for k,v in os.environ.items()};
    env['JAVA_HOME'] = JAVA_HOME;

    for i in range (1, RETRY + 1):
        if i > 1:
            print ("[WARNING] The %dth attempt for work '%s'." % (i, target));
        rc, cost = run_work_aux (cmd, target, env);
        if rc == 0:
            return rc, cost;
        time.sleep (i * 3);
    
    return rc, cost;

#################################################
if __name__ == '__main__':
    # parse options
    args = parse_opt ();
       
    # subtitute command
    d = {'DATAX_HOME':DATAX_HOME, 'JAVA_HOME':JAVA_HOME, 'LD_LIBRARY_PATH':LD_LIBRARY_PATH, 'PARAMS':EXTRA_PARAMS};
    cmd = [Template(x).substitute(**d) for x in COMMAND];
    print ("cmd = \n", cmd);
    
    # register signal handlers
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGQUIT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    
    # preprocess
    targets = preprocess (args);
    for x in targets:
        print (x);
    if PREPROCESS_ONLY:
        exit (0);

    # start works
    result = [];
    for t in targets:
        rc, cost = run_work (cmd, t);
        result.append (rc);
        if rc == 0:
            print ("[INFO] Work '%s' complete successfully, time cost %d seconds." % (t, cost));
        elif rc == -1:
            print ("[ERROR] Unable to launch work '%s', check arguments and configurations please." % t, file=sys.stderr);
        elif rc == -2:
            print ("[ERROR] Work '%s' timeout, check arguments or try use a bigger timeout(currently %d)." % (t, TIMEOUT), file=sys.stderr);
        else:
            print ("[ERROR] work '%s' failed." % t);
            exit (1);
    
    # exit base on work status
    exit (0 if all (r == 0 for r in result) else 2);



