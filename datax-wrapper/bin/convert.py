# -*- coding: UTF-8 -*-

from __future__ import print_function;

import os, os.path, re, sys, xml.dom.minidom;
import xml.etree.ElementTree as XT;

from datetime import datetime;
from string import Template;
from traceback import print_exc;

import pattern

__all__ = ['generate_files_with_config', 'generate_files', 'load_base_config'];

def _load_xml_safe (file):
    if not os.path.isfile (file):
        print ("[ERROR] no such file '%s'." % file);
        return None;
    try:
        xml = XT.parse (file);
    except:
        print_exc ();
        return None;
    return xml.getroot ();
    
E1 = re.compile ('{yyyymmdd}', re.I);
E2 = re.compile ('{v_month_first}', re.I);
def _date_replace (s, dt):
    sdt = dt.strftime('%Y%m%d');
    s = E1.sub (sdt, s);
    sdt = dt.strftime('%Y%m01');
    s = E2.sub (sdt, s);
    return s;
    
###############################################################
ENTRIES     = {};
STORAGES    = {};
BLUEPRINTS  = {};


class Storage:
    def __init__ (self, elem):
        self.__info = {};
        self.__name = elem.tag;
        for e in elem:
            self.__info[e.tag] = e.text;
        self.alias = self.__name;
        self.dbtype = self.__info['type'];
    
    def __getitem__ (self, key):
        return self.__info[key];
        
    def get (self, key, default=None):
        try:
            return self[key];
        except KeyError:
            return default;

class Work:
    def __init__ (self, elem):
        self.__info = {};
        self.__name = elem.get ('title', None);
        for e in elem:
            self.__info[e.tag] = e.text;
        self.id = int (self['no']);
        self.source = self['ext_alias'];
        self.sink = self['load_alias'];
        self.enable = int (self['is_able']);
        
    def __getitem__ (self, key):
        return self.__info[key];
        
    def get (self, key, default=None):
        try:
            return self[key];
        except KeyError:
            return default;

class Box:
    def __init__ (self, work, storage):
        self.__d = {'JOB':work, 'DB':storage, 'ENV':os.environ};
        
    def __getitem__ (self, key):
        p, k = key.split ('_', 1);
        return self.__d[p][k];
        
class Blueprint:

    def __init__ (self, elem):
        self.__sttype = elem.tag;
        # readers and writers are kept here
        self.__rd = {};
        self.__wt = {};
        # default reader and writer
        self.__rddft = None;
        self.__wtdft = None;
        
        for e in elem:
            par = [];
            for p in e:
                par.append (p.attrib);
                
            name = e.get ('plugin');
            x = e.get ('isdefault');
            if e.tag == 'reader':
                self.__rd[name] = par;
                if x != None and (x.lower () == 'true' or int(x) != 0):
                    self.__rddft = name;
            elif e.tag == 'writer':
                self.__wt[name] = par;
                if x != None and (x.lower () == 'true' or int(x) != 0):
                    self.__wtdft = name;
            else:
                print ("[WARNNING] invalid template type '%s', ignored." % e.tag, file=sys.stderr);
                continue;
        
        # select random reader/writer as default if it has not been explicitly set
        if self.__rddft == None and self.__rd:
            self.__rddft = list(self.__rd.keys ())[0];
        if self.__wtdft == None and self.__wt:
            self.__wtdft = list(self.__wt.keys ())[0];
        
        self.dbtype = self.__sttype;
        
    # these following routine is actually a static method
    def _select (self, name, io=0):
        tp = ['reader', 'writer'][io];
        tdk = [self.__rddft, self.__wtdft][io];
        rwd = [self.__rd, self.__wt][io];
        if not name:
            print ("[WARNNING] no plugin name has been speicified, fall down to default method.", file=sys.stderr);
            t = tdk;
        elif name not in rwd:
            print ("[WARNNING] no %s to %s bearing the name of '%s', try using default method" % (tp, self.dbtype, name), file=sys.stderr);
            t = tdk;
        else:
            t = name;
        if t == None:
            print ("[ERROR] the %s to %s is not implemented yet, skipping..." % (tp, self.dbtype), file=sys.stderr);
            return None;
        return t, rwd[t];
        
    def combine (self, work, storage, dt, io=0):
        # step 1: validate storage type
        if storage.dbtype != self.dbtype:
            return None;
        b = Box (work, storage);
        
        # step 2: select proper reader/writer
        name = work.get ('plugin');
        rw = self._select (name, io);
        if rw == None:
            return None;
        name, rw = rw;
        
        # step 3: variables replace
        r = [];
        for p in rw:
            d = {};
            z = [];
            for k,v in p.items ():
                if k == 'value':
                    z.insert (0, v);
                elif k == 'default':
                    z.append (v);
                else:
                    d[k] = v;
            v = None;
            for x in z:
                t = Template (x);
                try:
                    v = t.substitute (b);
                    break;
                except KeyError:
                    pass
            if v == None:
                print ('ERROR! parameter %s is required!' % d['key'], file=sys.stderr);
                return None;

            # replace {YYYYMMDD}
            v = _date_replace (v, dt);
            # pattern expand
            v = ','.join (pattern.pattern_expand (v));

            d['value'] = v;
            r.append (d);
            
        # step 4: build xml node
        node = XT.Element (['reader', 'writer'][io]);
        e = XT.Element ('plugin');
        e.text = name;
        node.append (e);
        for p in r:
            e = XT.Element ('param', p);
            node.append (e);
        return node;
        
#################################################################
        
def entry_load (entryfile, job_conf_path):
    #dir = os.path.abspath (entryfile);
    #dir = os.path.dirname (dir);
    #dir = os.path.join (dir, '..');
    #dir = os.path.abspath (dir);
    
    global ENTRIES;
    r = _load_xml_safe (entryfile);
    if r == None:
	return -1;
    for e in r:
        try:
            x = int (e.findtext ('no'));
            f = os.path.join (job_conf_path, e.findtext ('param_file'));
            if x and f:
                ENTRIES[x] = [f, None];
        except:
            print_exc ();
    if not ENTRIES:
        return -1;
    return 0;

def storage_load (stfile):
    global STORAGES;
    r = _load_xml_safe (stfile);
    if r == None:
	return -1;
    for e in r:
        try:
            s = Storage (e);
            STORAGES[s.alias] = s;
        except:
            print_exc ();
    if not STORAGES:
        return -1;
    return 0;
    
def blueprint_load (tplfile):
    global BLUEPRINTS;
    r = _load_xml_safe (tplfile);
    if r == None:
	return -1;
    for e in r:
        try:
            t = Blueprint (e);
            BLUEPRINTS[t.dbtype] = t;
        except:
            print_exc ();
    if not BLUEPRINTS:
        return -1;
    return 0;
    
def job_load  (jid):
    global ENTRIES;
    x = ENTRIES[jid];
    if x[1] != None:
        return;
        
    r = _load_xml_safe (x[0]);
    if r == None:
	return -1;
    d = {};
    for e in r:
        try:
            w = Work (e);
            d[w.id] = w;
        except:
            print_exc ();
        
    ENTRIES[jid][1] = d;
    
def job_load_all ():
    for jid in ENTRIES.keys():
        job_load (jid);
    
def work_finalize (jid, wid, work_date):
    job_load (jid);
    jobs = ENTRIES[jid][1];
    w = jobs[wid];
    if not w.enable:
        return None;
        
    wrd = None;
    wwt = None;
    try:
        st = STORAGES[w.source];
        bp = BLUEPRINTS[st.dbtype];
        wrd = bp.combine (w, st, work_date, 0);
        st = STORAGES[w.sink];
        bp = BLUEPRINTS[st.dbtype];
        wwt = bp.combine (w, st, work_date, 1);
    except:
        print_exc ();
    
    if wrd == None and wwt == None:
        return None;
    
    node = XT.Element ('job');
    if wrd != None:
        node.append (wrd);
    if wwt != None:
        node.append (wwt);
    return node;
    
def work_finalize_all (jid, work_date):
    job_load_all ();
    ws = [];
    jobs = ENTRIES[jid][1];
    for wid in jobs.keys ():
        n = work_finalize (jid, wid, work_date);
        if n != None:
            ws.append ((jid, wid, n));
    return ws;
    
def to_file (xfile, work_date, *nodes):
    now = datetime.now ().isoformat();
    sdt = work_date.strftime ('%Y-%m-%d');
    root = XT.Element ('jobs');
    root.append (XT.Comment (' This is an automatic generated file at %s .' % now))
    root.append (XT.Comment (' DO NOT modify this file unless you are certain about your action .'))
    root.append (XT.Comment (' The work date of this file is %s .' % sdt))
    for node in nodes:
        root.append (node);
        
    et = XT.ElementTree (root);
    et.write (xfile, encoding='utf-8', xml_declaration=True);
    # optional step, pretty print xml
    x = xml.dom.minidom.parse (xfile);
    with open (xfile, "w") as f:
        x.writexml (f, indent="", addindent="  ", newl="\n");

def generate_files (jid, wid, work_date, filedir):
    if jid <= 0:
        jids = list (ENTRIES.keys ());
        wid = 0;
        job_load_all ();
    else:
        jids = [jid];
    
    r = [];
    for j in jids:
        if wid <= 0:
            r += work_finalize_all (j, work_date);
        else:
            w = work_finalize (jid, wid, work_date);
            if w != None:
                r.append ((jid, wid, w));
    
    files = [];
    for jid, wid, node in r:
        xfile = os.path.join (filedir, '%02d_%03d.xml' % (jid, wid));
        to_file (xfile, work_date, node);
        files.append (xfile);
    return files;
        
BASE_CONF_LOADED_FLAG = 0;
def load_base_config (tpl_file, entry_file, job_conf_path, *db_files):
    global BASE_CONF_LOADED_FLAG;
    if not BASE_CONF_LOADED_FLAG:
        if entry_load (entry_file, job_conf_path) < 0:
            print ("[ERROR] can not load entry file '%s'." % entry_file, file=sys.stderr);
            return -1;
        for dbfile in db_files:
            if storage_load (dbfile) < 0:
                print ("[ERROR] can not load DB file '%s'." % dbfile, file=sys.stderr);
                return -1;
        if blueprint_load (tpl_file) < 0:
            print ("[ERROR] can not load template file '%s'." % tpl_file, file=sys.stderr);
            return -1;
        BASE_CONF_LOADED_FLAG = 1;
    return 0;

# for quick usage
def generate_files_with_config (jid, wid, work_date, gen_file_dir, tpl_file, entry_file, job_conf_path, *db_files):
    if load_base_config (tpl_file, entry_file, job_conf_path, *db_files) < 0:
        return None;
    return generate_files (jid, wid, work_date, gen_file_dir);
        
#################################################################
#if __name__ == '__main__':
#    from datetime import date
#    DIR = 'config/'
#    generate_files_with_config (0, 0, date.today(), 'gen', 'template.xml',
#                                DIR + 'job_config/datax_job_config.xml',
#                                DIR + 'db_config/ext_db_config.xml',
#                                DIR + 'db_config/load_db_config.xml');
    

