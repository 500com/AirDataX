# -*- coding: UTF-8 -*-

from __future__ import print_function;

import re

__all__ = ['pattern_expand'];

class Pattern:
    
    E = re.compile (r'{(?:[a-z]\.\.[a-z]|\d+\.\.\d+|0x[0-9a-f]+\.\.0x[0-9a-f]+)(?:\.\.\d+)?}', re.I);
    
    def __init__ (self, source):
        m = Pattern.E.search (source);
        if not m:
            self.head = source
            self.core = [''];
            self.next = None;
            return;
            
        b = m.start ();
        e = m.end ();
        self.head = source[:b];
        self.core = self.compile (source[b+1:e-1]);
        if source[e:]:
            self.next = Pattern (source[e:]);
        else:
            self.next = None;
        
    def expand (self):
        if self.next:
            r = cartesian_product (str.__add__, self.core, self.next.expand ());
        else:
            r = self.core;
        return cartesian_product (str.__add__, [self.head], r);
        
    def compile (self, s):
        parts = s.split ('..');
        beg, end = parts[:2];
        
        step = 1;
        if len (parts) == 3:
            step = int (parts[2]);
            
        if beg.isdigit ():
            return self._compile_decs (beg, end, step);
        elif beg.lower ().startswith ('0x'):
            return self._compile_hexs (beg, end, step);
        else:
            return self._compile_char (beg, end, step);
            
    def _compile_decs (self, beg, end, step):
        b = int (beg, 10);
        e = int (end, 10);
        if len(beg) == len(end):
            # fix length formation
            tpl = '%%0%dd' % len(beg);
        else:
            tpl = '%d';
        return [tpl % x for x in range (b, e + 1, step)];
        
    def _compile_hexs (self, beg, end, step):
        b = int (beg, 16);
        e = int (end, 16);
        if len(beg) == len(end):
            # fix length formation
            tpl = '%%0%dx' % (len(beg) - 2);
        else:
            tpl = '%x';
        return [tpl % x for x in range (b, e + 1, step)];
    
    def _compile_char (self, beg, end, step):
        b = ord (beg);
        e = ord (end);
        return ['%c' % x for x in range (b, e + 1, step)];
            
def cartesian_product (func, iter1, iter2):
    r = [];
    for x in iter1:
        f = lambda z:func (x, z);
        y = map (f, iter2);
        r += y;
    return r;
    
def pattern_expand (s):
    return Pattern (s).expand ();
    
##########################################################
#if __name__ == '__main__':
#    for s in ['xy', 'x{a..g}y', 'x{1..10}y', 'x{01..10}y', 'x{a..g..2}y', 
#                'x{1..10..2}y', 'x{1..3}{a..c}y', 'x{1..a}y}', 
#                'x{0x01..0Xff..11}y', 'x{1..f}y', '{1..3}xy', 'xy{1..3}', 
#                '{1..3}', '']:
#        print ('%-20s' % s, pattern_expand (s));
    
