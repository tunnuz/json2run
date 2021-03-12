from __future__ import print_function
import re
from math import *
import json

from . parameter import *

class PostProcessor(object):
    """A class which takes a list of generated parameters and transforms them."""

    @staticmethod
    def from_obj(obj):
        """Generates postprocessor from JSON object."""
    
        # dispatch postprocessor type
        if "type" in obj:
            n_type = obj["type"]
            if n_type == "ignore":
                return Ignore(obj)
            if n_type == "sorting":
                return Sort(obj)
            elif n_type == "rounding":
                return Rounding(obj)
            elif n_type == "renaming":
                return Rename(obj)
            elif n_type == "hammersley":
                return Hammersley(obj)
            elif n_type == "expression":
                return Expression(obj)
            elif n_type == "counter":
                return Counter(obj)
            else:
                raise ValueError("Unrecognized post-processor type \""+n_type+"\"")

    def process(self, params):
        """Default behaviour, return list of processed params."""        
        return [self._process(p) for p in params]

    def _process(self, param):
        """Default behaviour, processing is identity."""
        return param 

    def has_more(self, params = None):
        """Checks if postprocessor can issue more parameters."""
        return False
        
    def count(self):
        return 1

    def __init__(self, obj = None):
        """Constructor, stores original object."""
        if obj != None:
            self.obj = obj 

class Ignore(PostProcessor):
    """A post-processor that ignores whatever matches the regex."""

    def __init__(self, obj = None):
        """Constructor, compiles the regex for future use."""
        
        super(Ignore,self).__init__(obj)
        
        if obj != None:        
            self.pattern = re.compile(obj["match"])

    def process(self, params):
        return [p for p in params if self.pattern.match(p.name) == None]
        
    def __repr__(self):
        return '{ "type": "ignore", "match": "' + self.pattern.pattern + '" }'


class Sort(PostProcessor):
    """A post-processor that sorts the specified parameters according to a list."""

    def __init__(self, obj = None):
        """Constructor, takes an ordered list of parameters."""
        
        super(Sort,self).__init__(obj)
        
        if obj != None:        
            self.order = obj["order"]

    def process(self, params):
        sorted = []
        
        for sp in self.order:
            sorted.extend([p for p in params if p.name == sp])
        sorted.extend([p for p in params if p.name not in self.order])
        
        return sorted
        
    def __repr__(self):
        return '{ "type": "sorting", "order": [' + ",".join(map(lambda x: '"'+str(x)+'"', self.order)) + '] }'

class Rename(PostProcessor):
    """A post-processor that renames a field."""

    def __init__(self, obj = None):
        """Constructor, sets the old and new name.""" 
        
        super(Rename,self).__init__(obj)

        if obj != None:
            self.renames = {}
            if "rename" in obj:
                for old in obj["rename"]:
                    self.renames[str(old)] = str(obj["rename"][old])
            else:
                self.renames[obj["old"]] = str(obj["new"])
            

    def _process(self, param):
        """Renames matching parameters."""
        
        for r in self.renames:
            if param.name == r:
                param.name = self.renames[r] 
            
        return param
                
    def __repr__(self):
        repr = '{ "type": "renaming", "rename": {'
        rename = ['"'+r+'": "'+str(self.renames[r])+'"' for r in self.renames ]
        repr += ", ".join(rename)
        repr += '} }'
        return repr

class Rounding(PostProcessor):
    """A postprocessor to round a number down to a number of decimal digits."""
    
    def __init__(self, obj = None):
        """Constructor, sets the precision."""
        
        super(Rounding, self).__init__(obj)
        
        if obj != None:
            self.rounding = {}
            if "round" in obj:
                for match in obj["round"]:
                    self.rounding[re.compile(match)] = int(obj["round"][match])
            else:
                self.rounding[re.compile(obj["match"])] = int(obj["decimal_digits"])
                
            if "force_precision" in obj:
                self.force_precision = bool(obj["force_precision"])
            else:
                self.force_precision = False
        
    def _process(self, param):
        """Truncates the value of the matching parameters to a certain precision"""
        
        param_value = floor(float(param.value) * pow(10, self.decimal_digits) + 0.5) / pow(10, self.decimal_digits)
        if self.decimal_digits != 0:
            if self.force_precision:
                    param_value = ("{:."+ str(self.decimal_digits) +"f}").format(param_value)
        else:
            param_value = str(int(param_value))
                
        return Parameter(param.name, param_value, param.separator, param.prefix)
        
    def process(self, params):
        """Matches each param to each pattern and process if needed."""
        
        newconf = []
        for p in params:
            for r in self.rounding:
                if r.match(p.name) != None:
                    self.decimal_digits = self.rounding[r]
                    p = self._process(p)
            
            newconf.append(p)
        
        return newconf
        
    def __repr__(self):
        repr = '{ "type": "rounding", "force_precision": '+str(self.force_precision).lower()+',"round": {'
        roundings = ['"'+r.pattern+'": '+str(self.rounding[r]) for r in self.rounding ]
        repr += ", ".join(roundings)
        repr += '} }'
        return repr

class Counter(PostProcessor):
    """A postprocessor to generate an unique incremental index to each generated configuration."""

    def __init__(self, obj = None):
        """Constructor, initializes counter"""

        super(Counter, self).__init__(obj)

        if obj != None:
            self.name = obj["name"]

            if "init" in obj:
                self.init = int(obj["init"])
            else:
                self.init = 0

            self.counter = self.init


    def __repr__(self):
        return '{ "type": "counter", "name": '+ str(self.name) +', "init": '+ str(self.init) +' }'

    def process(self, params):

        params.append(Parameter(self.name, self.counter))
        self.counter += 1
        return params

class Hammersley(PostProcessor):
    """A postprocessor to generate the Hammersley point set in a d-dimensional interval."""
    
    def __init__(self, obj = None):
        """Constructor, sets the size of the point set."""
        
        super(Hammersley, self).__init__(obj)
        
        # If just constructing
        if obj != None:
            self.points = float(obj["points"])

        self.sampled = 0
    
    def process(self, params):
        """Sort out interval parameters for later processing."""
        
        intervals, other = Hammersley.__partition(params)
        
        if len(intervals) == 0:
            return params
                
        # only advance postprocessor if we have some intervals to sample on
        sample = self.__point(self.sampled+1, len(intervals))
        self.sampled += 1
        
        for i in range(len(intervals)):
            interval = intervals[i]
            scaled = interval.min_v + (interval.max_v - interval.min_v) * sample[i]
            other.append(Parameter(interval.name, scaled, interval.separator, interval.prefix))
        
        return other
    
    def has_more(self, params):
        if (any(map(lambda p: isinstance(p, IntervalParameter), params))) and self.sampled < self.points:
            return True
        return False

    def count(self):
        if self.parent.has_continuous():
            return self.points
        else:
            return 1
    
    @staticmethod
    def __partition(params):
        """Partitions a parameter list into intervals and non-intervals."""
        intervals, other = [], []
        for p in params:
            (intervals if isinstance(p, IntervalParameter) else other).append(p)
        return [intervals, other]
    
    def __point(self, k, d):
        """Generates the k^{th} Hammersley point set's point of dimension d."""
        
        point = []
        point.append(float(k) / float(self.points))
        
        for i in range(d-1):
            p = Hammersley.primes[i]
            pi, ki, phi = float(p), float(k), 0.0
            
            while ki > 0.0:
                a = float(int(ki) % int(p))
                phi += a / pi
                ki = int(ki / p)
                pi *= float(p)
                
            point.append(phi)
            
        return point    
    
    primes = [2,3,5,7,9,11,13,17,19,23,25,29,31,37,41,43,47,49,53,59,61,67,
             71,73,79,83,89,97,101,103,107,109,113,121,127,131,137,139,149,
             151,157,163,167,169,173,179,181,191,193,197,199,211,223,227,229,
             233,239,241,251,257,263,269,271,277,281,283,289,293,307,311,313,
             317,331,337,347,349,353,359,361,367,373,379,383,389,397,401,409,
             419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,503,
             509,521,523,529,541,547,557,563,569,571,577,587,593,599,601,607,
             613,617,619,631,641,643,647,653,659,661,673,677,683,691,701,709,
             719,727,733,739,743,751,757,761,769,773,787,797,809,811,821,823,
             827,829,839,841,853,857,859,863,877,881,883,887,907,911,919,929,
             937,941,947,953,961,967,971,977,983,991,997]
    """Prime numbers used as seed for Hammersley points generation."""
    
    def __repr__(self):
        return '{ "type": "hammersley", "points": '+ str(self.points) +' }'
        
class Expression(PostProcessor):
    """Extremely generic postprocessor, take a capture pattern and an expression and generates a new parameter
    using the captured parameters as values in the expression. Supports every parameter name, not only the
    identifier allowed by python. """
    
    def __init__(self, obj = None):
        super(Expression, self).__init__(obj)
        
        if obj != None:
            
            if obj["match"]:
                self.pattern = re.compile(obj["match"])
            else:
                self.pattern = None
            self.result = obj["result"]
            
            if "expression" in obj:
                self.expression = obj["expression"]
                self.interval = False
            else:
                self.min = obj["min"]
                self.max = obj["max"]
                self.interval = True
            
            self.separator = obj["separator"] if "separator" in obj else None
            self.prefix = obj["prefix"] if "prefix" in obj else None
            
    def process(self, params):
        """Process list of parameters."""
        
        # localize captured parameters (previously done by writing locals(), now changed to support arbitrary variable names)
        if self.pattern:
            captured = { p.name: p for p in params if self.pattern.match(p.name) != None}
        else:
            captured = { p.name: p for p in params }
                
        # compute result, add new parameter
        try:
            # remove old parameter, if same name
            if self.pattern and (self.pattern.match(self.result)):
                params = filter(lambda x: x.name != self.result, params)
            
            
            if not self.interval:
                
                try:
                    expression = self.expression
                    for p in captured:
                        expression = re.sub(re.compile("%s\." % p), "captured[\"%s\"]." % p, expression)
                    
                    result = eval(expression)
                    params.append(Parameter(self.result, result, self.separator, self.prefix))
                except:
                    pass
                
            else:
                
                min_e = self.min
                max_e = self.max
                
                for p in captured:
                    min_e = re.sub(re.compile("%s\." % p), "captured[\"%s\"]." % p, min_e)
                    max_e = re.sub(re.compile("%s\." % p), "captured[\"%s\"]." % p, max_e)
                
                min_v = eval(min_e)
                max_v = eval(max_e)
                
                params.append(IntervalParameter(self.result, min_v, max_v, self.separator, self.prefix))
                    
        except Exception as e:
            print(e)
        
        pre = ""
        if self.prefix:
            pre = '"prefix": "%s", ' % self.prefix
        
        if not self.interval:
            return '{ "type": "expression", '+ sep + pre + ' "match": "'+self.pattern.pattern+'", "expression": "'+self.expression+'", "result": "' + self.result + '" }'
        else:
            return '{ "type": "expression", '+ sep + pre + ' "match": "'+self.pattern.pattern+'", "min": "'+self.min+'", "max": "'+self.max+'", "result": "' + self.result + '" }'
