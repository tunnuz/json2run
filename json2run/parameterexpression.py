from postprocessor import *
from parameter import *
import json
import collections
import math
import os

class ParameterExpression(object):
    """A class representing a tree-like pseudo-logical expression composed of several
    kind of nodes, each one with its own parameter-generation capabilities. Each 
    node, aside from producing sets of parameters, can be configured with a number
    of post-processors, whose aim is to modify on-the-fly the set of parameters 
    generated in the nodes below."""

    def_prefix = "--"
    def_separator = " "

    @staticmethod
    def from_string(text):
        """Generates a parameter expression from a JSON string."""
        return ParameterExpression.from_obj(json.loads(text, object_pairs_hook=collections.OrderedDict))

    @staticmethod
    def from_obj(obj):
        """Generates a parameter expression from a JSON parse."""

        # batch definition language: version 1
        if "type" in obj:
            n_type = obj["type"]
            if n_type == "and":
                return And(obj)
            elif n_type == "or":
                return Or(obj)
            elif n_type == "continuous":
                return Continuous(obj)
            elif n_type == "discrete":
                return Discrete(obj)
            elif n_type == "file":
                return File(obj)
            elif n_type == "directory":
                return Directory(obj)
            elif n_type == "flag":
                return Flag(obj)
            else:
                raise ValueError("Unrecognized node type \""+n_type+"\"")
        
        # batch definition language: version 2 (compact, incomplete)
        else:
            name = [k for k in obj.keys() if k is not "match" ][0] 
            
            if "and" in obj and type(obj["and"]) == list:
                if "postprocessors" in obj:
                    return And({ "type": "and", "descendants": obj["and"], "postprocessors": obj["postprocessors"] })
                return And({ "type": "and", "descendants": obj["and"] })
            
            if "or" in obj and type(obj["or"]) == list:
                if "postprocessors" in obj:
                    return Or({ "type": "or", "descendants": obj["or"], "postprocessors": obj["postprocessors"] })
                return Or({ "type": "or", "descendants": obj["or"] })
        
            if "on" in obj:
                if "hammersley" in obj:  
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "hammersley", 
                            "points": obj["hammersley"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )

                if "rounding" in obj:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "rounding", 
                            "round": obj["rounding"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                
                if "rename" in obj:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "renaming", 
                            "rename": obj["rename"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                    
                if "counter" in obj:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "counter",
                            "name": obj["counter"],
                            "init": obj["init"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                    
                if "sort" in obj:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "sorting",
                            "order": obj["sort"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                    
                if "ignore" in obj:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "ignore",
                            "match": obj["ignore"]
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                
                # must be expression
                try:
                    return Processor(
                        PostProcessor.from_obj(
                        { 
                            "type": "expression",
                            "expression": obj[name],
                            "match": None,
                            "result": name
                        }), 
                        ParameterExpression.from_obj(obj["on"])
                    )
                except Exception as e:
                    raise ValueError("Unrecognized postprocessor, %s" % e)
                        
            if type(obj[name]) == list:
                return Discrete({ "name": name, "values": obj[name] })
            
            if type(obj[name]) == str or type(obj[name]) == unicode:
                                
                match = None
                if "match" in obj:
                    match = obj["match"]
                
                path = obj[name]
                if os.path.isdir(path):
                    return Directory({ "name": name, "path": path, "match": match })
                if os.path.isfile(path):
                    return File({ "name": name, "path": path, "match": match })
            if type(obj[name]) == dict or type(obj[name]) == collections.OrderedDict:
                if "step" in obj[name]:
                    return Discrete({ "name": name, "values": obj[name] })
                else:
                    return Continuous({ "name": name, "values": obj[name] })

    @staticmethod
    def format(executable = None, params = [], separator = None, prefix = None, name = True):
        """Format a parameter list with the most common options."""
        ps = []

        if executable != None:
            ps = [executable]

        if separator == None:
            separator = ParameterExpression.def_separator

        if prefix == None:
            prefix = ParameterExpression.def_prefix

        ps.extend(map(lambda p: p.format(separator, prefix), params))
        return " ".join(ps)
        
    def __init__(self, obj = None):
        """Generic initialization for parameter expression.""" 
        
        if (obj != None):
            self.obj = obj
            
        self.value_index = 0

    def next(self):
        """Generates next parameter list."""
        pass
        
    def has_more(self):
        """Checks if a parameter expression can produce more parameters."""
        return False
        
    def has_continuous(self):
        return False
        
    def __str__(self):
        """Prints representation of a parameter expression."""
        return self.__repr__()
        
    def save(self, file):
        """Saves parameter expression on JSON file (reparse)."""
        try:
            f  = open(file, 'w')
            f.write(str(self)+"\n")
            f.close()
        except Exception, e:
            print e
            
    def headers(self):
        """Return headers of this parameter expression."""
        
        h = set()
        self.__init__()
        while self.has_more():
            n = self.next()
            h = h.union(set([p.name for p in n]))
        
        self.__init__()
        return list(h)
        
    def count(self):
        """Count the configurations generated by this parameter expression"""
        pass
            
    def all(self):
        """Return all configurations generated by this parameter expression"""

        # reset
        self.__init__()
        conf = []

        # generate confs
        while self.has_more():
            conf.append(self.next())
                
        # reset, then return confs
        self.__init__()
        return conf
        
    def add_descendant(self, descendant):
        """Adds a descendant to a parameter expression"""
        pass
        
    def remove_descendant(self, name):
        """Remove descendant with specific name."""
        pass
        
    def get_descendant(self, name):
        """Get descendant with specific name."""
        pass
        
    def pop_descendant(self, name):
        """Gets and removes a descendant with a specific name."""

class Processor(ParameterExpression):
    
    def __init__(self, postprocessor = None, subject = None):
        """Generic constructor for Processor nodes, rewire PostProcessor to process parameter lists generated by subject."""
        
        if postprocessor and subject:

            super(Processor, self).__init__(None)
        
            self.postprocessor = postprocessor
            self.subject = subject
        
        # reset subcomponents
        self.subject.__init__()
        self.postprocessor.__init__()
        self.flat_values = []
        
    def has_more(self):
        return self.postprocessor.has_more(self.flat_values) or self.subject.has_more()
        
    def next(self):
        """Generic parameter generation for inner nodes (call subcomponents, then postprocessors)."""
        
        # if postprocessors are inservible
        if not self.postprocessor.has_more(self.flat_values):

            # generate values through subcomponents
            self.flat_values = self.subject.next()
            
            # reset postprocessor (at each new value generated by descendants)
            self.postprocessor.__init__()
            
        # postprocess values (at least once)           
        values = self.postprocessor.process(self.flat_values)
        
        return values

    def has_continuous(self):
        return self.subject.has_continuous()

class Inner(ParameterExpression):
    
    def __init__(self, obj = None):
        """Generic constructor for inner nodes, takes care of initializing descendants, postprocessors and flat values."""
        super(Inner, self).__init__(obj)
        
        if obj != None:
            self.descendants = []
            if "descendants" in obj:
                self.descendants = [ParameterExpression.from_obj(d) for d in obj["descendants"]]

            self.postprocessors = []
            if "postprocessors" in obj:
                self.postprocessors = [PostProcessor.from_obj(p) for p in obj["postprocessors"]]
                
            for p in self.postprocessors:
                p.parent = self
        
        # reset subcomponents
        map(lambda x: x.__init__(), self.descendants)
        map(lambda x: x.__init__(), self.postprocessors)
        self.flat_values = []
            
    def has_more(self):
        """Checks if some postprocessors are not exhausted."""
        return not self._postprocessors_exhausted()
    
    def has_continuous(self):
        return any(map(lambda x: x.has_continuous(), self.descendants))
    
    def _postprocessors_exhausted(self):
        """Checks if all postprocessors are exhausted."""
        return all(map(lambda p: not p.has_more(self.flat_values), self.postprocessors))
        
    def next(self):
        """Generic parameter generation for inner nodes (call subcomponents, then postprocessors)."""
        
        # if postprocessors are inservible
        if self._postprocessors_exhausted():

            # generate values through subcomponents
            self._gen_values()
            
            # reset postprocessor (at each new value generated by descendants)
            map(lambda p: p.__init__(), self.postprocessors)
            
        # postprocess values (at least once)           
        values = self.flat_values
        for p in self.postprocessors:
            values = p.process(values)
        
        return values
        
    def add_descendant(self, descendant):
        """Adds a descendant to an inner node, resets the generation."""
        self.descendants.append(descendant)
        self.__init__()

    def remove_descendant(self, name):
        """Removes the specified descendant."""
        self.descendants = [d for d in self.descendants if "name" not in dir(d) or d.name != name]
        self.__init__()
        
    def get_descendant(self, name):
        """Gets the specified descendant."""
        selected = [d for d in self.descendants if "name" in dir(d) and d.name == name]
        return selected[0] if len(selected) else None
        
    def pop_descendant(self, name):
        """Pops a descendant and removes it from the parameter expression."""
        selected = self.get_descendant(name)
        if selected != None:
            self.remove_descendant(name)
        return selected

class And(Inner):
    """Generates a Cartesian product of descendants' parameters."""

    def __init__(self, obj = None):
        """Resets the values generated in subcomponents."""
        
        super(And, self).__init__(obj)
        self.values = [None]*len(self.descendants)
    
    def _gen_values(self):
        """And-specific value generation."""
        
        # generate points since last index to the end
        while self.value_index < len(self.descendants)-1:
            self.values[self.value_index] = self.descendants[self.value_index].next()
            self.value_index += 1
        
        # this is outside the while because we don't want to increase self.value_index
        self.values[self.value_index] = self.descendants[self.value_index].next()
        
        # go back until a non-exhausted descendant is found, resetting generation
        while self.value_index > -1 and not self.descendants[self.value_index].has_more():
            self.descendants[self.value_index].__init__()
            self.value_index -= 1
            
        self.flat_values = [p for d_values in self.values for p in d_values]

    def has_more(self):
        """Checks if subcomponents or postprocessors have more."""
        return super(And, self).has_more() or self.value_index != -1    
        
    def count(self):
        from_descendants = reduce(lambda x,y: x*y, [d.count() for d in self.descendants])
        return reduce(lambda x, y: x*y, [p.count() for p in self.postprocessors], from_descendants)

    def __repr__(self):        
        postprocessors = (', "postprocessors": [' + ",".join([p.__repr__() for p in self.postprocessors]) + ' ]') if len(self.postprocessors) else ""
        descendants = (', "descendants": [' + ",".join([p.__repr__() for p in self.descendants]) + ' ]') if len(self.descendants) else ""
        
        return json.dumps(json.loads('{ "type": "and" ' + postprocessors + descendants + ' }', object_pairs_hook=collections.OrderedDict), indent = 4)
        
class Or(Inner):
    """Generates alternative descendants' parameters."""
    
    def __init__(self, obj = None):
        """Does whatever its superclass' constructor does."""
        super(Or, self).__init__(obj)
    
    def _gen_values(self):
        """Or-specific parameter generation."""
        
        # generate next value for current descendant
        self.flat_values = self.descendants[self.value_index].next()
        
        # advance if descendant is exhausted
        if not self.descendants[self.value_index].has_more():
            self.value_index += 1
    
    def has_more(self):
        """Until there are more descendants, don't check if they have more, so they are run at least once."""
        return super(Or, self).has_more() or self.value_index < len(self.descendants)
    
    def count(self):
        from_descendants = reduce(lambda x,y: x+y, [d.count() for d in self.descendants])
        return reduce(lambda x, y: x*y, [p.count() for p in self.postprocessors], from_descendants)

    def __repr__(self):    
        postprocessors = (', "postprocessors": [' + ",".join([p.__repr__() for p in self.postprocessors]) + ' ]') if len(self.postprocessors) else ""
        descendants = (', "descendants": [' + ",".join([p.__repr__() for p in self.descendants]) + ' ]') if len(self.descendants) else ""
        return json.dumps(json.loads('{ "type": "or" ' + postprocessors + descendants + ' }', object_pairs_hook=collections.OrderedDict), indent = 4)

class Leaf(ParameterExpression):
    
    def __init__(self, obj = None):
        
        super(Leaf, self).__init__(obj)
        
        if obj != None:
            self.name = obj["name"]
            self.separator = obj["separator"] if "separator" in obj else None
            self.prefix = obj["prefix"] if "prefix" in obj else None
            
    def has_continuous(self):
        return False

class Continuous(Leaf):
    """Generates an interval parameter to be later post-processed."""

    def __init__(self, obj = None):
        """Sets parameter's extrema and step."""
        
        super(Continuous, self).__init__(obj)

        if obj != None:
            self.min_v = float(obj["values"]["min"])
            self.max_v = float(obj["values"]["max"])
        
    def next(self):
        """Produces incomplete parameter definition, to be postprocessed later."""
        return [IntervalParameter(self.name, self.min_v, self.max_v, self.separator, self.prefix)]
    
    def has_continuous(self):
        return True
    
    def count(self):
        return 1

    def __repr__(self):
        
        sep = ""
        if self.separator:
            sep = '"separator": "%s", ' % self.separator
        
        pre = ""
        if self.separator:
            pre = '"prefix": "%s", ' % self.prefix
            
        return '{ "type": "continuous", "name": "'+ self.name +'", ' + sep + pre +'"values": { "min": '+ str(self.min_v) + ', "max": '+ str(self.max_v) +' } }'
        

class Discrete(Leaf):
    """Generates a parameter with a discrete set of values."""

    def __init__(self, obj = None):
        """Sets parameter's values."""
        
        super(Discrete, self).__init__(obj)

        if obj != None:            
            if "values" in obj:
                if "min" in obj["values"]:
                    self.explicit = False
                    self.min_v = float(obj["values"]["min"]) 
                    self.max_v = float(obj["values"]["max"])            
                    self.step = float(obj["values"]["step"])
                    self.values = [v for v in frange(self.min_v, self.max_v, self.step)]
                else:
                    self.explicit = True
                    self.values = obj["values"] # map(to_intrinsic_type, obj["values"])
            else:
                self.values = []
    
    def next(self):
        self.value_index += 1
        return [Parameter(self.name, self.values[self.value_index-1], self.separator, self.prefix)]
        
    def has_more(self):
        return self.value_index < len(self.values)
    
    def count(self):
        return len(self.values)

    def add_value(self, value):
        """Adds a value to the list"""
        self.values.append(value)

    def __repr__(self):        
        
        sep = ""
        if self.separator != None:
            sep = '"separator": "%s", ' % self.separator
        
        pre = ""
        if self.prefix != None:
            pre = '"prefix": "%s", ' % self.prefix
        
        repr = '{ "type": "discrete", "name": "'+ self.name +'", '+ sep + pre +'"values": '
        if self.explicit:

            repr += '['+ ",".join(map(lambda x: to_json_compatible(x), self.values)) +']'
        else:
            repr += '{ "min": '+ str(self.min_v) +', "max": '+ str(self.max_v) +', "step": '+ str(self.step) +' }'
        repr += '}'
        return repr

class Directory(Discrete):
    """A leaf node which generates values from the contents of a directory which also match a pattern."""
    
    def __init__(self, obj = None):
        """Generates a list of files."""
                
        super(Discrete, self).__init__(obj)

        self.explicit = True
        
        if obj != None:
            self.values = []
            self.name = obj["name"]
            self.path = obj["path"]
            
            if obj["match"]:
                self.match = re.compile(obj["match"])
            else:
                self.match = None
            
            if os.path.isdir(self.path):
                for file in os.listdir(self.path):
                    if os.path.isfile(os.path.join(self.path, file)):
                        if (self.match and self.match.match(file) != None) or not self.match:
                            self.values.append(os.path.join(self.path, file))

            if not self.values:
                raise ValueError("No values generated, check your JSON or %s." % self.path)
                    
class File(Discrete):
    """A leaf node which generates values from the content of a file."""
    
    def __init__(self, obj = None):
        """Generates a list of files."""
        
        super(Discrete, self).__init__(obj)

        self.explicit = True       
 
        if obj != None:
            self.values = []
            self.name = obj["name"]
            self.path = obj["path"]
            
            if obj["match"]:
                self.match = re.compile(obj["match"])
            else:
                self.match = None            
            
            if os.path.isfile(self.path):
                file = open(self.path, "r")
                for line in file:
                    if (self.match and self.match.match(line.rstrip()) != None) or not self.match:
                        self.values.append(line.rstrip())
                        
            if not self.values:
                raise ValueError("No values generated, check your JSON or %s." % self.path)



class Flag(Leaf):
    """Generates a flag, e.g. --verbose"""
    
    def __init__(self, obj = None):
        """Sets flag's name."""
        
        if obj != None:
            super(Flag, self).__init__(obj)
            self.name = obj["name"]
            
    def next(self):
        return [Parameter(self.name, None, None, self.separator, self.prefix)]

    def count(self):
        return 1
        
    def __repr__(self):
        return '{ "type": "flag", "name": "'+ self.name +'" }'
        
def frange(min_v, max_v, step):
    """Generator, equivalent of xrange for floats."""
    while min_v <= max_v:
        yield min_v
        min_v += step
