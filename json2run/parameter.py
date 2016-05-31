class Parameter(object):
    """Generic named and valued parameter."""

    def __init__(self, name, value=None, separator=None, prefix=None):
        """Initializes a named parameter."""
        self.name = str(name)
        self.value = value
        self.separator = separator
        self.prefix = prefix

    def __repr__(self):
        return '"%s": %s' % (self.name, str(self.value))

    def __hash__(self):
        """Custom hashing."""
        return hash("".join([self.name, str(self.value)]))

    def format(self, separator, prefix):
        """Format parameter for printing."""

        fmt = ""

        if self.separator:
            separator = self.separator

        if self.prefix:
            prefix = self.prefix

        if self.name:
            fmt += prefix + self.name

        if self.value != None:
            if self.name:
                fmt += separator
            fmt += str(self.value)

        return fmt


class ParameterList(list):
    """A list of parameter that can also be hashed for use in sets and dictionaries."""

    def __hash__(self):
        """Compute hash of list as hash of parameters."""

        # sort parameters prior to hashing, so that order doesn't matter
        self.sort(key=lambda p: p.name)
        return hash("".join(map(str, self)))

    def __getitem__(self, field):
        """Subscript getter."""
        for i in self:
            if i.name == field:
                return i
        raise KeyError

    def __eq__(self, other):
        """Equivalence check (for sets)."""
        return hash(self) == hash(other)


class IntervalParameter(Parameter):
    """Specialization of parameter, which handles an interval."""

    def __init__(self, name, min_v, max_v, separator=None, prefix=None):
        """Additionally sets min and max of the interval."""

        super(IntervalParameter, self).__init__(name, None, separator, prefix)
        self.min_v = min_v
        self.max_v = max_v

    def __repr__(self):
        return self.name + "=" + "[" + str(self.min_v) + "," + str(self.max_v) + "]"

    def __hash__(self):
        """Custom hashing."""
        return hash("".join([self.name, str(self.min_v), str(self.max_v)]))


def to_intrinsic_type(s):
    """Translates value to its intrinsic type"""

    print("converting", s, "to its intrinsic type")

    s = str(s)
    if s.isdigit():
        return int(s)
    else:
        try:
            return float(s)
        except:
            if s.lower() == "true":
                return True
            elif s.lower() == "false":
                return False
            elif s.lower() == "none":
                return None
            else:
                return "\"" + s + "\""


def to_json_compatible(s):
    if type(s) == str:
        return "\"%s\"" % s
    elif type(s) == bool:
        return "true" if s == True else "false"
    elif type(s) == float:
        return "%f" % s
    elif type(s) == int:
        return "%d" % s
    else:
        return "\"%s\"" % s
    raise ValueError
