import pycassa

def monkeyinit(self, default=None, required=True):
    self.default = default
    self.required = required
pycassa.types.Column.__init__ = monkeyinit

for key, value in pycassa.types.__dict__.items():
    try:
        if type(value) == type and issubclass(value, pycassa.types.Column):
            globals()[key] = value
    except:
        pass