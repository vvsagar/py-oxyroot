from .oxyroot import *

__doc__: str = oxyroot.__doc__
__version__: str = oxyroot.version()

if hasattr(oxyroot, "__all__"):
    __all__ = oxyroot.__all__
