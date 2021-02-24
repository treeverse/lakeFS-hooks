

class Path(object):
    DEFAULT_PATH_SEPARATOR = '/'
    DEFAULT_EXTENSION_SEPARATOR = '.'

    def __init__(self, v: str, separator: str = DEFAULT_PATH_SEPARATOR):
        self.path = v
        self.separator = separator
        self.parts = v.split(separator)

    @property
    def base_name(self):
        _, sep, ext = self.path.rpartition(self.separator)
        return ext if sep == self.separator else ''

    @property
    def dir_name(self):
        prefix, sep, _ = self.path.rpartition(self.separator)
        return prefix if sep == self.separator else ''

    @property
    def extension(self, ext_separator: str = DEFAULT_EXTENSION_SEPARATOR):
        _, sep, ext = self.base_name.rpartition(ext_separator)
        return ext if sep == ext_separator else ''
