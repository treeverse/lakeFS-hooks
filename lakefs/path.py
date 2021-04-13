DEFAULT_PATH_SEPARATOR = '/'
DEFAULT_EXTENSION_SEPARATOR = '.'


class Path:
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
    def extension(self):
        _, sep, ext = self.base_name.rpartition(DEFAULT_EXTENSION_SEPARATOR)
        return ext if sep == DEFAULT_EXTENSION_SEPARATOR else ''
