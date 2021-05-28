import os
import re

class Whitelist:
    def __init__(self, file):
        self.patterns = []
        comment_pattern = re.compile(r'^\s*\#.*')
        for line in file.readlines():
            if comment_pattern.match(line):
                continue
            self.patterns.append(re.compile(line.strip()))

    def includes(self, storage, store, name=None):
        if storage is None:
            path = '/'
        else:
            base = f'/{storage.name}{store.path}'
            if name is not None:
                path = os.path.join(base, name)
            else:
                path = base
        return any([pattern.match(path) is not None for pattern in self.patterns])
