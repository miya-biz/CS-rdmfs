import logging
import sys

log = logging.getLogger(__name__)

class FileHandlers:
    def __init__(self):
        self.file_handlers = {}
        self.offset_fh = 1

    def find_node_by_fh(self, fh):
        return self.file_handlers[fh]

    def get_node_fh(self, node):
        new_fh = None
        for fh in range(self.offset_fh, sys.maxsize):
            if fh not in self.file_handlers:
                new_fh = fh
                break
        if new_fh is None:
            raise ValueError('Cannot allocate new handler')
        self.file_handlers[new_fh] = node
        return new_fh

    def release_fh(self, fh):
        if fh not in self.file_handlers:
            return
        del self.file_handlers[fh]
