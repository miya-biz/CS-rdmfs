import io
import logging
import os
import tempfile
import pyfuse3
from aiofile import AIOFile, Reader


log = logging.getLogger(__name__)

def flags_can_write(flags):
    if flags & 0x03 == os.O_RDWR:
        return True
    if flags & 0x03 == os.O_WRONLY:
        return True
    return False

class BaseFileContext:
    def __init__(self, context, flags=None):
        self.context = context
        self.current_id = None
        self.aiterator = None
        self.buffer = None
        self.bufferfile = None
        self.flags = flags
        self.flush_count = 0

    async def _flush(self, fp):
        raise NotImplementedError()

    async def _write_to(self, fp):
        raise NotImplementedError()

    async def _invalidate(self):
        pass

    def is_write(self):
        return flags_can_write(self.flags)

    def is_new_file(self):
        return False

    async def close(self):
        if self.buffer is None and self.is_new_file() and self.is_write():
            await self._ensure_buffer()
        await self.flush()
        self.buffer = None
        await self._invalidate()

    async def read(self, offset, size):
        f = await self._ensure_buffer()
        f.seek(offset)
        return f.read(size)

    async def write(self, offset, buf):
        f = await self._ensure_buffer()
        f.seek(offset)
        f.write(buf)
        return len(buf)

    async def flush(self):
        if self.bufferfile is None:
            return
        self.flush_count += 1
        self.bufferfile.close()
        self.bufferfile = None
        if not self.is_write():
            return
        async with AIOFile(self.buffer, 'rb') as afp:
            reader = Reader(afp, chunk_size=4096)
            #reader.mode = 'rb'
            #reader.peek = lambda x=None: True
            await self._flush(reader)
        os.remove(self.buffer)

    async def readdir(self, start_id, token):
        if self.aiterator is None:
            self.current_id = 0
            self.aiterator = self.__aiter__()
        if start_id != self.current_id:
            return None
        self.current_id += 1
        try:
            object = await self.aiterator.__anext__()
            inode = self.get_inode(object)
            log.info('Result: name={}, inode={}'.format(object.name, inode))
            pyfuse3.readdir_reply(
                token, object.name.encode('utf8'),
                await self.context.getattr(inode),
                self.current_id)
        except StopAsyncIteration:
            log.info('Finished')
            return None

    async def _ensure_buffer(self):
        if self.bufferfile is not None:
            return self.bufferfile
        if self.buffer is None:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                await self._write_to(f)
                self.buffer = f.name
        mode = 'rb'
        if self.flags is None:
            pass
        elif self.flags & 0x03 == os.O_RDWR and self.flags & os.O_APPEND:
            mode = 'a+b'
        elif self.flags & 0x03 == os.O_RDWR:
            mode = 'r+b'
        elif self.flags & 0x03 == os.O_WRONLY and self.flags & os.O_APPEND:
            mode = 'ab'
        elif self.flags & 0x03 == os.O_WRONLY:
            mode = 'wb'
        log.info('buffer: file={2}, flags={0:08x}, mode={1}'.format(
            self.flags, mode, self.buffer
        ))
        self.bufferfile = open(self.buffer, mode)
        return self.bufferfile

class Project(BaseFileContext):
    def __init__(self, context, osfproject):
        super(Project, self).__init__(context)
        self.osfproject = osfproject

    def __aiter__(self):
        return self.osfproject.storages.__aiter__()

    def get_inode(self, storage):
        return self.context.inodes.get_storage_inode(storage)

class Folder(BaseFileContext):
    def __init__(self, context, storage, folder):
        super(Folder, self).__init__(context)
        self.storage = storage
        self.folder = folder

    def __aiter__(self):
        return self._get_folders_and_files().__aiter__()

    def get_inode(self, file):
        return self.context.inodes.get_file_inode(self.storage, file)

    async def _get_folders_and_files(self):
        if self.storage == self.folder:
            async for f in self.storage.child_folders:
                yield f
            async for f in self.storage.child_files:
                yield f
        else:
            async for f in self.folder.folders:
                yield f
            async for f in self.folder.files:
                yield f

class File(BaseFileContext):
    def __init__(self, context, storage, file_, flags):
        super(File, self).__init__(context, flags)
        self.storage = storage
        self.file_ = file_

    async def _write_to(self, fp):
        await self.file_.write_to(fp)

    async def _flush(self, fp):
        await self.file_.update(fp)

    async def _invalidate(self):
        self.context.inodes.clear_inode_cache(self.storage, self.file_.path)

class NewFile(BaseFileContext):
    def __init__(self, context, storage, path, flags):
        super(NewFile, self).__init__(context, flags)
        self.storage = storage
        self.path = path

    def is_new_file(self):
        return True

    async def _write_to(self, fp):
        pass

    async def _flush(self, fp):
        await self.storage.create_file(self.path, fp)

    async def _invalidate(self):
        self.context.inodes.clear_inode_cache(self.storage, self.path)
