import logging
import tempfile
import pyfuse3


log = logging.getLogger(__name__)

class BaseFileContext:
    def __init__(self, context):
        self.context = context
        self.current_id = None
        self.aiterator = None
        self.buffer = None

    async def close(self):
        pass

    async def read(self, offset, size):
        if self.buffer is None:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                await self.write_to(f)
                self.buffer = f.name
        with open(self.buffer, 'rb') as f:
            f.seek(offset)
            return f.read(size)

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
    def __init__(self, context, storage, file_):
        super(File, self).__init__(context)
        self.storage = storage
        self.file_ = file_

    async def write_to(self, fp):
        await self.file_.write_to(fp)
