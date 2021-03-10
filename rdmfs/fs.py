from datetime import datetime
import os
import sys
import stat
import logging
import errno
import traceback
import pyfuse3
import pyfuse3_asyncio
from . import node

log = logging.getLogger(__name__)

class RDMFileSystem(pyfuse3.Operations):
    def __init__(self, osf, project):
        super(RDMFileSystem, self).__init__()
        self.osf = osf
        self.project = project
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self.path_inodes = {}
        self.file_handlers = {}

    async def getattr(self, inode, ctx=None):
        try:
            log.info('getattr: inode={inode}'.format(inode=inode))
            entry = pyfuse3.EntryAttributes()
            storage, store = await self._find_by_inode(inode)
            if hasattr(store, 'files') or hasattr(store, 'storages'):
                entry.st_mode = (stat.S_IFDIR | 0o755)
                entry.st_size = 0
            else:
                entry.st_mode = (stat.S_IFREG | 0o644)
                log.info('getattr: name={}, size={}'.format(store.name, store.size))
                if store.size is not None:
                    entry.st_size = store.size
                else:
                    entry.st_size = 0
            stamp = 0
            mstamp = stamp
            if hasattr(store, 'date_created') and store.date_created is not None:
                stamp = self._fromisoformat(store.date_created)
            if hasattr(store, 'date_modified') and store.date_modified is not None:
                mstamp = self._fromisoformat(store.date_modified)
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = stamp
            entry.st_gid = os.getgid()
            entry.st_uid = os.getuid()
            entry.st_ino = inode
            return entry
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    def _fromisoformat(self, datestr):
        datestr = datestr.replace('Z', '+00:00')
        return int(datetime.fromisoformat(datestr).timestamp() * 1e9)

    async def _get_osfproject(self):
        if self.osfproject is not None:
            return self.osfproject
        self.osfproject = await self.osf.project(self.project)
        return self.osfproject

    async def _find_storage_by_inode(self, inode):
        if inode not in self.path_inodes:
            return None
        path, _ = self.path_inodes[inode]
        if len(path) > 1:
            return None
        osfproject = await self._get_osfproject()
        return await osfproject.storage(path[0])

    async def _find_file_by_inode(self, inode):
        if inode not in self.path_inodes:
            return None, None
        path, _ = self.path_inodes[inode]
        return await self._get_file(path)

    async def _get_file(self, path):
        if len(path) == 1:
            osfproject = await self._get_osfproject()
            storage = await osfproject.storage(path[0])
            return storage, storage
        storage, parent = await self._get_file(path[:-1])
        log.info('_get_file: path={}, parent={}'.format(path, parent))
        async for file_ in self._get_files(parent):
            if file_.name == path[-1]:
                log.info('_get_file: name={}'.format(file_.name))
                return storage, file_
        log.error('not found: name={}'.format(path[-1]))
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def _get_files(self, parent):
        if hasattr(parent, 'child_files'):
            async for f in parent.child_files:
                yield f
            async for f in parent.child_folders:
                yield f
        else:
            async for f in parent.files:
                yield f
            async for f in parent.folders:
                yield f

    def _register_new_inode(self, path, file_path):
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self.path_inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self.path_inodes[new_inode] = (path, file_path)
        return new_inode

    def _get_storage_inode(self, storage):
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) == 1 and path[0] == storage.name:
                return inode
        return self._register_new_inode([storage.name], None)

    def _get_file_inode(self, storage, file_):
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) > 1 and path[0] == storage.name and file_path == file_.path:
                return inode
        log.info('_get_file_inode, path={}'.format(file_.path))
        path_segments = file_.path.strip('/').split('/')
        return self._register_new_inode([storage.name] + path_segments, file_.path)

    async def _find_by_inode(self, inode):
        if inode == pyfuse3.ROOT_INODE:
            return None, await self._get_osfproject()
        store = await self._find_storage_by_inode(inode)
        if store is not None:
            return store, store
        storage, store = await self._find_file_by_inode(inode)
        if store is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return storage, store

    async def lookup(self, parent_inode, bname, ctx=None):
        try:
            name = bname.decode('utf8')
            log.info('lookup parent_inode={parent_inode}, name={name}'.format(
                parent_inode=parent_inode, name=name
            ))
            if parent_inode == pyfuse3.ROOT_INODE:
                # Storages
                osfproject = await self._get_osfproject()
                storage = await osfproject.storage(name)
                inode = self._get_storage_inode(storage)
                return await self.getattr(inode)
            # Files
            storage, store = await self._find_by_inode(parent_inode)
            if store is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if not hasattr(store, 'files'):
                raise pyfuse3.FUSEError(errno.ENOENT)
            target = None
            async for file_ in self._get_files(store):
                if file_.name == name:
                    target = file_
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            inode = self._get_file_inode(storage, target)
            return await self.getattr(inode)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def opendir(self, inode, ctx):
        log.info('opendir: inode={inode}'.format(inode=inode))
        try:
            if inode == pyfuse3.ROOT_INODE:
                osfproject = await self._get_osfproject()
                return self._get_node_fh(node.Project(self, osfproject))
            if inode in self.path_inodes:
                storage, store = await self._find_by_inode(inode)
                log.info('find_by_inode: storage={}, folder={}'.format(storage, store))
                return self._get_node_fh(node.Folder(self, storage, store))
            raise pyfuse3.FUSEError(errno.ENOENT)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    def _find_node_by_fh(self, fh):
        return self.file_handlers[fh]

    def _get_node_fh(self, node):
        new_fh = None
        for fh in range(self.offset_inode, sys.maxsize):
            if fh not in self.file_handlers:
                new_fh = fh
                break
        if new_fh is None:
            raise ValueError('Cannot allocate new handler')
        self.file_handlers[new_fh] = node
        return new_fh

    def _release_fh(self, fh):
        if fh not in self.file_handlers:
            return
        del self.file_handlers[fh]

    async def readdir(self, fh, start_id, token):
        log.info('readdir: fh={fh}, start_id={start_id}'.format(
            fh=fh, start_id=start_id
        ))
        try:
            folder = self._find_node_by_fh(fh)
            assert folder is not None
            await folder.readdir(start_id, token)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)
        return

    async def setxattr(self, inode, name, value, ctx):
        log.info('setxattr')
        if inode != pyfuse3.ROOT_INODE or name != b'command':
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        if value == b'terminate':
            pyfuse3.terminate()
        else:
            raise pyfuse3.FUSEError(errno.EINVAL)

    async def open(self, inode, flags, ctx):
        try:
            log.info('open: inode={inode}'.format(inode=inode))
            if inode not in self.path_inodes:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if flags & os.O_RDWR or flags & os.O_WRONLY:
                raise pyfuse3.FUSEError(errno.EACCES)
            storage, store = await self._find_by_inode(inode)
            return pyfuse3.FileInfo(fh=self._get_node_fh(node.File(self, storage, store)))
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def read(self, fh, off, size):
        try:
            log.info('read: fh={fh}'.format(fh=fh))
            file_ = self._find_node_by_fh(fh)
            assert file_ is not None
            return await file_.read(off, size)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def release(self, fh):
        log.info('release')
        file_ = self._find_node_by_fh(fh)
        assert file_ is not None
        await file_.close()
        self._release_fh(fh)

    async def releasedir(self, fh):
        log.info('releasedir')
        file_ = self._find_node_by_fh(fh)
        assert file_ is not None
        await file_.close()
        self._release_fh(fh)
