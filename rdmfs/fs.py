from datetime import datetime
import json
import os
import sys
import stat
import logging
import errno
import time
import traceback
import pyfuse3
import pyfuse3_asyncio
from . import node
from .inode import Inodes, fromisoformat
from .filehandle import FileHandlers

log = logging.getLogger(__name__)

class RDMFileSystem(pyfuse3.Operations):
    def __init__(self, osf, project):
        super(RDMFileSystem, self).__init__()
        self.inodes = Inodes(osf, project)
        self.file_handlers = FileHandlers()

    async def getattr(self, inode, ctx=None):
        try:
            log.info('getattr: inode={inode}'.format(inode=inode))
            entry = pyfuse3.EntryAttributes()
            storage, store = await self.inodes.find_by_inode(inode)
            if hasattr(store, 'files') or hasattr(store, 'storages'):
                entry.st_mode = (stat.S_IFDIR | 0o755)
                entry.st_size = 0
            else:
                entry.st_mode = (stat.S_IFREG | 0o644)
                log.info('getattr: name={}, size={}'.format(store.name, store.size))
                if store.size is not None:
                    entry.st_size = int(store.size)
                else:
                    entry.st_size = 0
            stamp = 0
            mstamp = stamp
            if hasattr(store, 'date_created') and store.date_created is not None:
                stamp = fromisoformat(store.date_created)
            if hasattr(store, 'date_modified') and store.date_modified is not None:
                mstamp = fromisoformat(store.date_modified)
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

    async def lookup(self, parent_inode, bname, ctx=None):
        try:
            name = bname.decode('utf8')
            log.info('lookup parent_inode={parent_inode}, name={name}'.format(
                parent_inode=parent_inode, name=name
            ))
            if parent_inode == pyfuse3.ROOT_INODE:
                # Storages
                osfproject = await self.inodes.get_osfproject()
                storage = await osfproject.storage(name)
                inode = self.inodes.get_storage_inode(storage)
                return await self.getattr(inode)
            # Files
            storage, store = await self.inodes.find_by_inode(parent_inode)
            if store is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if not hasattr(store, 'files'):
                raise pyfuse3.FUSEError(errno.ENOENT)
            target = None
            async for file_ in self.inodes.get_files(store):
                if file_.name == name:
                    target = file_
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            inode = self.inodes.get_file_inode(storage, target)
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
                osfproject = await self.inodes.get_osfproject()
                return self.file_handlers.get_node_fh(node.Project(self, osfproject))
            if self.inodes.exists(inode):
                storage, store = await self.inodes.find_by_inode(inode)
                log.info('find_by_inode: storage={}, folder={}'.format(storage, store))
                return self.file_handlers.get_node_fh(node.Folder(self, storage, store))
            raise pyfuse3.FUSEError(errno.ENOENT)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def readdir(self, fh, start_id, token):
        log.info('readdir: fh={fh}, start_id={start_id}'.format(
            fh=fh, start_id=start_id
        ))
        try:
            folder = self.file_handlers.find_node_by_fh(fh)
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
            if not self.inodes.exists(inode):
                raise pyfuse3.FUSEError(errno.ENOENT)
            if flags & os.O_RDWR or flags & os.O_WRONLY:
                raise pyfuse3.FUSEError(errno.EACCES)
            storage, store = await self.inodes.find_by_inode(inode)
            return pyfuse3.FileInfo(fh=self.file_handlers.get_node_fh(node.File(self, storage, store)))
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def read(self, fh, off, size):
        try:
            log.info('read: fh={fh}'.format(fh=fh))
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            return await file_.read(off, size)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def release(self, fh):
        log.info('release')
        file_ = self.file_handlers.find_node_by_fh(fh)
        assert file_ is not None
        await file_.close()
        self.file_handlers.release_fh(fh)

    async def releasedir(self, fh):
        log.info('releasedir')
        file_ = self.file_handlers.find_node_by_fh(fh)
        assert file_ is not None
        await file_.close()
        self.file_handlers.release_fh(fh)
