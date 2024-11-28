import os
import fuse
from fuse import FUSE, Operations
import logging

# FUSE Directory Path (the root of the virtual filesystem)
fuse_directory = '/mnt/fuse_directory'  # You should adjust the path based on where you want to mount the FUSE filesystem

# Log configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleFS(Operations):
    def __init__(self):
        # Ensure the FUSE directory exists
        if not os.path.exists(fuse_directory):
            os.makedirs(fuse_directory)
        logger.info(f"SimpleFS is using FUSE directory: {fuse_directory}")
    
    def getattr(self, path):
        """Get file attributes"""
        full_path = os.path.join(fuse_directory, path.strip('/'))
        if not os.path.exists(full_path):
            raise fuse.FuseOSError(fuse.ENOENT)
        return os.stat(full_path)
    
    def readdir(self, path, fh):
        """List the contents of a directory"""
        logger.info(f"Reading directory: {path}")
        return ['.', '..', 'received_data.txt']

    def read(self, path, size, offset, fh):
        """Read a file"""
        full_path = os.path.join(fuse_directory, path.strip('/'))
        with open(full_path, 'r') as f:
            f.seek(offset)
            return f.read(size)
    
    def write(self, path, buf, offset, fh):
        """Write to a file"""
        full_path = os.path.join(fuse_directory, path.strip('/'))
        with open(full_path, 'a') as f:
            f.seek(offset)
            written = f.write(buf)
            return written

# Mount the FUSE filesystem
def mount_fuse():
    fuse.FUSE(SimpleFS(), fuse_directory, foreground=True)
    logger.info("FUSE filesystem mounted successfully")

if __name__ == '__main__':
    mount_fuse()
