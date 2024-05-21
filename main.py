class BlockStorage:
    def __init__(self, num_blocks, block_size):
        self.block_size = block_size
        self.num_blocks = num_blocks
        self.blocks = [bytearray(block_size) for _ in range(num_blocks)]
        self.bitmap = [0] * num_blocks

    def allocate_block(self):
        for i in range(self.num_blocks):
            if self.bitmap[i] == 0:
                self.bitmap[i] = 1
                return i
        raise RuntimeError("No free blocks available")

    def free_block(self, block_index):
        if self.bitmap[block_index] == 1:
            self.bitmap[block_index] = 0
            self.blocks[block_index] = bytearray(self.block_size)

    def write_block(self, block_index, data):
        if len(data) > self.block_size:
            raise ValueError("Data size exceeds block size")
        self.blocks[block_index][:len(data)] = data

    def read_block(self, block_index):
        return self.blocks[block_index]

class FileDescriptor:
    def __init__(self, file_type, max_direct_blocks=10):
        self.file_type = file_type
        self.hard_links = 1
        self.size = 0
        self.direct_blocks = [-1] * max_direct_blocks
        self.indirect_block = -1

    def add_block(self, block_index):
        for i in range(len(self.direct_blocks)):
            if self.direct_blocks[i] == -1:
                self.direct_blocks[i] = block_index
                return
        raise RuntimeError("No free direct blocks, indirect block needed")

    def get_blocks(self):
        return [block for block in self.direct_blocks if block != -1]

class FileSystem:
    def __init__(self, num_blocks, block_size, max_files):
        self.block_storage = BlockStorage(num_blocks, block_size)
        self.max_files = max_files
        self.file_descriptors = [None] * max_files
        self.directory = {}
        self.open_files = {}
        self.next_fd = 0

    def mkfs(self, num_descriptors):
        self.__init__(self.block_storage.num_blocks, self.block_storage.block_size, num_descriptors)

    def stat(self, name):
        if name not in self.directory:
            raise FileNotFoundError("File not found")
        fd_index = self.directory[name]
        fd = self.file_descriptors[fd_index]
        return fd

    def ls(self):
        return self.directory

    def create(self, name):
        if name in self.directory:
            raise FileExistsError("File already exists")
        for i in range(self.max_files):
            if self.file_descriptors[i] is None:
                fd = FileDescriptor('regular')
                self.file_descriptors[i] = fd
                self.directory[name] = i
                return
        raise RuntimeError("Maximum number of files reached")

    def open(self, name):
        if name not in self.directory:
            raise FileNotFoundError("File not found")
        fd_index = self.directory[name]
        self.open_files[self.next_fd] = (fd_index, 0)
        self.next_fd += 1
        return self.next_fd - 1

    def close(self, fd):
        if fd in self.open_files:
            del self.open_files[fd]

    def seek(self, fd, offset):
        if fd in self.open_files:
            fd_index, _ = self.open_files[fd]
            self.open_files[fd] = (fd_index, offset)
        else:
            raise FileNotFoundError("File descriptor not found")

    def read(self, fd, size):
        if fd not in self.open_files:
            raise FileNotFoundError("File descriptor not found")
        fd_index, offset = self.open_files[fd]
        file_data = bytearray()
        blocks = self.file_descriptors[fd_index].get_blocks()
        total_size = self.file_descriptors[fd_index].size
        if offset + size > total_size:
            size = total_size - offset
        current_offset = offset
        while size > 0:
            block_index = current_offset // self.block_storage.block_size
            block_offset = current_offset % self.block_storage.block_size
            bytes_to_read = min(size, self.block_storage.block_size - block_offset)
            block_data = self.block_storage.read_block(blocks[block_index])
            file_data.extend(block_data[block_offset:block_offset + bytes_to_read])
            size -= bytes_to_read
            current_offset += bytes_to_read
        self.open_files[fd] = (fd_index, current_offset)
        return file_data.decode()

    def write(self, fd, data):
        if fd not in self.open_files:
            raise FileNotFoundError("File descriptor not found")
        fd_index, offset = self.open_files[fd]
        fd_obj = self.file_descriptors[fd_index]
        current_offset = offset
        while data:
            block_index = current_offset // self.block_storage.block_size
            block_offset = current_offset % self.block_storage.block_size
            if block_index >= len(fd_obj.direct_blocks) or fd_obj.direct_blocks[block_index] == -1:
                new_block = self.block_storage.allocate_block()
                fd_obj.add_block(new_block)
            block_data = data[:self.block_storage.block_size - block_offset]
            self.block_storage.write_block(fd_obj.direct_blocks[block_index], block_data)
            data = data[len(block_data):]
            current_offset += len(block_data)
        fd_obj.size = max(fd_obj.size, current_offset)
        self.open_files[fd] = (fd_index, current_offset)

    def link(self, name1, name2):
        if name1 not in self.directory:
            raise FileNotFoundError("Source file not found")
        if name2 in self.directory:
            raise FileExistsError("Destination file already exists")
        fd_index = self.directory[name1]
        self.file_descriptors[fd_index].hard_links += 1
        self.directory[name2] = fd_index

    def unlink(self, name):
        if name not in self.directory:
            raise FileNotFoundError("File not found")
        fd_index = self.directory.pop(name)
        fd_obj = self.file_descriptors[fd_index]
        fd_obj.hard_links -= 1
        if fd_obj.hard_links == 0 and fd_index not in [desc[0] for desc in self.open_files.values()]:
            for block in fd_obj.get_blocks():
                self.block_storage.free_block(block)
            self.file_descriptors[fd_index] = None

    def truncate(self, name, size):
        if name not in self.directory:
            raise FileNotFoundError("File not found")
        fd_index = self.directory[name]
        fd_obj = self.file_descriptors[fd_index]
        if size > fd_obj.size:
            current_blocks = len(fd_obj.get_blocks())
            blocks_needed = (size + self.block_storage.block_size - 1) // self.block_storage.block_size
            for _ in range(blocks_needed - current_blocks):
                new_block = self.block_storage.allocate_block()
                fd_obj.add_block(new_block)
            fd_obj.size = size
        elif size < fd_obj.size:
            blocks_to_keep = (size + self.block_storage.block_size - 1) // self.block_storage.block_size
            blocks_to_free = fd_obj.get_blocks()[blocks_to_keep:]
            for block in blocks_to_free:
                self.block_storage.free_block(block)
            fd_obj.direct_blocks = fd_obj.direct_blocks[:blocks_to_keep]
            fd_obj.size = size

# Приклад
fs = FileSystem(num_blocks=100, block_size=512, max_files=10)
fs.create('file1.txt')
fd = fs.open('file1.txt')
fs.write(fd, b'Hello, this is a test file.')
fs.seek(fd, 0)
print(fs.read(fd, 27))
print('-----file1.txt information------')
print(fs.stat('file1.txt').file_type)
print(fs.stat('file1.txt').size)
print(fs.stat('file1.txt').hard_links)
print('--------------------------------')
fs.close(fd)


fs.create('file2.txt')
fd = fs.open('file2.txt')
fs.write(fd, b'My name is Marian')
fs.seek(fd, 0)
print(fs.read(fd, 17))
print(fs.stat('file2.txt').size)
fs.truncate('file2.txt', 5)
# fs.truncate('file22.txt', 10)
print(fs.stat('file2.txt').size)
fs.seek(fd, 0)
print(fs.read(fd, 17))
print('-----file2.txt information------')
print(fs.stat('file2.txt').file_type)
print(fs.stat('file2.txt').size)
print(fs.stat('file2.txt').hard_links)
print('--------------------------------')
fs.close(fd)

fs.create('file3.txt')
print(fs.ls())


fs.link('file1.txt', 'file1_link.txt')
print(fs.ls())

fd = fs.open('file1_link.txt')
fs.seek(fd, 0)
print(fs.read(fd, 27))
print(fs.stat('file1_link.txt').size)
fs.truncate('file1_link.txt', 5)
print(fs.stat('file1_link.txt').size)
fs.seek(fd, 0)
print(fs.read(fd, 27))
fs.close(fd)

fd = fs.open('file1.txt')
fs.seek(fd, 0)
print(fs.read(fd, 27))
fs.close(fd)


print('-----file1.txt information------')
print(fs.stat('file1.txt').file_type)
print(fs.stat('file1.txt').size)
print(fs.stat('file1.txt').hard_links)
print('--------------------------------')

print(fs.stat('file1.txt').size)
fs.truncate('file1.txt', 10)
print(fs.stat('file1.txt').size)
fs.unlink('file1.txt')
print(fs.ls())

fs.create('file4.txt')
print(fs.ls())

fd = fs.open('file1_link.txt')
fs.seek(fd, 0)
print(fs.read(fd, 20))
print(fs.stat('file1_link.txt').size)
fs.close(fd)

print('-----file3.txt information------')
print(fs.stat('file3.txt').file_type)
print(fs.stat('file3.txt').size)
print(fs.stat('file3.txt').hard_links)
print('--------------------------------')


print('-----file4.txt information------')
print(fs.stat('file4.txt').file_type)
print(fs.stat('file4.txt').size)
print(fs.stat('file4.txt').hard_links)
print('--------------------------------')


