import os
import struct
import portalocker


class Storage:
    INTEGER_LENGTH = 8
    SUPERBLOCK_SIZE = 2048
    INTEGER_FORMAT = "!Q"  # big-endian unsigned long long

    def __init__(self, file):
        self._file = file
        self.locked = False
        self._reserve_superblock()

    def _itob(self, integer: int) -> bytes:
        """
        convert integer to packed binary data
        for represent python `bytes` object as C struct.
        :param integer: int
        :return: bytes
        """
        return struct.pack(self.INTEGER_FORMAT, integer)

    def _btoi(self, bytes_: bytes) -> int:
        """
        convert packed binary data to python int
        :param bytes_: bytes
        :return: int
        """
        return struct.unpack(self.INTEGER_FORMAT, bytes_)[0]

    def _seek_end(self) -> None:
        """
        move cursor pointer to end of file
        :return:
        """
        self._file.seek(0, os.SEEK_END)

    def _seek_superblock(self) -> None:
        """
        move cursor pointer to 0 byte
        :return: None
        """
        self._file.seek(0)

    def _reserve_superblock(self) -> None:
        self.lock()
        self._seek_end()
        address = self._file.tell()
        if address < self.SUPERBLOCK_SIZE:
            self._file.write(b'\x00' * (self.SUPERBLOCK_SIZE - address))

    def _write_integer(self, integer) -> None:
        """
        Write packed integer value
        :param integer:
        :return: None
        """
        self.lock()
        self._file.write(self._itob(integer))

    def _read_integer(self) -> int:
        """
        Read unpacked integer value
        :return: int
        """
        return self._btoi(self._file.read(self.INTEGER_LENGTH))

    def lock(self) -> bool:
        if not self.locked:
            portalocker.lock(self._file, portalocker.LOCK_EX)
            self.locked = True
            return True
        else:
            return False

    def unlock(self) -> None:
        if self.locked:
            self._file.flush()
            portalocker.unlock(self._file)
            self.locked = False

    def get_root_address(self) -> int:
        """
        get data root address from superblock
        :return: int
        """
        self._seek_superblock()
        address = self._read_integer()
        return address

    def commit_root_address(self, root_address) -> None:
        """
        update data root address in superblock
        :param root_address:
        :return: None
        """
        self.lock()
        self._file.flush()
        self._seek_superblock()
        self._write_integer(root_address)
        self._file.flush()
        self.unlock()

    def write(self, data: bytes) -> int:
        self.lock()
        self._seek_end()
        address = self._file.tell()
        self._write_integer(len(data))
        self._file.write(data)
        return address

    def read(self, address: int) -> bytes:
        self._file.seek(address)
        length = self._read_integer()
        data = self._file.read(length)
        return data

    def close(self):
        self.unlock()
        self._file.close()
