import os
import struct
import sys
from typing import List, Optional

# 32位整数（I），32字节的字符串（32s），255字节的字符串（255s）
STRUCT_PACK_FMT = 'I32s255s'
EXIT_FAILURE = 0


class MetaCommandResult:
    """
    命令执行结果
    """
    META_COMMAND_SUCCESS = 0
    META_COMMAND_UNRECOGNIZED_COMMAND = 1


class PrepareResult:
    """
    预处理结果
    """
    PREPARE_SUCCESS = 0
    PREPARE_NEGATIVE_ID = 1
    PREPARE_STRING_TOO_LONG = 2
    PREPARE_SYNTAX_ERROR = 3
    PREPARE_UNRECOGNIZED_STATEMENT = 4


class StatementType:
    """
    声明类型
    """
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2


class ExecuteResult:
    """
    执行结果
    """
    EXECUTE_SUCCESS = 0
    EXECUTE_TABLE_FULL = 1


ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255


class Row:
    def __init__(self, id=None, username=None, email=None):
        self.id = id
        self.username = username
        self.email = email

    def __repr__(self):
        return f"({self.id}, {self.username}, {self.email})"

    def serialize_row(self, destination):
        """
        将行记录，进行序列化操作
        :param destination:
        :return:
        """
        struct.pack_into(STRUCT_PACK_FMT, destination, 0, self.id, self.username.encode("utf-8"),
                         self.email.encode("utf-8"))

    def deserialize_row(self, source):
        """
        将行记录，进行反序列化操作
        :param source:
        :return:
        """
        (self.id, username_bytes, email_bytes) = struct.unpack_from(STRUCT_PACK_FMT, source)
        self.username = username_bytes.decode("utf-8").rstrip("\x00")
        self.email = email_bytes.decode("utf-8").rstrip("\x00")


class Pager:
    def __init__(self, file_descriptor: int, file_length: int, filename: str):
        self.file_descriptor = file_descriptor
        self.file_length = file_length
        self.pages: List = [None] * TABLE_MAX_PAGES
        self.filename = filename

    def get_page(self, page_num: int) -> bytearray:
        if page_num > TABLE_MAX_PAGES:
            print(f"Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}")
            exit(EXIT_FAILURE)

        if self.pages[page_num] is None:
            page = bytearray(PAGE_SIZE)
            num_pages = self.file_length // PAGE_SIZE

            if self.file_length % PAGE_SIZE:
                num_pages += 1

            if page_num <= num_pages:
                os.lseek(self.file_descriptor, page_num * PAGE_SIZE, os.SEEK_SET)
                bytes_read = os.read(self.file_descriptor, PAGE_SIZE)
                if bytes_read == -1:
                    print(f"Error reading file: {self.filename}")
                    exit()

            self.pages[page_num] = page

        return self.pages[page_num]

    def pager_flush(self, page_num: int, size: int):
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            exit(EXIT_FAILURE)

        offset = os.lseek(self.file_descriptor, page_num * PAGE_SIZE, os.SEEK_SET)

        if offset == -1:
            print(f"Error seeking:")
            exit(EXIT_FAILURE)

        bytes_written = os.write(self.file_descriptor, self.pages[page_num][:size])

        if bytes_written == -1:
            print(f"Error writing: ")
            exit(EXIT_FAILURE)


class Table:
    def __init__(self):
        self.num_rows = 0
        self.pager: Optional[Pager] = None

    def init_self(self, pager: Pager, num_rows: int):
        """
        执行表的初始化操作
        :param pager:
        :param num_rows:
        :return:
        """
        self.pager = pager
        self.num_rows = num_rows

    def row_slot(self, row_num: int):
        """
        获取行记录，在表中的插入位置
        :param row_num:
        :return:
        """
        page_num = row_num // ROWS_PER_PAGE  # 当前行记录，所在的页面
        page = self.get_page(page_num)
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return memoryview(page)[byte_offset:byte_offset + ROW_SIZE]

    def get_page(self, page_num: int) -> bytearray:
        return self.pager.get_page(page_num)


def db_open(filename: str = None) -> Table:
    pager = pager_open(filename)
    num_rows = pager.file_length // ROW_SIZE
    table = Table()
    table.init_self(pager, num_rows)
    return table


def pager_open(filename: str = None) -> Pager:
    fd = os.open(
        filename,
        os.O_RDWR | os.O_CREAT,
    )
    file_length = os.lseek(fd, 0, os.SEEK_END)
    pager = Pager(fd, file_length, filename)
    return pager


def db_close(table: Table):
    pager = table.pager
    num_full_pages = table.num_rows // ROWS_PER_PAGE

    for i in range(num_full_pages):
        if pager.pages[i] is not None:
            pager.pager_flush(i, pager.pages[i])
            pager.pages[i] = None

    num_additional_rows = table.num_rows % ROWS_PER_PAGE
    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.pages[page_num] is not None:
            pager.pager_flush(page_num, pager.pages[page_num][:num_additional_rows * ROW_SIZE])
            pager.pages[page_num] = None

    os.close(pager.file_descriptor)
    for i in range(TABLE_MAX_PAGES):
        page = pager.pages[i]
        if page:
            pager.pages[i] = None

    del pager
    del table


class InputBuffer:
    def __init__(self):
        self.buffer = None


class Statement:
    def __init__(self):
        self.type = None
        self.row_to_insert = Row()


def print_prompt():
    sys.stdout.write("db > ")
    sys.stdout.flush()


def read_input(input_buffer):
    input_buffer.buffer = input()


def do_meta_command(input_buffer, table: Table):
    if input_buffer.buffer == ".exit":
        sys.exit(0)
    else:
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(input_buffer: InputBuffer, statement: Statement) -> int:
    if input_buffer.buffer.startswith("insert"):
        return prepare_insert(input_buffer, statement)
    elif input_buffer.buffer.startswith('select'):
        statement.type = StatementType.STATEMENT_SELECT
        return PrepareResult.PREPARE_SUCCESS
    else:
        return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT


def prepare_insert(input_buffer: InputBuffer, statement: Statement) -> int:
    statement.type = StatementType.STATEMENT_INSERT
    tokens = input_buffer.buffer.split()
    id_string = tokens[1] if len(tokens) > 1 else None
    username = tokens[2] if len(tokens) > 2 else None
    email = tokens[3] if len(tokens) > 3 else None
    if id_string is None or username is None or email is None:
        return PrepareResult.PREPARE_SYNTAX_ERROR
    t_id = int(id_string)
    if t_id < 0:
        return PrepareResult.PREPARE_NEGATIVE_ID
    if len(username) > COLUMN_USERNAME_SIZE:
        return PrepareResult.PREPARE_STRING_TOO_LONG
    if len(email) > COLUMN_EMAIL_SIZE:
        return PrepareResult.PREPARE_STRING_TOO_LONG
    statement.row_to_insert.id = t_id
    statement.row_to_insert.username = username
    statement.row_to_insert.email = email
    return PrepareResult.PREPARE_SUCCESS


def execute_statement(statement: Statement, table: Table):
    if statement.type == StatementType.STATEMENT_INSERT:
        return execute_insert(statement, table)
    elif statement.type == StatementType.STATEMENT_SELECT:
        return execute_select(statement, table)


def execute_insert(statement, table) -> int:
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.EXECUTE_TABLE_FULL

    row_to_insert = statement.row_to_insert
    row_to_insert.serialize_row(table.row_slot(table.num_rows))
    table.num_rows += 1

    return ExecuteResult.EXECUTE_SUCCESS


def execute_select(statement, table):
    for i in range(table.num_rows):
        row = Row(0, "", "")
        row.deserialize_row(table.row_slot(i))
        print(row)
    return ExecuteResult.EXECUTE_SUCCESS


def main():
    table = db_open()
    input_buffer = InputBuffer()

    while True:
        print_prompt()
        read_input(input_buffer)

        if input_buffer.buffer[0] == ".":
            if do_meta_command(input_buffer, table) == MetaCommandResult.META_COMMAND_SUCCESS:
                continue

        statement = Statement()
        prepare_result = prepare_statement(input_buffer, statement)

        if prepare_result == PrepareResult.PREPARE_SUCCESS:
            execute_result = execute_statement(statement, table)
            if execute_result == ExecuteResult.EXECUTE_SUCCESS:
                print("Executed.")
            elif execute_result == ExecuteResult.EXECUTE_TABLE_FULL:
                print("Error: Table full.")
        elif prepare_result == PrepareResult.PREPARE_NEGATIVE_ID:
            print("ID must be positive.")
        elif prepare_result == PrepareResult.PREPARE_STRING_TOO_LONG:
            print("String is too long.")
        elif prepare_result == PrepareResult.PREPARE_SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
        elif prepare_result == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{input_buffer.buffer}'.")


if __name__ == "__main__":
    main()
