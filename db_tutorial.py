import os
import struct
import sys
from typing import List

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


class Statement:
    def __init__(self):
        self.type = None
        self.row_to_insert = Row()


class InputBuffer:
    def __init__(self):
        self.buffer = None


class Pager:
    def __init__(self, filename: str):
        self.filename = filename
        self.file_descriptor = os.open(
            self.filename,
            os.O_RDWR | os.O_CREAT,
        )
        self.file_length = os.lseek(self.file_descriptor, 0, os.SEEK_END)
        self.pages: List = [None] * TABLE_MAX_PAGES

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

    def pager_flush(self, page_num: int):
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            exit(EXIT_FAILURE)

        offset = os.lseek(self.file_descriptor, page_num * PAGE_SIZE, os.SEEK_SET)

        if offset == -1:
            print(f"Error seeking:")
            exit(EXIT_FAILURE)
        bytes_written = os.write(self.file_descriptor, self.pages[page_num])
        if bytes_written == -1:
            print(f"Error writing: ")
            exit(EXIT_FAILURE)


class Table:
    def __init__(self, pager: Pager, num_rows: int = 0):
        self.pager = pager
        self.num_rows = num_rows


class Cursor:
    """
    游标对象
    """

    def __init__(self, table: Table, row_num: int = 0, end_of_table: bool = False):
        self.table = table
        self.row_num = row_num
        self.end_of_table = end_of_table

    def cursor_value(self):
        """
        通过行数，来判断当前的数据被插入到那个页面上
        :return:
        """
        page_num = self.row_num // ROWS_PER_PAGE
        page = self.table.pager.get_page(page_num)
        row_offset = self.row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return memoryview(page)[byte_offset: byte_offset + ROW_SIZE]

    def cursor_advance(self):
        """
        游标前进对象
        :return:
        """
        self.row_num += 1
        if self.row_num >= self.table.num_rows:
            self.end_of_table = Table


def serialize_row(source: Row, destination):
    """
    将行记录，进行序列化操作
    :param source:
    :param destination:
    :return:
    """
    struct.pack_into(STRUCT_PACK_FMT, destination, 0, source.id, source.username.encode("utf-8"),
                     source.email.encode("utf-8"))


def deserialize_row(source) -> Row:
    """
    将行记录，进行反序列化操作
    :param source:
    :return:
    """
    row = Row()
    (row.id, username_bytes, email_bytes) = struct.unpack_from(STRUCT_PACK_FMT, source)
    row.username = username_bytes.decode("utf-8").rstrip("\x00")
    row.email = email_bytes.decode("utf-8").rstrip("\x00")
    return row


def table_start(table: Table) -> Cursor:
    cursor = Cursor(table, row_num=0, end_of_table=bool(table.num_rows == 0))
    return cursor


def table_end(table: Table) -> Cursor:
    cursor = Cursor(table, row_num=table.num_rows, end_of_table=True)
    return cursor


def db_open(filename: str) -> Table:
    pager = Pager(filename)
    num_rows = pager.file_length // ROW_SIZE
    table = Table(pager, num_rows)
    return table


def db_close(table: Table):
    pager = table.pager
    num_full_pages = table.num_rows // ROWS_PER_PAGE

    for i in range(num_full_pages):
        if pager.pages[i] is not None:
            pager.pager_flush(i)
            pager.pages[i] = None

    num_additional_rows = table.num_rows % ROWS_PER_PAGE
    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.pages[page_num] is not None:
            pager.pager_flush(page_num)
            pager.pages[page_num] = None

    os.close(pager.file_descriptor)
    for i in range(TABLE_MAX_PAGES):
        page = pager.pages[i]
        if page:
            pager.pages[i] = None


def do_meta_command(input_buffer: InputBuffer, table: Table):
    if input_buffer.buffer == ".exit":
        db_close(table)
        sys.exit(EXIT_FAILURE)
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
    """
    执行插入前的预处理操作
    :param input_buffer:
    :param statement:
    :return:
    """
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


def execute_insert(statement: Statement, table: Table) -> int:
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.EXECUTE_TABLE_FULL

    cursor = table_end(table)
    serialize_row(statement.row_to_insert, cursor.cursor_value())
    table.num_rows += 1

    return ExecuteResult.EXECUTE_SUCCESS


def execute_select(statement: Statement, table: Table):
    cursor = table_start(table)
    while not cursor.end_of_table:
        row = deserialize_row(cursor.cursor_value())
        print(str(row))
        cursor.cursor_advance()

    return ExecuteResult.EXECUTE_SUCCESS


def main():
    # vim mydb.db
    # :%!xxd

    # argv = sys.argv
    # if len(argv) < 2:
    #     print("Must supply a database filename.\n")
    #     exit(EXIT_FAILURE)
    # filename = argv[1]
    filename = 'mydb.db'
    table = db_open(filename)
    input_buffer = InputBuffer()

    while True:
        input_buffer.buffer = input()
        if input_buffer.buffer[0] == ".":
            mate_command = do_meta_command(input_buffer, table)
            if mate_command == MetaCommandResult.META_COMMAND_SUCCESS:
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
