import struct
import sys

# 32位整数（I），32字节的字符串（32s），255字节的字符串（255s）
STRUCT_PACK_FMT = 'I32s255s'


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
    PREPARE_SYNTAX_ERROR = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2


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


class Row:
    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email


class Table:
    def __init__(self):
        self.num_rows = 0
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]


def print_row(row):
    print(f"({row.id}, {row.username}, {row.email})")


def serialize_row(source: Row, destination):
    struct.pack_into(STRUCT_PACK_FMT, destination, 0, source.id, source.username.encode("utf-8"),
                     source.email.encode("utf-8"))


def deserialize_row(source, destination: Row):
    (destination.id, username_bytes, email_bytes) = struct.unpack_from(STRUCT_PACK_FMT, source)
    destination.username = username_bytes.decode("utf-8").rstrip("\x00")
    destination.email = email_bytes.decode("utf-8").rstrip("\x00")


def row_slot(table: Table, row_num: int):
    page_num = row_num // ROWS_PER_PAGE
    page = table.pages[page_num]
    if page is None:
        page = table.pages[page_num] = bytearray(PAGE_SIZE)
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return memoryview(page)[byte_offset:byte_offset + ROW_SIZE]


def new_table() -> Table:
    return Table()


def free_table(table) -> None:
    for page in table.pages:
        if page is not None:
            del page
    del table


class InputBuffer:
    def __init__(self):
        self.buffer = None


class Statement:
    def __init__(self):
        self.type = None
        self.row_to_insert = None


def print_prompt():
    sys.stdout.write("db > ")
    sys.stdout.flush()


def read_input(input_buffer):
    input_buffer.buffer = input()


def do_meta_command(input_buffer, table: Table):
    if input_buffer.buffer == ".exit":
        free_table(table)
        sys.exit(0)
    else:
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(input_buffer: InputBuffer, statement: Statement) -> int:
    if input_buffer.buffer.startswith("insert"):
        statement.type = StatementType.STATEMENT_INSERT
        args = input_buffer.buffer.split()
        if len(args) < 4:
            return PrepareResult.PREPARE_SYNTAX_ERROR
        statement.row_to_insert = Row(int(args[1]), args[2], args[3])
        return PrepareResult.PREPARE_SUCCESS
    elif input_buffer.buffer.startswith('select'):
        statement.type = StatementType.STATEMENT_SELECT
        return PrepareResult.PREPARE_SUCCESS
    else:
        return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT


def execute_insert(statement, table) -> int:
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.EXECUTE_TABLE_FULL

    row_to_insert = statement.row_to_insert
    serialize_row(row_to_insert, row_slot(table, table.num_rows))
    table.num_rows += 1

    return ExecuteResult.EXECUTE_SUCCESS


def execute_select(statement, table):
    for i in range(table.num_rows):
        row = Row(0, "", "")
        deserialize_row(row_slot(table, i), row)
        print_row(row)
    return ExecuteResult.EXECUTE_SUCCESS


def execute_statement(statement: Statement, table: Table):
    if statement.type == StatementType.STATEMENT_INSERT:
        return execute_insert(statement, table)
    elif statement.type == StatementType.STATEMENT_SELECT:
        return execute_select(statement, table)


def main():
    table = new_table()
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
        elif prepare_result == PrepareResult.PREPARE_SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
        elif prepare_result == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{input_buffer.buffer}'.")


if __name__ == "__main__":
    main()
