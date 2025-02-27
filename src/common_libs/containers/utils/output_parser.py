import re

from common_libs.logging import get_logger

logger = get_logger(__name__)


def parse_table_output(output: str) -> list[dict[str, str | None]]:
    """Parse tabulated command output and return it as a list of dictionaries

    :param output: Command output in table format with column headers where the first line is headesrs and the rest is
                   table rows

    NOTE: This function assumes that each header is separeted by at least 3 white spaces

    Example:
    >>> from pprint import pprint
    >>> output = '''
    ... CONTAINER ID   IMAGE         COMMAND   CREATED       STATUS       PORTS     NAMES
    ... baee57c75f17   python:3.11   "bash"    5 hours ago   Up 5 hours             elegant_chatterjee
    ... '''
    >>> pprint(parse_table_output(output))
    [{'COMMAND': '"bash"',
      'CONTAINER ID': 'baee57c75f17',
      'CREATED': '5 hours ago',
      'IMAGE': 'python:3.11 ',
      'NAMES': 'elegant_chatterjee',
      'PORTS': None,
      'STATUS': 'Up 5 hours'}]
    """
    non_table_pattern = r"^\S+(?: \S+)*$"
    lines = output.strip().splitlines()

    # Output could contain some messages at the beginning. Attempt to locate the correct header line.
    header_pos = 0
    for i, line in enumerate(lines):
        if line and not re.match(non_table_pattern, line):
            header_pos = i
            # Note that if none of lines match the condition, we will assume the output is a table with one column
            # even if it's just regular messages without table
            if header_pos:
                logger.warning("Ignored non table data at the beginning of the output")
            break

    table_data = lines[header_pos:]
    if len(table_data) < 2:
        raise ValueError("The table data should contain at least one row")

    header_line = table_data[0].rstrip()
    headers = re.split(r"\s{3,}", header_line)
    table = []
    for line in table_data[1:]:
        # Output could contain some messages at the end. Ignore these lines
        if not line or (len(headers) > 1 and re.match(non_table_pattern, line)):
            logger.warning("Ignored non table data at the end of the output")
            break

        row = {}
        pos = 0
        for i, header in enumerate(headers):
            start = pos + header_line[pos:].index(header)
            if i == len(headers) - 1:
                end = len(line)
            else:
                offset = pos + len(header)
                end = offset + header_line[offset:].index(headers[i + 1])
            col_value = line[start:end].strip()
            row[header] = col_value or None
            pos = end
        table.append(row)

    if not table:
        raise ValueError("Unable to parse the table data from the given output")

    return table
