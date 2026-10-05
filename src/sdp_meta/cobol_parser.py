# Pure-Python COBOL copybook parser and record decoder (new in this repository).
"""Parse a COBOL copybook and decode fixed-length records.

Supported: elementary fields with PIC X/A (text), PIC [S]9[V9] as DISPLAY (zoned),
COMP-3 (packed) or COMP/BINARY (big-endian), nested groups (flattened) and FILLER.
Not supported: OCCURS, REDEFINES, COMP-1/COMP-2, variable-length records.
"""
import re
from collections import namedtuple
from decimal import Decimal

Field = namedtuple("Field", "name kind offset length scale spark_type")

PACKED = {"COMP-3", "COMPUTATIONAL-3", "PACKED-DECIMAL"}
BINARY = {"COMP", "COMP-4", "COMP-5", "COMPUTATIONAL", "BINARY"}


def parse_copybook(text):
    """Return (fields, record_length) for a copybook in standard format (code in columns 8-72)."""
    lines = [line[6:72] for line in text.splitlines() if len(line) > 6 and line[6] not in "*/"]
    fields, offset = [], 0
    for statement in re.split(r"\.(?:\s|$)", " ".join(lines)):
        tokens = statement.upper().split()
        if len(tokens) < 2 or not tokens[0].isdigit() or tokens[0] in ("66", "88"):
            continue
        unsupported = {"OCCURS", "REDEFINES", "COMP-1", "COMP-2"} & set(tokens)
        if unsupported:
            raise ValueError(f"Unsupported copybook clause {sorted(unsupported)} in: {statement.strip()}")
        pic_at = next((i for i, token in enumerate(tokens) if token in ("PIC", "PICTURE")), None)
        if pic_at is None:
            continue  # group item: its children carry the data
        clauses = [token for token in tokens[pic_at + 1:] if token != "IS"]
        pic = re.sub(r"(.)\((\d+)\)", lambda m: m[1] * int(m[2]), clauses[0])
        kind, scale, spark_type, length = "string", 0, "string", len(pic)
        if re.fullmatch(r"S?9+(V9*)?", pic):
            digits, scale = pic.count("9"), len(pic.partition("V")[2])
            if PACKED & set(clauses):
                kind, length = "packed", digits // 2 + 1
            elif BINARY & set(clauses):
                kind, length = "binary", 2 if digits <= 4 else 4 if digits <= 9 else 8
            else:
                kind, length = "zoned", digits
            if scale or digits > 18:
                spark_type = f"decimal({digits},{scale})"
            else:
                spark_type = "int" if digits <= 9 else "bigint"
        if tokens[1] != "FILLER":
            fields.append(Field(tokens[1].replace("-", "_"), kind, offset, length, scale, spark_type))
        offset += length
    return fields, offset


def decode_field(raw, field, encoding="cp037"):
    """Decode one field's bytes; undecodable numeric values become None."""
    if field.kind == "string":
        return raw.decode(encoding, errors="replace").rstrip(" \x00")
    try:
        if field.kind == "binary":
            number = int.from_bytes(raw, "big", signed=True)
        elif field.kind == "packed":  # one digit per half byte, the last half byte is the sign
            digits = raw.hex()
            if digits[-1] not in "abcdef":
                raise ValueError("invalid packed sign")
            number = -int(digits[:-1]) if digits[-1] in "bd" else int(digits[:-1])
        else:  # zoned: digit in the low half of each byte, sign in the high half of the last byte
            number = int("".join(str(byte & 0x0F) for byte in raw))
            if raw[-1] >> 4 in (0xB, 0xD):
                number = -number
    except ValueError:
        return None
    if field.spark_type.startswith("decimal"):
        return Decimal(number).scaleb(-field.scale)
    return number


def decode_records(data, fields, record_length, encoding="cp037"):
    """Split a file's bytes into fixed-length records and decode each into a tuple."""
    if len(data) % record_length:
        raise ValueError(
            f"File size {len(data)} is not a multiple of the copybook record length {record_length}: "
            "wrong copybook, or a variable-length file (not supported)"
        )
    return [
        tuple(decode_field(data[start + f.offset:start + f.offset + f.length], f, encoding) for f in fields)
        for start in range(0, len(data), record_length)
    ]
