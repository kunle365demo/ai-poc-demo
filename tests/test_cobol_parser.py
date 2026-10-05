import unittest
from decimal import Decimal

from sdp_meta.cobol_parser import decode_records, parse_copybook

COPYBOOK = """\
      * comment line
       01  REC.
           05  ID-NUM         PIC 9(4).
           05  NAME-GRP.
               10  FIRST-NAME PIC X(5).
               10  FILLER     PIC X(2).
           05  AMOUNT         PIC S9(5)V99 COMP-3.
           05  QTY            PIC S9(4) COMP.
           05  SIGNED-ZONED   PICTURE IS S9(3)V9.
           05  BIG            PIC 9(12) BINARY.
"""


class CobolParserTests(unittest.TestCase):
    def test_layout(self):
        fields, record_length = parse_copybook(COPYBOOK)
        self.assertEqual(
            [(f.name, f.kind, f.offset, f.length, f.spark_type) for f in fields],
            [("ID_NUM", "zoned", 0, 4, "int"),
             ("FIRST_NAME", "string", 4, 5, "string"),
             ("AMOUNT", "packed", 11, 4, "decimal(7,2)"),
             ("QTY", "binary", 15, 2, "int"),
             ("SIGNED_ZONED", "zoned", 17, 4, "decimal(4,1)"),
             ("BIG", "binary", 21, 8, "bigint")])
        self.assertEqual(record_length, 29)

    def test_decode_ebcdic_and_ascii(self):
        fields, record_length = parse_copybook(COPYBOOK)
        tail = bytes.fromhex("0012345d") + (-7).to_bytes(2, "big", signed=True)
        big = (123456789012).to_bytes(8, "big")
        ebcdic = "0042".encode("cp037") + "BOB  ".encode("cp037") + b"\x40\x40" + tail + b"\xf1\xf2\xf3\xd4" + big
        ascii_ = b"0042" + b"BOB  " + b"  " + tail + b"1234" + big
        expected = (42, "BOB", Decimal("-123.45"), -7, Decimal("-123.4"), 123456789012)
        self.assertEqual(decode_records(ebcdic * 2, fields, record_length), [expected, expected])
        self.assertEqual(decode_records(ascii_, fields, record_length, "ascii"),
                         [expected[:4] + (Decimal("123.4"),) + expected[5:]])

    def test_bad_packed_value_is_null(self):
        fields, record_length = parse_copybook("       01 R.\n          05 A PIC S9(3) COMP-3.\n")
        self.assertEqual(decode_records(b"\x40\x40", fields, record_length), [(None,)])

    def test_rejects_wrong_record_length_and_unsupported_clauses(self):
        fields, record_length = parse_copybook(COPYBOOK)
        with self.assertRaises(ValueError):
            decode_records(b"\x00" * 30, fields, record_length)
        with self.assertRaises(ValueError):
            parse_copybook("       01 R.\n          05 A PIC X(2) OCCURS 3 TIMES.\n")


if __name__ == "__main__":
    unittest.main()
