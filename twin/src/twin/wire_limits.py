"""Wire limits and byte-budget safety bounds for Twin Layer (Layer 1).

Enforces payload size guarantees (strictly <= 300 bytes) and prevents
memory and representation blowup across Kafka and SQLite streams.
"""

WIRE_VALUE_MIN = -999.99
WIRE_VALUE_MAX = 9999.99
ROGUE_ID_MAX_LEN = 6
SEQ_SAFE_MAX = 999_999
