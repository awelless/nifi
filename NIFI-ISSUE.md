# JsonRecordSetWriter promotes quoted JSON strings to numbers when field schema is CHOICE(INT, STRING)

## Description

When a JSON field carries a quoted string value in one record and a bare number in another, the output incorrectly promotes the quoted string to a bare number.

## Root cause

Schema inference sees `TextNode("42")` as STRING and `IntNode(7)` as INT, so `FieldTypeInference` merges the field to `CHOICE(INT, STRING)`. At write time, `DataTypeUtils.findMostSuitableType` sorts candidates by `RecordFieldType` enum ordinal (INT=3 before STRING=13) and returns the first type the string value is convertible to. Because `"42"` is convertible to INT, the string is silently coerced to a number.

The same issue applies to any type narrower than STRING that appears in a CHOICE, including BOOLEAN: `"false"` is promoted to bare `false`.

## Steps to reproduce

Use any flow with JsonTreeReader + JsonRecordSetWriter and inferred schema with records:

```json
{"val":"42"}
{"val":7}
```

Expected output: `[{"val":"42"},{"val":7}]`  
Actual output:   `[{"val":42},{"val":7}]`

## Fix

In `DataTypeUtils.findMostSuitableType`, call `inferDataType(value)` uniformly for all values. A Java `String` value infers as STRING; if STRING is present as a CHOICE candidate it is returned immediately as the exact native-type match. The ordinal-sort conversion path (`findMostSuitableTypeByStringValue`) is used only as a fallback when STRING is not a candidate, preserving existing behaviour for text-only sources (CSV, Grok) where numeric conversion is intentional.

## Components

- nifi-record-serialization-services
- nifi-record

## Related

- NIFI-6640
