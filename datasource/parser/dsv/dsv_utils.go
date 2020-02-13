package dsv

import (
	"fmt"
	"strconv"
	"time"

	columntype "github.com/go-sif/sif/columntype"
	core "github.com/go-sif/sif/core"
)

// Parses a slice of strings into a Row, according to a schema
func scanRow(conf *ParserConf, names []string, types []core.ColumnType, rowStrings []string, row *core.Row) error {
	for i := 0; i < len(rowStrings); i++ {
		colVal := rowStrings[i]
		// check for a nil value
		if len(colVal) == 0 || colVal == conf.NilValue {
			row.SetNil(names[i])
			continue
		}
		// otherwise, parse type
		switch types[i].(type) {
		case *columntype.ByteColumnType:
			if len(colVal) > 1 {
				return fmt.Errorf("ByteColumn %s contains more than one byte", names[i])
			}
			row.SetByte(names[i], colVal[0])
		case *columntype.BytesColumnType:
			if len(colVal) > types[i].Size() {
				return fmt.Errorf("BytesColumn %s contains more than %d bytes", names[i], types[i].Size())
			}
			row.SetBytes(names[i], []byte(colVal))
		case *columntype.BoolColumnType:
			bval, err := strconv.ParseBool(colVal)
			if err != nil {
				return err
			}
			row.SetBool(names[i], bval)
		case *columntype.Uint8ColumnType:
			ival, err := strconv.ParseUint(colVal, 10, 8)
			if err != nil {
				return err
			}
			row.SetUint8(names[i], uint8(ival))
		case *columntype.Uint16ColumnType:
			ival, err := strconv.ParseUint(colVal, 10, 16)
			if err != nil {
				return err
			}
			row.SetUint16(names[i], uint16(ival))
		case *columntype.Uint32ColumnType:
			ival, err := strconv.ParseUint(colVal, 10, 32)
			if err != nil {
				return err
			}
			row.SetUint32(names[i], uint32(ival))
		case *columntype.Uint64ColumnType:
			ival, err := strconv.ParseUint(colVal, 10, 64)
			if err != nil {
				return err
			}
			row.SetUint64(names[i], ival)
		case *columntype.Int8ColumnType:
			ival, err := strconv.ParseInt(colVal, 10, 8)
			if err != nil {
				return err
			}
			row.SetInt8(names[i], int8(ival))
		case *columntype.Int16ColumnType:
			ival, err := strconv.ParseInt(colVal, 10, 16)
			if err != nil {
				return err
			}
			row.SetInt16(names[i], int16(ival))
		case *columntype.Int32ColumnType:
			ival, err := strconv.ParseInt(colVal, 10, 32)
			if err != nil {
				return err
			}
			row.SetInt32(names[i], int32(ival))
		case *columntype.Int64ColumnType:
			ival, err := strconv.ParseInt(colVal, 10, 64)
			if err != nil {
				return err
			}
			row.SetInt64(names[i], int64(ival))
		case *columntype.Float32ColumnType:
			fval, err := strconv.ParseFloat(colVal, 32)
			if err != nil {
				return err
			}
			row.SetFloat32(names[i], float32(fval))
		case *columntype.Float64ColumnType:
			fval, err := strconv.ParseFloat(colVal, 64)
			if err != nil {
				return err
			}
			row.SetFloat64(names[i], fval)
		case *columntype.StringColumnType:
			row.SetString(names[i], colVal)
		case *columntype.TimeColumnType:
			format := types[i].(*columntype.TimeColumnType).Format
			tval, err := time.Parse(format, colVal)
			if err != nil {
				return fmt.Errorf("Column %s could not be parsed as datetime with format %s. Was: %#v", names[i], format, colVal)
			}
			row.SetTime(names[i], tval)
		case *columntype.VarStringColumnType:
			row.SetVarString(names[i], colVal)
		case *columntype.VarBytesColumnType:
			row.SetVarBytes(names[i], []byte(colVal))
		default:
			return fmt.Errorf("DSV parsing does not support column type %T", types[i])
		}
	}
	return nil
}
