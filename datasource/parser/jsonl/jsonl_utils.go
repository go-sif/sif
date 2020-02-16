package jsonl

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-sif/sif"
	json "github.com/json-iterator/go"
)

func parseValue(val interface{}, colName string, colType sif.ColumnType, row sif.Row) error {
	// parse type
	switch colType.(type) {
	// TODO array/slice type
	case *sif.BoolColumnType:
		bval, ok := val.(bool)
		if !ok {
			return fmt.Errorf("Column %s was not a boolean. Was: %#v", colName, val)
		}
		row.SetBool(colName, bval)
	case *sif.Int8ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetInt8(colName, int8(nval))
	case *sif.Int16ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetInt16(colName, int16(nval))
	case *sif.Int32ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetInt32(colName, int32(nval))
	case *sif.Int64ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetInt64(colName, int64(nval))
	case *sif.Float32ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetFloat32(colName, float32(nval))
	case *sif.Float64ColumnType:
		nval, ok := val.(float64)
		if !ok {
			return fmt.Errorf("Column %s was not a number. Was: %#v", colName, val)
		}
		row.SetFloat64(colName, nval)
	case *sif.StringColumnType:
		sval, ok := val.(string)
		if !ok {
			return fmt.Errorf("Column %s was not a string. Was: %#v", colName, val)
		}
		row.SetString(colName, sval)
	case *sif.TimeColumnType:
		format := colType.(*sif.TimeColumnType).Format
		tval, err := time.Parse(format, val.(string))
		if err != nil {
			return fmt.Errorf("Column %s could not be parsed as datetime with format %s. Was: %#v", colName, format, val)
		}
		row.SetTime(colName, tval)
	case *sif.VarStringColumnType:
		sval, ok := val.(string)
		if !ok {
			return fmt.Errorf("Column %s was not a string. Was: %#v", colName, val)
		}
		row.SetVarString(colName, sval)
	default:
		return fmt.Errorf("JSONL parsing does not support column type %T", colType)
	}
	return nil
}

// locateValue recursively, parsing and caching parts of the JSON that are relevant
func locateValue(colName []string, fullName string, colType sif.ColumnType, jsonData map[string]interface{}, row sif.Row) error {
	val, ok := jsonData[colName[0]]
	if !ok || val == nil {
		// Then we know that this key doesn't exist in the JSON and we're done
		row.SetNil(fullName)
		return nil
	} else if len(colName) > 1 {
		// we've parsed colName[0] before and it MUST be in the jsonData
		// we have more key components to recurse on, so we're not done yet
		newJSONData, ok := val.(map[string]interface{})
		if !ok {
			return fmt.Errorf("value for key component %s should be a sub-object, but is not", colName[0])
		}
		return locateValue(colName[1:], fullName, colType, newJSONData, row)
	} else {
		// actually parse the value, since we're done looking for column name components
		return parseValue(val, fullName, colType, row)
	}
}

// Parses a slice of strings into a Row, according to a schema
func scanRow(conf *ParserConf, names []string, types []sif.ColumnType, rowString string, row sif.Row) error {
	var jsonData map[string]interface{}
	err := json.Unmarshal([]byte(rowString), &jsonData)
	if err != nil {
		return err
	}
	for idx, colName := range row.Schema().ColumnNames() {
		path := strings.Split(colName, ".")
		// find and parse data into row, for each column
		err := locateValue(path, colName, types[idx], jsonData, row)
		if err != nil {
			return err
		}
	}
	return nil
}
