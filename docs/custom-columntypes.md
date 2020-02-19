# Implementing Custom ColumnTypes

`Sif` supports, and encourages, the definition of custom variable-width `ColumnType`s which adhere to the two following interface:

```go
type CustomColumnType interface {
	// returns size in bytes of a column type
	Size() int
	// produces a string representation of a value
	// of this type, for logging purposes
	ToString(v interface{}) string
	// Defines how this variable-length type is serialized into bytes
	Serialize(v interface{}) ([]byte, error)
	// Defines how this variable-length type is deserialized from bytes
	Deserialize([]byte) (interface{}, error)
}
```

The primary intention of defining a `CustomColumnType` is in defining custom `Serialize` and `Deserialize` methods. Any serialization approach may be used, as long as the end result is a `[]byte`.

## Defining a CustomColumnType

An easy approach to defining a `CustomColumnType` is to leverage the `gob` package:

```go
type MapStringIntColumnType struct {
}

func (t *MapStringIntColumnType) Size() int {
	return 0
}

func (t *MapStringIntColumnType) ToString(v interface{}) string {
	return "[HEATMAP]"
}

func (t *MapStringIntColumnType) Serialize(v interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(v)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (t *MapStringIntColumnType) Deserialize(ser []byte) (interface{}, error) {
	var deser map[string]int
	buff := bytes.NewBuffer(ser)
	d := gob.NewDecoder(buff)
	err := d.Decode(&deser)
	if err != nil {
		return nil, err
	}
	return deser, nil
}
```

## Using a CustomColumnType

Using one's own `CustomColumnType` is as simple as including it in a Schema:

```go
schema := schema.CreateSchema()
schema.CreateColumn("my_map", MapStringIntColumnType{})
```

As `Sif` treats all custom `ColumnType`s as variable-length in nature, getting or setting custom values from a `Row` requires the use of the `GetVarCustomData` and `SetVarCustomData` methods, and a type assertion:

```go
ops.Map(func(row sif.Row) error {
	if row.IsNil("my_map") {
		return nil
	}
	data, err := row.GetVarCustomData("my_map")
	if err != nil {
		return err
	}
	tdata := data.(map[string]int)
	tdata["answer"] = 42
	row.SetVarCustomData("my_map", tdata)
	return nil
})
```
