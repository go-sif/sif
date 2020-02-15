package dsv

import (
	"encoding/csv"
	"io"

	types "github.com/go-sif/sif/types"
)

// ParserConf configures a DSV Parser
type ParserConf struct {
	PartitionSize int    // The maximum number of rows per Partition. Defaults to 128.
	HeaderLines   int    // The number of lines to ignore from the beginning of each file. Defaults to 0.
	Delimiter     rune   // The delimiter separating columns in the file. Defaults to ,
	Comment       rune   // Lines beginning with the comment character are ignored. Cannot be equal to the Delimiter. Defaults to no comment character.
	NilValue      string // A special string which represents nil values in the dataset. Defaults to "" (the empty string).
}

// Parser produces partitions from DSV data
type Parser struct {
	conf *ParserConf
}

// CreateParser returns a new DSV Parser
func CreateParser(conf *ParserConf) *Parser {
	if conf.PartitionSize == 0 {
		conf.PartitionSize = 128
	}
	if conf.Delimiter == 0 {
		conf.Delimiter = ','
	}
	return &Parser{conf: conf}
}

// PartitionSize returns the maximum size in rows of Partitions produced by this Parser
func (p *Parser) PartitionSize() int {
	return p.conf.PartitionSize
}

// Parse parses DSV data to produce Partitions
func (p *Parser) Parse(r io.Reader, source types.DataSource, schema types.Schema, widestInitialSchema types.Schema, onIteratorEnd func()) (types.PartitionIterator, error) {
	// start parsing by creating a reader
	reader := csv.NewReader(r)
	reader.Comma = p.conf.Delimiter
	reader.Comment = p.conf.Comment
	reader.FieldsPerRecord = schema.NumColumns()
	reader.ReuseRecord = true

	// ignore header lines, if configured to do so
	for i := 0; i < p.conf.HeaderLines; i++ {
		_, err := reader.Read()
		if err != nil {
			return nil, err
		}
	}

	iterator := &dsvFilePartitionIterator{
		parser:              p,
		reader:              reader,
		hasNext:             true,
		source:              source,
		schema:              schema,
		widestInitialSchema: widestInitialSchema,
		endListeners:        []func(){},
	}
	if onIteratorEnd != nil {
		iterator.OnEnd(onIteratorEnd)
	}
	return iterator, nil
}
