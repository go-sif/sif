package jsonl

import (
	"bufio"
	"io"

	"github.com/go-sif/sif"
)

// ParserConf configures a JSONL Parser, suitable for JSON lines data
type ParserConf struct {
	PartitionSize int  // The maximum number of rows per Partition. Defaults to 128.
	HeaderLines   int  // The number of lines to ignore from the beginning of each file. Defaults to 0.
	Comment       rune // Lines beginning with the comment character are ignored. Cannot be equal to the Delimiter. Defaults to no comment character.
}

// Parser produces partitions from JSONL data
type Parser struct {
	conf *ParserConf
}

// CreateParser returns a new JSONL Parser. Columns are parsed lazily from each row of JSON using their column name, which should be a gjson path. Values within the JSON which do not correspond to a Schema column are ignored.
func CreateParser(conf *ParserConf) *Parser {
	if conf.PartitionSize == 0 {
		conf.PartitionSize = 128
	}
	return &Parser{conf: conf}
}

// PartitionSize returns the maximum size in rows of Partitions produced by this Parser
func (p *Parser) PartitionSize() int {
	return p.conf.PartitionSize
}

// Parse parses JSONL data to produce Partitions
func (p *Parser) Parse(r io.Reader, source sif.DataSource, schema sif.Schema, widestInitialSchema sif.Schema, onIteratorEnd func()) (sif.PartitionIterator, error) {
	// start parsing by creating a scanner
	scanner := bufio.NewScanner(r)

	// ignore header lines, if configured to do so
	for i := 0; i < p.conf.HeaderLines; i++ {
		scanner.Scan()
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	iterator := &jsonlFilePartitionIterator{
		parser:              p,
		scanner:             scanner,
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
