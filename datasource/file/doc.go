// Package file provides a DataSource which reads data from a directory of files on disk.
// Files are assigned to workers in their entirety, so it is favourable if individual
// files represent roughly equal-sized divisions of data.
package file
