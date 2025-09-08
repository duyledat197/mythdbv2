package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
)

// writeIndex writes the index to the writer
func writeIndex(writer *bufio.Writer, index []IndexEntry) error {
	// Write number of index entries
	if err := binary.Write(writer, binary.BigEndian, uint32(len(index))); err != nil {
		return err
	}

	// Write each index entry
	for _, entry := range index {
		// Key length
		if err := binary.Write(writer, binary.BigEndian, uint32(len(entry.Key))); err != nil {
			return err
		}
		// Key
		if _, err := writer.Write(entry.Key); err != nil {
			return err
		}
		// Offset
		if err := binary.Write(writer, binary.BigEndian, entry.Offset); err != nil {
			return err
		}
		// Length
		if err := binary.Write(writer, binary.BigEndian, entry.Length); err != nil {
			return err
		}
		// ChecksumOffset
		if err := binary.Write(writer, binary.BigEndian, entry.ChecksumOffset); err != nil {
			return err
		}
	}

	return nil
}

// readIndex reads the index from the reader
func readIndex(reader *bufio.Reader) ([]IndexEntry, error) {
	// Read number of index entries
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	index := make([]IndexEntry, count)
	for i := uint32(0); i < count; i++ {
		// Key length
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		// Key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		// Offset
		var offset int64
		if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		// Length
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		// ChecksumOffset
		var checksumOffset int64
		if err := binary.Read(reader, binary.BigEndian, &checksumOffset); err != nil {
			return nil, err
		}

		index[i] = IndexEntry{
			Key:            key,
			Offset:         offset,
			Length:         length,
			ChecksumOffset: checksumOffset,
		}
	}

	return index, nil
}
