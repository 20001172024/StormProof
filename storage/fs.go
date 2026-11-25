package storage

import "io"

// FS is the small file-system interface that a node's durable storage is
// written against. The write-ahead log and snapshot code (M4, M6) use only
// this, so the same code runs on a real disk (an os-backed FS, added in M4)
// and on the simulator's in-memory disk with injected faults.
//
// Names are flat: one directory per node, no subdirectories.
//
// Errors for a missing file wrap io/fs.ErrNotExist, so callers can test
// them with errors.Is(err, fs.ErrNotExist) for both implementations.
type FS interface {
	// Create creates the named file, or truncates it if it exists, and
	// opens it for writing.
	Create(name string) (File, error)
	// Append opens the named file for appending, creating it if needed.
	Append(name string) (File, error)
	// ReadFile returns the file's current contents.
	ReadFile(name string) ([]byte, error)
	// Rename atomically replaces newName with oldName.
	Rename(oldName, newName string) error
	// Remove deletes the named file.
	Remove(name string) error
	// List returns all file names, sorted.
	List() ([]string, error)
}

// File is an open file. Data written is not durable until Sync returns
// nil: a crash may lose any of it, or keep only part of it.
type File interface {
	io.Writer
	Sync() error
	Close() error
}
