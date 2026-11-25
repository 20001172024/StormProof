package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// OSFS is an FS backed by one directory on the real file system.
type OSFS struct {
	dir string
}

var _ FS = (*OSFS)(nil)

// NewOSFS returns an FS for dir, creating the directory if needed.
func NewOSFS(dir string) (*OSFS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &OSFS{dir: dir}, nil
}

// path turns a flat file name into a path inside the directory, refusing
// anything that could point elsewhere.
func (o *OSFS) path(name string) (string, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("storage: invalid file name %q", name)
	}
	return filepath.Join(o.dir, name), nil
}

func (o *OSFS) open(name string, flag int) (File, error) {
	p, err := o.path(name)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(p, flag, 0o644) // *os.File implements File
}

// Create implements FS.
func (o *OSFS) Create(name string) (File, error) {
	return o.open(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
}

// Append implements FS.
func (o *OSFS) Append(name string) (File, error) {
	return o.open(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
}

// ReadFile implements FS.
func (o *OSFS) ReadFile(name string) ([]byte, error) {
	p, err := o.path(name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

// Rename implements FS.
func (o *OSFS) Rename(oldName, newName string) error {
	from, err := o.path(oldName)
	if err != nil {
		return err
	}
	to, err := o.path(newName)
	if err != nil {
		return err
	}
	return os.Rename(from, to)
}

// Remove implements FS.
func (o *OSFS) Remove(name string) error {
	p, err := o.path(name)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

// List implements FS. Subdirectories are ignored.
func (o *OSFS) List() ([]string, error) {
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// SyncDir implements FS by fsyncing the directory. On Linux this is what
// makes creates and renames inside it durable.
func (o *OSFS) SyncDir() (err error) {
	d, err := os.Open(o.dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	return d.Sync()
}
