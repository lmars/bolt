package main_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/boltdb/bolt/cmd/bolt"
)

// Ensure the "info" command can print information about a database.
func TestInfoCommand_Run(t *testing.T) {
	db := MustOpen(0666, nil)
	db.DB.Close()
	defer db.Close()

	// Run the info command.
	m := NewMain()
	if err := m.Run("info", db.Path); err != nil {
		t.Fatal(err)
	}
}

// Ensure the "stats" command can execute correctly.
func TestStatsCommand_Run(t *testing.T) {
	// Ignore
	if os.Getpagesize() != 4096 {
		t.Skip("system does not use 4KB page size")
	}

	db := MustOpen(0666, nil)
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		// Create "foo" bucket.
		b, err := tx.CreateBucket([]byte("foo"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				return err
			}
		}

		// Create "bar" bucket.
		b, err = tx.CreateBucket([]byte("bar"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				return err
			}
		}

		// Create "baz" bucket.
		b, err = tx.CreateBucket([]byte("baz"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.DB.Close()

	// Generate expected result.
	exp := "Aggregate statistics for 3 buckets\n\n" +
		"Page count statistics\n" +
		"\tNumber of logical branch pages: 0\n" +
		"\tNumber of physical branch overflow pages: 0\n" +
		"\tNumber of logical leaf pages: 1\n" +
		"\tNumber of physical leaf overflow pages: 0\n" +
		"Tree statistics\n" +
		"\tNumber of keys/value pairs: 111\n" +
		"\tNumber of levels in B+tree: 1\n" +
		"Page size utilization\n" +
		"\tBytes allocated for physical branch pages: 0\n" +
		"\tBytes actually used for branch data: 0 (0%)\n" +
		"\tBytes allocated for physical leaf pages: 4096\n" +
		"\tBytes actually used for leaf data: 1996 (48%)\n" +
		"Bucket statistics\n" +
		"\tTotal number of buckets: 3\n" +
		"\tTotal number on inlined buckets: 2 (66%)\n" +
		"\tBytes used for inlined buckets: 236 (11%)\n"

	// Run the command.
	m := NewMain()
	if err := m.Run("stats", db.Path); err != nil {
		t.Fatal(err)
	} else if m.Stdout.String() != exp {
		t.Fatalf("unexpected stdout:\n\n%s", m.Stdout.String())
	}
}

// Ensure the "buckets" command can print a list of buckets.
func TestBucketsCommand_Run(t *testing.T) {
	db := MustOpen(0666, nil)
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"foo", "bar", "baz"} {
			_, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.DB.Close()

	expected := "bar\nbaz\nfoo\n"

	// Run the command.
	m := NewMain()
	if err := m.Run("buckets", db.Path); err != nil {
		t.Fatal(err)
	} else if actual := m.Stdout.String(); actual != expected {
		t.Fatalf("unexpected stdout:\n\n%s", actual)
	}
}

// Ensure the "keys" command can print a list of keys for a bucket.
func TestKeysCommand_Run(t *testing.T) {
	db := MustOpen(0666, nil)
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"foo", "bar"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("%s-%d", name, i)
				if err := b.Put([]byte(key), []byte{0}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.DB.Close()

	expected := "foo-0\nfoo-1\nfoo-2\n"

	// Run the command.
	m := NewMain()
	if err := m.Run("keys", db.Path, "foo"); err != nil {
		t.Fatal(err)
	} else if actual := m.Stdout.String(); actual != expected {
		t.Fatalf("unexpected stdout:\n\n%s", actual)
	}
}

// Ensure the "get" command can print the value of a key in a bucket.
func TestGetCommand_Run(t *testing.T) {
	db := MustOpen(0666, nil)
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"foo", "bar"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("%s-%d", name, i)
				val := fmt.Sprintf("val-%s-%d", name, i)
				if err := b.Put([]byte(key), []byte(val)); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.DB.Close()

	expected := "val-foo-1\n"

	// Run the command.
	m := NewMain()
	if err := m.Run("get", db.Path, "foo", "foo-1"); err != nil {
		t.Fatal(err)
	} else if actual := m.Stdout.String(); actual != expected {
		t.Fatalf("unexpected stdout:\n\n%s", actual)
	}
}

// Main represents a test wrapper for main.Main that records output.
type Main struct {
	*main.Main
	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	m := &Main{Main: main.NewMain()}
	m.Main.Stdin = &m.Stdin
	m.Main.Stdout = &m.Stdout
	m.Main.Stderr = &m.Stderr
	return m
}

// MustOpen creates a Bolt database in a temporary location.
func MustOpen(mode os.FileMode, options *bolt.Options) *DB {
	// Create temporary path.
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())

	db, err := bolt.Open(f.Name(), mode, options)
	if err != nil {
		panic(err.Error())
	}
	return &DB{DB: db, Path: f.Name()}
}

// DB is a test wrapper for bolt.DB.
type DB struct {
	*bolt.DB
	Path string
}

// Close closes and removes the database.
func (db *DB) Close() error {
	defer os.Remove(db.Path)
	return db.DB.Close()
}
