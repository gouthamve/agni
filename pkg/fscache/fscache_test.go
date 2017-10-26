package fscache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddNonExistentFile(t *testing.T) {
	getter := func(key string) ([]byte, error) {
		return []byte{1, 2, 3}, nil
	}
	tmpdir, err := ioutil.TempDir("", "fscache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	c, err := NewCache(1, tmpdir, getter)
	require.NoError(t, err)

	ok, err := c.Add("whoopsie", "./test/bleh")
	require.False(t, ok)
	require.Error(t, err)
}

func TestAdd(t *testing.T) {
	getter := func(key string) ([]byte, error) {
		return []byte(key), nil
	}
	tmpdir, err := ioutil.TempDir("", "fscache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	c, err := NewCache(1, tmpdir, getter)
	require.NoError(t, err)

	file := filepath.Join(tmpdir, "file")
	str := "blah-bleh"
	err = ioutil.WriteFile(file, []byte(str), 0777)
	require.NoError(t, err)

	ok, err := c.Add(file, file)
	require.False(t, ok)
	require.NoError(t, err)

	v, err := c.Get(file)
	require.NoError(t, err)
	require.Equal(t, []byte(str), v.B)
	require.Equal(t, file, v.f.Name())

	v, err = c.Get("non-existent")
	require.NoError(t, err)
	require.Equal(t, []byte("non-existent"), v.B)
	require.Equal(t, filepath.Join(tmpdir, "non-existent"), v.f.Name())
}

func TestAddOverwrite(t *testing.T) {
	getter := func(key string) ([]byte, error) {
		return []byte(key), nil
	}
	tmpdir, err := ioutil.TempDir("", "fscache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	c, err := NewCache(1, tmpdir, getter)
	require.NoError(t, err)

	file := filepath.Join(tmpdir, "file")
	str := "blah-bleh"
	err = ioutil.WriteFile(file, []byte(str), 0777)
	require.NoError(t, err)

	ok, err := c.Add(file, file)
	require.False(t, ok)
	require.NoError(t, err)

	str = "blah-bloop"
	err = ioutil.WriteFile(file, []byte(str), 0777)
	require.NoError(t, err)
	ok, err = c.Add(file, file)
	require.False(t, ok)
	require.NoError(t, err)

	v, err := c.Get(file)
	require.NoError(t, err)
	require.Equal(t, []byte(str), v.B)
	require.Equal(t, file, v.f.Name())
}

func TestEvictRemoval(t *testing.T) {
	getter := func(key string) ([]byte, error) {
		return []byte(key), nil
	}
	tmpdir, err := ioutil.TempDir("", "fscache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	c, err := NewCache(1, tmpdir, getter)
	require.NoError(t, err)

	// Add the first key.
	file := filepath.Join(tmpdir, "file")
	str := "blah-bleh"
	err = ioutil.WriteFile(file, []byte(str), 0777)
	require.NoError(t, err)
	ok, err := c.Add(file, file)
	require.False(t, ok)
	require.NoError(t, err)

	// Add another key.
	anotherFile := filepath.Join(tmpdir, "file2")
	str = "blah-bloop"
	err = ioutil.WriteFile(anotherFile, []byte(str), 0777)
	require.NoError(t, err)
	ok, err = c.Add(anotherFile, anotherFile)
	require.True(t, ok)
	require.NoError(t, err)

	_, ok = c.lru.Get(file)
	require.False(t, ok)

	v, err := c.Get(anotherFile)
	require.NoError(t, err)
	require.Equal(t, []byte(str), v.B)
	require.Equal(t, anotherFile, v.f.Name())

	f, err := os.Open(file)
	require.Error(t, err)
	require.Nil(t, f)
}
