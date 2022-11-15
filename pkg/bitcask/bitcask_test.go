package bitcask

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
)

var testBitcaskPath = path.Join("testing_dir")

func TestOpen(t *testing.T) {
	t.Run("open new bitcask with read and write permission", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
			t.Errorf("Expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open new bitcask with sync_on_put option", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite, SyncOnPut)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
			t.Errorf("Expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open new bitcask with default options", func(t *testing.T) {
		_, err := Open(testBitcaskPath)
		assertError(t, err, "open testing_dir: no such file or directory")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open existing bitcask with write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Put("key12", "value12345")
		b1.Close()

		b2, _ := Open(testBitcaskPath, ReadWrite)

		want := "value12345"
		got, _ := b2.Get("key12")
		b2.Close()

		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("two readers in the same bitcask at the same time", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Put("key2", "value2")
		b1.Put("key3", "value3")
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		b3, _ := Open(testBitcaskPath)

		want := "value2"
		got, _ := b2.Get("key2")
		assertString(t, got, want)
		b2.Close()

		got, _ = b3.Get("key2")
		assertString(t, got, want)
		b3.Close()
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open existing bitcask with hint files in it", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)

		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key%d", i+1)
			value := fmt.Sprintf("value%d", i+1)
			b1.Put(key, value)
		}
		b1.Merge()
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		got, _ := b2.Get("key50")
		want := "value50"

		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open bitcask with writer exists in it", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite)
		_, err := Open(testBitcaskPath)

		assertError(t, err, "access denied: datastore is locked")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open bitcask failed", func(t *testing.T) {
		// create a directory that cannot be openned since it has no execute permission
		os.MkdirAll(path.Join("no open dir"), 000)

		want := "open no open dir: permission denied"
		_, err := Open("no open dir")

		assertError(t, err, want)
		os.RemoveAll("no open dir")
	})
}

func TestGet(t *testing.T) {
	t.Run("get existing value", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
		b.Put("key12", "value12345")

		got, _ := b.Get("key12")
		want := "value12345"

		assertString(t, got, want)
		b.Close()
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("get not existing value", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite)

		want := "unknown key: key does not exist"
		_, err := b.Get("unknown key")

		assertError(t, err, want)
		os.RemoveAll(testBitcaskPath)
	})
}

func TestPut(t *testing.T) {
	t.Run("put values with writer permission", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite)
		b.Put("key12", "value12345")

		want := "value12345"
		got, _ := b.Get("key12")

		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("put with no write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		err := b2.Put("key12", "value12345")

		assertError(t, err, "Put: require write permission")
		os.RemoveAll(testBitcaskPath)
	})
}

func TestDelete(t *testing.T) {
	t.Run("delete existing key", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
		b.Put("key12", "value12345")
		b.Delete("key12")
		_, err := b.Get("key12")
		assertError(t, err, "key12: key does not exist")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("delete not existing key", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)
		err := b.Delete("key12")
		assertError(t, err, "key12: key does not exist")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("delete with no write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		err := b2.Delete("key12")
		assertError(t, err, "Delete: require write permission")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("check if loaded delete is detected", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
		b1.Put("key12", "value12345")
		b1.Delete("key12")
		b1.Close()

		b2, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
		_, err := b2.Get("key12")
		want := "key12: key does not exist"
		assertError(t, err, want)
		os.RemoveAll(testBitcaskPath)
	})
}

func TestListkeys(t *testing.T) {
	b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)

	key := "key12"
	value := "value12345"
	b.Put(key, value)

	want := []string{"key12"}
	got := b.ListKeys()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got:\n%v\nwant:\n%v", got, want)
	}
	os.RemoveAll(testBitcaskPath)
}

func TestFold(t *testing.T) {
	b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)

	for i := 0; i < 10; i++ {
		b.Put(fmt.Sprint(i+1), fmt.Sprint(i+1))
	}

	want := 110
	got := b.Fold(func(s1, s2 string, a any) any {
		acc, _ := a.(int)
		k, _ := strconv.Atoi(s1)
		v, _ := strconv.Atoi(s2)

		return acc + k + v
	}, 0)

	if got != want {
		t.Errorf("got:%d, want:%d", got, want)
	}
	os.RemoveAll(testBitcaskPath)
}

func TestMerge(t *testing.T) {
	t.Run("merge with write permission", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite)

		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("key%d", i+1)
			value := fmt.Sprintf("value%d", i+1)
			b.Put(key, value)
		}
		b.Merge()
		want := "value100"
		got, _ := b.Get("key100")

		b.Close()
		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("with no write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Close()

		b2, _ := Open(testBitcaskPath)

		err := b2.Merge()
		want := "Merge: require write permission"

		assertError(t, err, want)
		os.RemoveAll(testBitcaskPath)
	})
}

func TestSync(t *testing.T) {
	t.Run("put with sync on demand option is set", func(t *testing.T) {
		b, _ := Open(testBitcaskPath, ReadWrite)
		b.Put("key12", "value12345")
		b.Sync()

		want := "value12345"
		got, _ := b.Get("key12")

		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("sync with no write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		err := b2.Sync()

		assertError(t, err, "Sync: require write permission")
		os.RemoveAll(testBitcaskPath)
	})
}

func assertError(t testing.TB, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected Error %q", want)
	}
	assertString(t, err.Error(), want)
}

func assertString(t testing.TB, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

