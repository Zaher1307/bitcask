# Bitcask Description
Bitcask is an append-only key/value data storage engine. The origin of Bitcask is tied to the Riak distributed database system.

**NOTE:** All project specifications and usage are mentioned in the [Official Bitcask Design Paper](https://riak.com/assets/bitcask-intro.pdf)

# Bitcask package

- **Get the package:**
```sh
$ go get github.com/zaher1307/bitcask
```
- **Package:**

| Config Option                                                 | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| ```ReadWrite```| Gives a read and write permissions on the specified datastore. |
| ```ReadOnly```| Gives a read only permission on the specified datastore. |
| ```SyncOnPut```| Forces the data to be written directly to the datastore data files on every write operation, it is prefered to use this option only in cases of very sensitive data since all the data is flushed to the disk and won't be lost on catastrophic damages to the system. |
| ```SyncOnDemand```| Gives the user the control when to flush the data to the disk by using ```Sync```, data is flushed automatically when ```Close``` is called or whenever the process terminates or fails, it is generally good option since it makes write and read operations much more faster. |

| Functions and Methods                                                     | Description                                |
|---------------------------------------------------------------|--------------------------------------------------------|
| ```func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error)```| Open a new or an existing bitcask datastore. |
| ```func (bitcask *Bitcask) Put(key string, value string) error```| Stores a key and a value in the bitcask datastore. |
| ```func (bitcask *Bitcask) Get(key string) (string, error)```| Reads a value by key from a datastore. |
| ```func (bitcask *Bitcask) Delete(key string) error```| Removes a key from the datastore. |
| ```func (bitcask *Bitcask) Close()```| Close a bitcask data store and flushes all pending writes to disk. |
| ```func (bitcask *Bitcask) ListKeys() []string```| Returns list of all keys. |
| ```func (bitcask *Bitcask) Sync() error```| Force any writes to sync to disk. |
| ```func (bitcask *Bitcask) Merge() error```| Reduces the disk usage by removing old and deleted values from the datafiles. Also, produce hintfiles for faster startup. |
| ```func (bitcask *Bitcask) Fold(fun func(string, string, any) any, acc any) any```| Fold over all K/V pairs in a Bitcask datastore.??? Acc Fun is expected to be of the form: F(K,V,Acc0) ??? Acc. |

# Usage of bitcask library

```go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/zaher1307/bitcask"
)

func main() {
	b, err := bitcask.Open("datastore", bitcask.ReadWrite)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("key%d", i+1)
			value := fmt.Sprintf("value%d", i+1)
			err := b.Put(key, value)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}()

	// Sleep time simulate another work to be done by the program
	time.Sleep(time.Second)

	// Perform merge (if needed) at the end of the program
	err = b.Merge()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	
	b.Close()
}
```

# Install bitcask resp server

```sh
$ go install github.com/zaher1307/bitcask/cmd/bitresp@latest
$ bitresp -directory=/path/to/dirctory/of/datastore -port=12345
```
In another terminal window
```sh
$ redis-cli -p 12345
127.0.0.1:12345>
```

**Important Notes:**
- ```Put```, ```Get```, ```Delete``` and ```Sync``` are blocking calls as they deals with I/O, so - whenever possible - it is a good idea to make a goroutine handles these calls and continue on the rest of the program.
- ```Merge``` is also a blocking call like the mentioned above, but more slower since it works on all the data to reduce its size, so it prefered to use it when all writing operations is done. If there's another work to be done by the process, using a goroutine to handle the call will be a good idea as well.
