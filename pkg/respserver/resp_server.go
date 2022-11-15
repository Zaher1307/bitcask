package respserver

import (
	"errors"
	"log"

	"github.com/tidwall/resp"
	"github.com/zaher1307/bitcask/pkg/bitcask"
)

func StartServer() error {
	bitcask, err := bitcask.Open("./resp_server_datastore", bitcask.ReadWrite)
	if err != nil {
		return err
	}
    defer bitcask.Close()

	s := resp.NewServer()

	s.HandleFunc("set", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
		} else {
			err = bitcask.Put(args[1].String(), args[2].String())
			if err != nil {
				conn.WriteError(errors.New("ERR cannot set key to value in this store"))
			}
			conn.WriteSimpleString("OK")
		}
		return true
	})

	s.HandleFunc("get", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			s, err := bitcask.Get(args[1].String())
			if err != nil {
				conn.WriteNull()
			} else {
				conn.WriteString(s)
			}
		}
		return true
	})

	s.HandleFunc("delete", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			err := bitcask.Delete(args[1].String())
			if err != nil {
				conn.WriteError(errors.New("ERR cannot delete this item"))
			} else {
				conn.WriteSimpleString("OK")
			}
		}
		return true
	})
	if err := s.ListenAndServe(":6379"); err != nil {
		log.Fatal(err)
	}
	return nil
}
