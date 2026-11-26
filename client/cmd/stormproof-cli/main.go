// Command stormproof-cli talks to a StormProof cluster.
//
//	stormproof-cli -endpoints localhost:7001,localhost:7002 put foo bar
//	stormproof-cli -endpoints localhost:7001 get foo
//	stormproof-cli delete foo
//	stormproof-cli cas foo old new      # set foo to new if it is old
//	stormproof-cli cas -absent foo new  # set foo to new if it doesn't exist
//
// Any server will do as an endpoint: the client follows the servers' hints
// to the leader. -endpoints defaults to $STORMPROOF_ENDPOINTS.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/20001172024/stormproof/client"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "stormproof-cli:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fl := flag.NewFlagSet("stormproof-cli", flag.ContinueOnError)
	endpoints := fl.String("endpoints", os.Getenv("STORMPROOF_ENDPOINTS"), "comma-separated server addresses")
	timeout := fl.Duration("timeout", 10*time.Second, "give up after this long")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *endpoints == "" {
		return errors.New("no endpoints: use -endpoints or STORMPROOF_ENDPOINTS")
	}
	cl, err := client.New(strings.Split(*endpoints, ","), client.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	rest := fl.Args()
	if len(rest) == 0 {
		return errors.New("usage: stormproof-cli [flags] get, put, delete or cas")
	}
	switch cmd, a := rest[0], rest[1:]; cmd {
	case "get":
		if len(a) != 1 {
			return errors.New("usage: get KEY")
		}
		v, found, err := cl.Get(ctx, a[0])
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%s: not found", a[0])
		}
		fmt.Fprintln(out, string(v))
	case "put":
		if len(a) != 2 {
			return errors.New("usage: put KEY VALUE")
		}
		if err := cl.Put(ctx, a[0], []byte(a[1])); err != nil {
			return err
		}
		fmt.Fprintln(out, "OK")
	case "delete":
		if len(a) != 1 {
			return errors.New("usage: delete KEY")
		}
		existed, err := cl.Delete(ctx, a[0])
		if err != nil {
			return err
		}
		fmt.Fprintln(out, map[bool]string{true: "deleted", false: "not found"}[existed])
	case "cas":
		var r client.CASResult
		switch {
		case len(a) == 3 && a[0] == "-absent":
			r, err = cl.CompareAndSwap(ctx, a[1], nil, true, []byte(a[2]))
		case len(a) == 3:
			r, err = cl.CompareAndSwap(ctx, a[0], []byte(a[1]), false, []byte(a[2]))
		default:
			return errors.New("usage: cas KEY EXPECTED NEW | cas -absent KEY NEW")
		}
		if err != nil {
			return err
		}
		if r.Swapped {
			fmt.Fprintln(out, "swapped")
		} else if r.Found {
			fmt.Fprintf(out, "not swapped: current value is %q\n", r.Current)
		} else {
			fmt.Fprintln(out, "not swapped: key does not exist")
		}
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}
