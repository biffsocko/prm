// Command prmd is the PRM server.
//
// Usage:
//
//	prmd serve [flags]
//	prmd admin create-tenant <slug> [--display-name <name>]
//	prmd admin create-account <tenant-slug> <username> [--password <pw>] [--bot]
//	prmd admin generate-cert <host> [--out-dir ./certs]
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		fmt.Fprintln(os.Stderr, "serve: not implemented yet")
		os.Exit(1)
	case "admin":
		fmt.Fprintln(os.Stderr, "admin: not implemented yet")
		os.Exit(1)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `prmd: PRM server

Usage:
  prmd serve [flags]
  prmd admin create-tenant <slug>
  prmd admin create-account <tenant-slug> <username>
  prmd admin generate-cert <host>`)
}
