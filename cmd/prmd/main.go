// Command prmd is the PRM server.
//
// Usage (note: flags come BEFORE positional args, per stdlib flag conventions):
//
//	prmd serve [--addr :6697] [--storage sqlite:./prm.db] [--dev | --cert FILE --key FILE]
//	prmd admin create-tenant [--display-name NAME] [--storage URL] <slug>
//	prmd admin create-account [--password PW] [--bot] [--display-name NAME] [--storage URL] <tenant-slug> <username>
//	prmd admin generate-cert [--out-dir ./certs] <host>
//	prmd admin list-tenants [--storage URL]
//
// The --storage flag defaults to sqlite:./prm.db. Use
// "postgres://user:pass@host:5432/db?sslmode=..." for the Postgres backend
// (stub in slice 1).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/open"
)

const version = "0.1.0-slice1"

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		os.Exit(cmdServe(os.Args[2:]))
	case "admin":
		if len(os.Args) < 3 {
			usage(os.Stderr)
			os.Exit(2)
		}
		switch os.Args[2] {
		case "create-tenant":
			os.Exit(cmdCreateTenant(os.Args[3:]))
		case "create-account":
			os.Exit(cmdCreateAccount(os.Args[3:]))
		case "generate-cert":
			os.Exit(cmdGenerateCert(os.Args[3:]))
		case "list-tenants":
			os.Exit(cmdListTenants(os.Args[3:]))
		default:
			usage(os.Stderr)
			os.Exit(2)
		}
	case "-h", "--help", "help":
		usage(os.Stdout)
		os.Exit(0)
	case "-v", "--version", "version":
		fmt.Println("prmd", version)
		os.Exit(0)
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
}

func usage(w *os.File) {
	fmt.Fprintf(w, `prmd: PRM server (version %s)

Usage (flags come BEFORE positional args):
  prmd serve [flags]
  prmd admin create-tenant [flags] <slug>
  prmd admin create-account [flags] <tenant-slug> <username>
  prmd admin generate-cert [flags] <host>
  prmd admin list-tenants [flags]
  prmd version

Run any subcommand with -h to see its flags.
`, version)
}

func cmdServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":6697", "TCP address to listen on")
	storageURL := fs.String("storage", "sqlite:./prm.db", "storage backend URL")
	dev := fs.Bool("dev", false, "use a self-signed certificate for localhost (DEV ONLY)")
	certFile := fs.String("cert", "", "path to TLS certificate (PEM)")
	keyFile := fs.String("key", "", "path to TLS key (PEM)")
	_ = fs.Parse(args)

	log := newLogger()
	st, err := bringUpStorage(*storageURL, log)
	if err != nil {
		log.Error("storage open", "err", err)
		return 1
	}
	defer st.Close()

	tlsCfg, err := loadTLS(*dev, *certFile, *keyFile)
	if err != nil {
		log.Error("tls config", "err", err)
		return 1
	}

	srv, err := server.New(server.Config{
		Addr:      *addr,
		TLSConfig: tlsCfg,
		Store:     st,
		Logger:    log,
		Name:      "prmd",
		Version:   version,
	})
	if err != nil {
		log.Error("server new", "err", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Serve(ctx); err != nil {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}

func loadTLS(dev bool, certFile, keyFile string) (*tls.Config, error) {
	if dev {
		return server.DevTLSConfig("localhost")
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("either --dev or both --cert and --key are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func cmdCreateTenant(args []string) int {
	fs := flag.NewFlagSet("create-tenant", flag.ExitOnError)
	displayName := fs.String("display-name", "", "human-readable display name (defaults to slug)")
	storageURL := fs.String("storage", "sqlite:./prm.db", "storage backend URL")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: prmd admin create-tenant <slug> [flags]")
		return 2
	}
	slug := fs.Arg(0)
	display := *displayName
	if display == "" {
		display = slug
	}

	log := newLogger()
	st, err := bringUpStorage(*storageURL, log)
	if err != nil {
		log.Error("storage open", "err", err)
		return 1
	}
	defer st.Close()

	t := &storage.Tenant{Slug: slug, DisplayName: display}
	if err := st.CreateTenant(context.Background(), t); err != nil {
		log.Error("create tenant", "err", err)
		return 1
	}
	fmt.Printf("Created tenant\n  ID:    %s\n  Slug:  %s\n  Name:  %s\n", t.ID, t.Slug, t.DisplayName)
	return 0
}

func cmdCreateAccount(args []string) int {
	fs := flag.NewFlagSet("create-account", flag.ExitOnError)
	password := fs.String("password", "", "password (required; will read from terminal if empty in a future pass)")
	bot := fs.Bool("bot", false, "create as a bot account")
	displayName := fs.String("display-name", "", "display name (defaults to username)")
	storageURL := fs.String("storage", "sqlite:./prm.db", "storage backend URL")
	_ = fs.Parse(args)
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: prmd admin create-account <tenant-slug> <username> [flags]")
		return 2
	}
	tenantSlug := fs.Arg(0)
	username := fs.Arg(1)
	if *password == "" {
		fmt.Fprintln(os.Stderr, "--password is required for slice 1 (interactive prompt comes later)")
		return 2
	}
	display := *displayName
	if display == "" {
		display = username
	}
	accountType := storage.AccountHuman
	if *bot {
		accountType = storage.AccountBot
	}

	log := newLogger()
	st, err := bringUpStorage(*storageURL, log)
	if err != nil {
		log.Error("storage open", "err", err)
		return 1
	}
	defer st.Close()

	ten, err := st.GetTenantBySlug(context.Background(), tenantSlug)
	if err != nil {
		log.Error("lookup tenant", "err", err)
		return 1
	}

	hash, salt, params, err := auth.HashPassword(*password)
	if err != nil {
		log.Error("hash password", "err", err)
		return 1
	}
	acc := &storage.Account{
		Username:       username,
		DisplayName:    display,
		Type:           accountType,
		PasswordHash:   hash,
		PasswordSalt:   salt,
		PasswordParams: params,
	}
	if err := st.CreateAccount(context.Background(), ten.ID, acc); err != nil {
		log.Error("create account", "err", err)
		return 1
	}
	fmt.Printf("Created account\n  ID:       %s\n  Tenant:   %s (%s)\n  Username: %s\n  Display:  %s\n  Type:     %s\n",
		acc.ID, ten.Slug, ten.ID, acc.Username, acc.DisplayName, acc.Type)
	return 0
}

func cmdGenerateCert(args []string) int {
	fs := flag.NewFlagSet("generate-cert", flag.ExitOnError)
	outDir := fs.String("out-dir", "./certs", "directory to write cert.pem and key.pem")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: prmd admin generate-cert <host> [--out-dir DIR]")
		return 2
	}
	host := fs.Arg(0)
	if _, err := server.GenerateDevCert(host, *outDir); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	fmt.Printf("Wrote self-signed cert + key for %q to %s\n  NOTE: dev-only; do not use in production.\n", host, *outDir)
	return 0
}

func cmdListTenants(args []string) int {
	fs := flag.NewFlagSet("list-tenants", flag.ExitOnError)
	storageURL := fs.String("storage", "sqlite:./prm.db", "storage backend URL")
	_ = fs.Parse(args)

	log := newLogger()
	st, err := bringUpStorage(*storageURL, log)
	if err != nil {
		log.Error("storage open", "err", err)
		return 1
	}
	defer st.Close()
	list, err := st.ListTenants(context.Background())
	if err != nil {
		log.Error("list tenants", "err", err)
		return 1
	}
	if len(list) == 0 {
		fmt.Println("(no tenants)")
		return 0
	}
	fmt.Printf("%-36s  %-16s  %-8s  %s\n", "ID", "SLUG", "STATUS", "DISPLAY NAME")
	for _, t := range list {
		fmt.Printf("%-36s  %-16s  %-8s  %s\n", t.ID, t.Slug, t.Status, t.DisplayName)
	}
	return 0
}

func bringUpStorage(url string, log *slog.Logger) (storage.Store, error) {
	st, err := open.Store(url)
	if err != nil {
		return nil, err
	}
	if err := st.Migrate(context.Background()); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	log.Info("storage ready", "url", url)
	return st, nil
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
