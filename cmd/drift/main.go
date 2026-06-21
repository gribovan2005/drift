// Command drift is the Drift CLI. It loads declarative YAML jobs and runs,
// validates, or visualises them.
//
//	drift run <job.yaml> [--ui]                       build + run the pipeline
//	drift validate <job.yaml>                         parse + validate only
//	drift graph <job.yaml> [--format mermaid|dot|json] print the static lineage
//	drift list                                        list registered refs
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/andrejgribov/drift/internal/dotenv"
	"github.com/andrejgribov/drift/pkg/ai"
	"github.com/andrejgribov/drift/pkg/job"
	"github.com/andrejgribov/drift/pkg/lineage"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/runner"
	"github.com/andrejgribov/drift/pkg/schema"
	"github.com/andrejgribov/drift/pkg/web"
)

// Build metadata, injected at release time via -ldflags by GoReleaser.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	dotenv.Load()

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "run":
		err = cmdRun(args)
	case "validate":
		err = cmdValidate(args)
	case "graph":
		err = cmdGraph(args)
	case "list":
		err = cmdList(args)
	case "serve":
		err = cmdServe(args)
	case "version", "--version", "-v":
		fmt.Printf("drift %s (commit %s, built %s)\n", version, commit, date)
		return
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "drift: unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "drift: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `drift — declarative streaming pipelines

Usage:
  drift run <job.yaml> [--ui] [--lineage <file>]       build and run a job
  drift validate <job.yaml>                            parse and validate a job
  drift graph <job.yaml> [--format mermaid|dot|json]   print static lineage
  drift list                                           list registered refs
  drift serve --jobs-dir <dir> [--addr :8080]          web builder + control plane
  drift version                                        print version and build info

Run with ANTHROPIC_API_KEY set to enable the AI debugger in --ui / serve mode.
Set DRIFT_AUTH_TOKEN to require a bearer token on the serve API.
`)
}

// splitArgs separates positional (non-flag) args from flag args so flags may
// appear before or after the job path. The first positional is returned
// separately; everything else is treated as flags.
func splitArgs(args []string) (path string, flags []string) {
	for _, a := range args {
		if path == "" && len(a) > 0 && a[0] != '-' {
			path = a
			continue
		}
		flags = append(flags, a)
	}
	return path, flags
}

// loadJob reads and loads a job file at the given path.
func loadJob(path string) (*job.Built, error) {
	if path == "" {
		return nil, fmt.Errorf("missing <job.yaml> argument")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	b, err := job.Load(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func cmdValidate(args []string) error {
	path, _ := splitArgs(args)
	b, err := loadJob(path)
	if err != nil {
		return err
	}
	fmt.Printf("OK: %s — job %q, %d stage(s)\n", path, b.Spec.Name, len(b.Stages))
	return nil
}

func cmdGraph(args []string) error {
	path, flags := splitArgs(args)
	fs := flag.NewFlagSet("graph", flag.ContinueOnError)
	format := fs.String("format", "mermaid", "output format: mermaid|dot|json")
	if err := fs.Parse(flags); err != nil {
		return err
	}
	b, err := loadJob(path)
	if err != nil {
		return err
	}
	out, err := b.Graph(*format)
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}

func cmdList(_ []string) error {
	ops, sources, sinks := job.RegisteredRefs()
	sort.Strings(ops)
	sort.Strings(sources)
	sort.Strings(sinks)

	printRefs("operators", ops)
	printRefs("sources", sources)
	printRefs("sinks", sinks)
	return nil
}

func printRefs(kind string, names []string) {
	fmt.Printf("registered %s:\n", kind)
	if len(names) == 0 {
		fmt.Println("  (none)")
		return
	}
	for _, n := range names {
		fmt.Printf("  ref:%s\n", n)
	}
}

func cmdRun(args []string) error {
	path, flags := splitArgs(args)
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	ui := fs.Bool("ui", false, "serve the web dashboard on :8080")
	lineagePath := fs.String("lineage", "", "track record-level lineage and write the graph as JSON to this path on exit")
	if err := fs.Parse(flags); err != nil {
		return err
	}
	b, err := loadJob(path)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var tr *lineage.Tracker
	var popts []pipeline.Option
	if *lineagePath != "" {
		tr = lineage.New()
		popts = append(popts, pipeline.WithLineage(tr))
	}

	p := b.Pipeline(popts...)

	// Persist the lineage graph after the run, however it ends.
	defer func() {
		if tr == nil {
			return
		}
		data, err := tr.Export()
		if err != nil {
			fmt.Fprintf(os.Stderr, "drift: export lineage: %v\n", err)
			return
		}
		if err := os.WriteFile(*lineagePath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "drift: write lineage: %v\n", err)
			return
		}
		fmt.Printf("Wrote lineage graph (%d nodes) to %s\n", tr.Len(), *lineagePath)
	}()

	if *ui {
		reg := schema.NewRegistry()
		dbg := ai.New("", "") // reads ANTHROPIC_API_KEY from env
		srv := web.New(":8080", p, reg, dbg)

		runErr := make(chan error, 1)
		go func() { runErr <- p.Run(ctx) }()

		fmt.Printf("Running job %q with dashboard on http://localhost:8080 (Ctrl+C to stop)\n", b.Spec.Name)
		if err := srv.ListenAndServe(ctx); err != nil {
			return err
		}
		if err := <-runErr; err != nil && err != context.Canceled {
			return err
		}
		return nil
	}

	fmt.Printf("Running job %q (Ctrl+C to stop)\n", b.Spec.Name)
	if err := p.Run(ctx); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

// cmdServe starts the control plane: a runner over a folder of YAML jobs plus the
// web builder/dashboard. Jobs are built, saved, run, and stopped from the UI.
func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	jobsDir := fs.String("jobs-dir", "./jobs", "directory of YAML job files")
	addr := fs.String("addr", ":8080", "HTTP listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := runner.NewStore(*jobsDir)
	if err != nil {
		return err
	}
	r := runner.New(store)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer r.StopAll()

	reg := schema.NewRegistry()
	dbg := ai.New("", "") // reads ANTHROPIC_API_KEY from env
	srv := web.New(*addr, nil, reg, dbg,
		web.WithRunner(r),
		web.WithAuth(os.Getenv("DRIFT_AUTH_TOKEN")),
	)

	fmt.Printf("Drift control plane on http://localhost%s (jobs: %s, Ctrl+C to stop)\n", *addr, store.Dir())
	return srv.ListenAndServe(ctx)
}
