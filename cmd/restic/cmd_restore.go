package main

import (
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filter"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/termstatus"
	tomb "gopkg.in/tomb.v2"

	"github.com/spf13/cobra"
)

var cmdRestore = &cobra.Command{
	Use:   "restore [flags] snapshotID",
	Short: "Extract the data from a snapshot",
	Long: `
The "restore" command extracts the data from a snapshot from the repository to
a directory.

The special snapshot "latest" can be used to restore the latest snapshot in the
repository.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		var t tomb.Tomb
		term := termstatus.New(globalOptions.stdout, globalOptions.stderr, globalOptions.Quiet)
		t.Go(func() error { term.Run(t.Context(globalOptions.ctx)); return nil })

		prevStdout, prevStderr := globalOptions.stdout, globalOptions.stderr
		defer func() {
			globalOptions.stdout, globalOptions.stderr = prevStdout, prevStderr
		}()
		pm := ui.NewTermstatusProgressUI(term, globalOptions.verbosity)
		defer pm.Finish()
		globalOptions.stdout, globalOptions.stderr = pm.Stdout(), pm.Stderr()
		t.Go(func() error { return pm.Run(t.Context(globalOptions.ctx)) })

		return runRestore(restoreOptions, globalOptions, pm, args)
	},
}

// RestoreOptions collects all options for the restore command.
type RestoreOptions struct {
	Exclude []string
	Include []string
	Target  string
	Host    string
	Paths   []string
	Tags    restic.TagLists
	Verify  bool
}

var restoreOptions RestoreOptions

func init() {
	cmdRoot.AddCommand(cmdRestore)

	flags := cmdRestore.Flags()
	flags.StringArrayVarP(&restoreOptions.Exclude, "exclude", "e", nil, "exclude a `pattern` (can be specified multiple times)")
	flags.StringArrayVarP(&restoreOptions.Include, "include", "i", nil, "include a `pattern`, exclude everything else (can be specified multiple times)")
	flags.StringVarP(&restoreOptions.Target, "target", "t", "", "directory to extract data to")

	flags.StringVarP(&restoreOptions.Host, "host", "H", "", `only consider snapshots for this host when the snapshot ID is "latest"`)
	flags.Var(&restoreOptions.Tags, "tag", "only consider snapshots which include this `taglist` for snapshot ID \"latest\"")
	flags.StringArrayVar(&restoreOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path` for snapshot ID \"latest\"")
	flags.BoolVar(&restoreOptions.Verify, "verify", false, "verify restored files content")
}

func runRestore(opts RestoreOptions, gopts GlobalOptions, pm ui.ProgressUI, args []string) error {
	ctx := gopts.ctx

	switch {
	case len(args) == 0:
		return errors.Fatal("no snapshot ID specified")
	case len(args) > 1:
		return errors.Fatalf("more than one snapshot ID specified: %v", args)
	}

	if opts.Target == "" {
		return errors.Fatal("please specify a directory to restore to (--target)")
	}

	if len(opts.Exclude) > 0 && len(opts.Include) > 0 {
		return errors.Fatal("exclude and include patterns are mutually exclusive")
	}

	snapshotIDString := args[0]

	debug.Log("restore %v to %v", snapshotIDString, opts.Target)

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	if !gopts.NoLock {
		lock, err := lockRepo(repo)
		defer unlockRepo(lock)
		if err != nil {
			return err
		}
	}

	err = repo.LoadIndex(ctx)
	if err != nil {
		return err
	}

	var id restic.ID

	if snapshotIDString == "latest" {
		id, err = restic.FindLatestSnapshot(ctx, repo, opts.Paths, opts.Tags, opts.Host)
		if err != nil {
			Exitf(1, "latest snapshot for criteria not found: %v Paths:%v Host:%v", err, opts.Paths, opts.Host)
		}
	} else {
		id, err = restic.FindSnapshot(repo, snapshotIDString)
		if err != nil {
			Exitf(1, "invalid id %q: %v", snapshotIDString, err)
		}
	}

	res, err := restorer.NewRestorer(repo, id)
	if err != nil {
		Exitf(2, "creating restorer failed: %v\n", err)
	}

	totalErrors := 0
	res.Error = func(location string, err error) error {
		Warnf("ignoring error for %s: %s\n", location, err)
		totalErrors++
		return nil
	}

	selectExcludeFilter := func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		matched, _, err := filter.List(opts.Exclude, item)
		if err != nil {
			Warnf("error for exclude pattern: %v", err)
		}

		// An exclude filter is basically a 'wildcard but foo',
		// so even if a childMayMatch, other children of a dir may not,
		// therefore childMayMatch does not matter, but we should not go down
		// unless the dir is selected for restore
		selectedForRestore = !matched
		childMayBeSelected = selectedForRestore && node.Type == "dir"

		return selectedForRestore, childMayBeSelected
	}

	selectIncludeFilter := func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		matched, childMayMatch, err := filter.List(opts.Include, item)
		if err != nil {
			Warnf("error for include pattern: %v", err)
		}

		selectedForRestore = matched
		childMayBeSelected = childMayMatch && node.Type == "dir"

		return selectedForRestore, childMayBeSelected
	}

	if len(opts.Exclude) > 0 {
		res.SelectFilter = selectExcludeFilter
	} else if len(opts.Include) > 0 {
		res.SelectFilter = selectIncludeFilter
	}

	Verbosef("restoring %s to %s\n", res.Snapshot(), opts.Target)

	err = res.RestoreTo(ctx, pm, opts.Target, opts.Verify)
	if totalErrors > 0 {
		Verbosef("There were %d errors\n", totalErrors)
	}

	return err
}
