// Lifecycle and isolation (#20): the always-on Unix-socket daemon plus the
// commands that manage it. `usher serve --socket` is the daemon's foreground
// body (in main.go); start/stop/status background it via a PID file, and
// install/uninstall hand it to launchd so macOS keeps it running across logins
// and crashes. All paths route through the single state dir (config.SocketPath /
// config.PidPath) so every command agrees on one location.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/georgenijo/usher/internal/config"
)

// launchdLabel is the LaunchAgent label and plist basename. It namespaces the
// agent under the install user's ~/Library/LaunchAgents.
const launchdLabel = "com.georgenijo.usher"

// listenUnix creates the daemon's Unix-domain listener, removing any stale
// socket file left by a previous unclean shutdown (net.Listen fails if the path
// already exists). The state dir is created 0700 and the socket tightened to
// 0600 so only the owning user can connect — consistent with the local-only
// ethos. SetUnlinkOnClose(true) is Go's default, so a clean ln.Close() unlinks
// the file; the os.Remove here only handles the crash case.
func listenUnix(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	// Remove a stale socket from a prior crash. ENOENT is fine; anything else
	// (e.g. a real file we can't unlink) is surfaced.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove stale socket %s: %w", path, err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("chmod socket %s: %w", path, err)
	}
	return ln, nil
}

// writePID records pid at path (the state dir's usher.pid). The daemon writes its
// own PID on startup; status reads it back, and a clean shutdown removes it.
func writePID(path string, pid int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o644)
}

// readPID parses the PID file. A missing file returns os.ErrNotExist so callers
// can distinguish "never started" from "unreadable".
func readPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("malformed pid file %s: %w", path, err)
	}
	return pid, nil
}

// removePID deletes the PID file, best-effort (used in a daemon-shutdown defer).
func removePID(path string) { _ = os.Remove(path) }

// processAlive reports whether a process with pid is running. On Unix, signal 0
// is the liveness probe: it delivers nothing but still performs the existence +
// permission check, so ESRCH means dead and a nil/EPERM error means alive.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid) // never fails on Unix
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	// EPERM means the process exists but we may not signal it (still "alive").
	return errors.Is(err, syscall.EPERM)
}

// cmdStart backgrounds the daemon: it re-execs this binary as
// `usher serve --socket [--backend NAME]` detached into its own session, so it
// outlives the launching shell. It refuses to start a second daemon when one is
// already running (PID file + liveness), and prints the new PID and socket path.
func cmdStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	backendName := fs.String("backend", "", "backend to route to (default: configured default)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Already running? Don't spawn a competitor on the same socket.
	if pid, err := readPID(config.PidPath()); err == nil && processAlive(pid) {
		return fmt.Errorf("daemon already running (pid %d); run: usher stop", pid)
	}

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate self: %w", err)
	}
	serveArgs := []string{"serve", "--socket"}
	if *backendName != "" {
		serveArgs = append(serveArgs, "--backend", *backendName)
	}
	cmd := exec.Command(self, serveArgs...)
	// Detach into a new session so the daemon is not killed when the parent shell
	// exits, and is not in our process group (no inherited SIGINT on Ctrl-C).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin = nil
	// Route the daemon's diagnostics to the log dir so a backgrounded start is not
	// silent and a crash leaves a trail.
	logDir := logsDir()
	if err := os.MkdirAll(logDir, 0o755); err == nil {
		if f, ferr := os.OpenFile(filepath.Join(logDir, "usher.out.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); ferr == nil {
			cmd.Stdout = f
		}
		if f, ferr := os.OpenFile(filepath.Join(logDir, "usher.err.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); ferr == nil {
			cmd.Stderr = f
		}
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	// Capture the child PID before Release (which resets cmd.Process.Pid to -1 on
	// some platforms). The daemon also writes its own PID on startup, but record
	// it here too so a `usher status` right after start sees a value immediately.
	pid := cmd.Process.Pid
	_ = writePID(config.PidPath(), pid)
	// Don't Wait: detach and let it run. Release the OS resources for the child.
	_ = cmd.Process.Release()

	fmt.Printf("usher daemon started: pid %d, socket %s\n", pid, config.SocketPath())
	return nil
}

// cmdStop signals the running daemon to terminate and cleans up its socket and
// PID file. It reads the PID file, sends SIGTERM (the daemon's signal handler
// shuts the listener and exits), waits briefly for the process to disappear, and
// removes both runtime files. A missing/stale PID file is reported, not fatal.
func cmdStop(args []string) error {
	pid, err := readPID(config.PidPath())
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("daemon not running (no pid file at %s)", config.PidPath())
		}
		return err
	}
	if !processAlive(pid) {
		// Stale PID file: clean up so the next start is unobstructed.
		removePID(config.PidPath())
		_ = os.Remove(config.SocketPath())
		return fmt.Errorf("daemon not running (stale pid %d cleaned up)", pid)
	}

	p, _ := os.FindProcess(pid) // never errors on Unix
	if err := p.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}

	// Poll for the process to exit (up to ~3s) so cleanup races the daemon's own
	// PID-file removal gracefully.
	for i := 0; i < 30; i++ {
		if !processAlive(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	removePID(config.PidPath())
	_ = os.Remove(config.SocketPath())
	fmt.Printf("usher daemon stopped (pid %d)\n", pid)
	return nil
}

// cmdStatus prints the daemon's state from the PID file and a liveness check:
// stopped (no file), running (file + live process), or a stale pid file (file
// present but the process is gone) so the operator knows to run `usher stop`.
func cmdStatus(args []string) error {
	pid, err := readPID(config.PidPath())
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("usher: stopped")
			return nil
		}
		return err
	}
	if processAlive(pid) {
		fmt.Printf("usher: running pid=%d socket=%s ui=http://%s\n", pid, config.SocketPath(), uiAddr())
		return nil
	}
	fmt.Printf("usher: stale pid file (pid=%d not running); run: usher stop\n", pid)
	return nil
}

// cmdInstall writes the launchd LaunchAgent plist and loads it so macOS starts
// (and keeps restarting) the daemon. The plist points at the absolute current
// binary path so it works regardless of PATH at launch time.
func cmdInstall(args []string) error {
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	backendName := fs.String("backend", "", "backend to route to (default: configured default)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate self: %w", err)
	}

	if err := os.MkdirAll(logsDir(), 0o755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	plistPath, err := plistPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}

	data, err := renderPlist(self, *backendName)
	if err != nil {
		return fmt.Errorf("render plist: %w", err)
	}
	if err := os.WriteFile(plistPath, data, 0o644); err != nil {
		return fmt.Errorf("write plist: %w", err)
	}

	// load -w marks the agent enabled and starts it (RunAtLoad=true). launchctl is
	// the supported macOS path; its failure is surfaced verbatim.
	if out, err := exec.Command("launchctl", "load", "-w", plistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl load %s: %w: %s", plistPath, err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("installed launchd agent %s -> %s\n", launchdLabel, plistPath)
	return nil
}

// cmdUninstall unloads the LaunchAgent and removes its plist, the inverse of
// cmdInstall. A missing plist is reported, not fatal.
func cmdUninstall(args []string) error {
	plistPath, err := plistPath()
	if err != nil {
		return err
	}
	if _, err := os.Stat(plistPath); os.IsNotExist(err) {
		return fmt.Errorf("not installed (no plist at %s)", plistPath)
	}

	// unload -w stops and disables the agent. Tolerate a non-zero exit (e.g. it
	// was already unloaded) but still remove the plist so the install is gone.
	if out, err := exec.Command("launchctl", "unload", "-w", plistPath).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "usher: warning: launchctl unload: %v: %s\n", err, strings.TrimSpace(string(out)))
	}
	if err := os.Remove(plistPath); err != nil {
		return fmt.Errorf("remove plist: %w", err)
	}
	fmt.Printf("uninstalled launchd agent %s\n", launchdLabel)
	return nil
}

// logsDir is where the daemon's stdout/stderr land under both the `usher start`
// and launchd paths.
func logsDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(config.StateDir(), "logs")
	}
	return filepath.Join(home, "Library", "Logs", "usher")
}

// plistPath is the LaunchAgent plist location for the current user.
func plistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	return filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist"), nil
}
