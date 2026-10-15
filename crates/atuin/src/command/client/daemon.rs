use std::fs;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use atuin_client::database::Sqlite;
use atuin_client::history::{History, HistoryId};
use atuin_client::record::sqlite_store::SqliteStore;
use atuin_client::settings::Settings;
#[cfg(unix)]
use atuin_daemon::agent::AgentWorkingDir;
use atuin_daemon::agent::{ActiveAgent, PidfileWaitError, StopError};
use atuin_daemon::client::{DaemonClientErrorKind, HistoryClient, classify_error};
use atuin_daemon::grpc::{VersionMismatch, VersionedReply};
use atuin_daemon::pidfile::{self, PidfileGuard};
use clap::Subcommand;
#[cfg(unix)]
use daemonix::Daemonize;
use eyre::{Result, WrapErr, bail};

use crate::i18n::fl;

#[derive(clap::Args, Debug)]
pub struct Cmd {
    #[arg(long, hide = true, help = fl!("arg-daemon-daemonize"))]
    daemonize: bool,

    #[arg(long, help = fl!("arg-daemon-show-logs"))]
    show_logs: bool,

    #[command(subcommand)]
    subcmd: Option<SubCmd>,
}

#[derive(Subcommand, Debug)]
#[command(infer_subcommands = true)]
pub enum SubCmd {
    #[command(about = fl!("cmd-daemon-start"))]
    Start {
        #[arg(long, hide = true)]
        daemonize: bool,

        #[arg(long, help = fl!("arg-daemon-show-logs"))]
        show_logs: bool,

        #[arg(long, help = fl!("arg-daemon-start-force"))]
        force: bool,
    },

    #[command(about = fl!("cmd-daemon-status"))]
    Status,

    #[command(about = fl!("cmd-daemon-stop"))]
    Stop,

    #[command(about = fl!("cmd-daemon-restart"))]
    Restart,
}

impl Cmd {
    /// Returns `true` when the process should daemonize before creating the
    /// async runtime or opening any database connections.
    #[cfg(unix)]
    pub fn should_daemonize(&self) -> bool {
        match &self.subcmd {
            Some(SubCmd::Start { daemonize, .. }) => *daemonize,
            None => self.daemonize,
            _ => false,
        }
    }

    /// Returns `true` when logs should also be written to the console.
    pub fn show_logs(&self) -> bool {
        match &self.subcmd {
            Some(SubCmd::Start { show_logs, .. }) => *show_logs,
            None => self.show_logs,
            _ => false,
        }
    }

    pub async fn run(
        self,
        settings: Settings,
        store: SqliteStore,
        history_db: Sqlite,
    ) -> Result<()> {
        match self.subcmd {
            None => {
                eprintln!("Warning: `atuin daemon` is deprecated, use `atuin daemon start`");
                run(settings, store, history_db, false).await
            }
            Some(SubCmd::Start { force, .. }) => run(settings, store, history_db, force).await,
            Some(SubCmd::Status) => status_cmd(&settings).await,
            Some(SubCmd::Stop) => stop_cmd(&settings).await,
            Some(SubCmd::Restart) => restart_cmd(&settings, false).await,
        }
    }
}

const LEGACY_DAEMON_RESTART_MESSAGE: &str = "legacy daemon detected; restart daemon manually";

enum Probe {
    Ready(HistoryClient),
    NeedsRestart(VersionMismatch),
    /// A newer daemon we can't talk to. It's left running rather than replaced with ours.
    Incompatible(VersionMismatch),
    Unreachable(eyre::Report),
}

fn is_legacy_daemon_error(err: &eyre::Report) -> bool {
    matches!(classify_error(err), DaemonClientErrorKind::Unimplemented)
}

pub(super) fn should_retry_after_error(err: &eyre::Report) -> bool {
    matches!(
        classify_error(err),
        DaemonClientErrorKind::Connect
            | DaemonClientErrorKind::Unavailable
            | DaemonClientErrorKind::Unimplemented
    )
}

async fn connect_client(settings: &Settings) -> Result<HistoryClient> {
    HistoryClient::from_settings(settings).await
}

async fn probe(settings: &Settings) -> Probe {
    let mut client = match connect_client(settings).await {
        Ok(client) => client,
        Err(err) => return Probe::Unreachable(err),
    };

    match client.status().await {
        Ok(status) => match status.ensure_compatible() {
            Ok(()) => Probe::Ready(client),
            Err(mismatch) if mismatch.is_outdated() => Probe::NeedsRestart(mismatch),
            Err(mismatch) => Probe::Incompatible(mismatch),
        },
        Err(err) => Probe::Unreachable(err.into()),
    }
}

/// An error that occurred while trying to remove a socket.
#[cfg(unix)]
#[derive(Debug, thiserror::Error)]
#[error(
    "{}",
    fl!(
        "daemon-remove-socket-failed",
        path = .path.display().to_string(),
        source = .source.to_string()
    )
)]
struct RemoveSocketError {
    path: PathBuf,
    source: std::io::Error,
}

/// Remove the daemon's socket from every path it may be at, subject to `should_remove`.
#[cfg(unix)]
fn remove_sockets(
    settings: &Settings,
    should_remove: impl Fn(&Path) -> bool,
) -> Result<(), RemoveSocketError> {
    if settings.daemon.systemd_socket {
        return Ok(());
    }

    let mut error = None;
    for socket_path in settings.daemon.potential_socket_paths() {
        if !socket_path.exists() || !should_remove(&socket_path) {
            continue;
        }

        if let Err(e) = fs::remove_file(&socket_path)
            && e.kind() != ErrorKind::NotFound
        {
            // Log the error because we only return the first error when multiple occur.
            tracing::error!("failed to remove daemon socket {}: {e}", socket_path.display());
            error.get_or_insert_with(|| RemoveSocketError {
                path: socket_path.into_owned(),
                source: e,
            });
        }
    }

    error.map_or(Ok(()), Err)
}

/// Remove any socket left behind by a daemon that is no longer listening.
#[cfg(unix)]
fn remove_stale_socket_if_present(settings: &Settings) -> Result<(), RemoveSocketError> {
    use std::os::unix::fs::FileTypeExt;

    remove_sockets(settings, |socket_path| {
        // A refused connection means the socket is left over from a daemon that is gone. Only a
        // socket, though: connecting to any other file is refused too.
        fs::symlink_metadata(socket_path).is_ok_and(|meta| meta.file_type().is_socket())
            && matches!(
                StdUnixStream::connect(socket_path),
                Err(e) if e.kind() == ErrorKind::ConnectionRefused
            )
    })
}

#[allow(clippy::unnecessary_wraps)]
fn ensure_autostart_supported(settings: &Settings) -> Result<()> {
    #[cfg(unix)]
    if settings.daemon.systemd_socket {
        bail!(
            "daemon autostart is incompatible with `daemon.systemd_socket = true`; use systemd to \
             manage the daemon"
        );
    }
    #[cfg(not(unix))]
    let _ = settings;

    Ok(())
}

/// Ensure the daemon is running, starting it if necessary.
///
/// If the daemon is already running and up-to-date, this is a no-op.
/// If it is not running or needs a restart, this will spawn a new daemon
/// process and wait for it to become ready.
///
/// Returns an error if the daemon could not be started.
pub async fn ensure_daemon_running(settings: &Settings) -> Result<()> {
    ensure_autostart_supported(settings)?;
    ActiveAgent::find_or_spawn(settings).await?;
    Ok(())
}

async fn restart_daemon(settings: &Settings) -> Result<HistoryClient> {
    ensure_daemon_running(settings).await?;
    connect_client(settings).await
}

fn ensure_reply_compatible(settings: &Settings, reply: &impl VersionedReply) -> Result<()> {
    let Err(mismatch) = reply.ensure_compatible() else {
        return Ok(());
    };

    if settings.daemon.autostart {
        return Err(mismatch.into());
    }

    bail!("{mismatch}. Enable `daemon.autostart = true` or restart the daemon manually");
}

/// Acquire a [`HistoryClient`] connected to a daemon we expect to understand us, probing over the
/// wire-stable Status RPC first and restarting the daemon if it is absent or version/protocol
/// skewed.
///
/// TODO(markovejnovic): This is egregious slop, but not worse than the original solution. I want to
///                      remove this in a future PR: <https://github.com/atuinsh/atuin/pull/4002>
pub async fn ready_client(settings: &Settings) -> Result<HistoryClient> {
    match probe(settings).await {
        Probe::Ready(client) => return Ok(client),
        Probe::Incompatible(reason) => return Err(reason.into()),
        Probe::NeedsRestart(reason) if !settings.daemon.autostart => {
            bail!("{reason}. Enable `daemon.autostart = true` or restart the daemon manually");
        }
        Probe::Unreachable(err) if is_legacy_daemon_error(&err) => {
            return Err(err.wrap_err(LEGACY_DAEMON_RESTART_MESSAGE));
        }
        Probe::Unreachable(err) if !settings.daemon.autostart => return Err(err),
        Probe::Unreachable(err) if !should_retry_after_error(&err) => return Err(err),
        Probe::NeedsRestart(_) | Probe::Unreachable(_) => {}
    }

    restart_daemon(settings).await
}

/// Send a request to the daemon, first ensuring (via [`ready_client`]) that it is running and
/// speaks our version.
async fn try_with_restart<F, R>(settings: &Settings, send_request: F) -> Result<R>
where
    F: AsyncFnOnce(&mut HistoryClient) -> Result<R> + Sync,
    R: atuin_daemon::grpc::VersionedReply,
{
    let client = ready_client(settings).await?;
    send_checked(settings, client, send_request).await
}

/// Send a request to an already-running daemon that speaks our version.
///
/// This function never starts or restarts the daemon.
async fn try_without_restart<F, R>(settings: &Settings, send_request: F) -> Result<R>
where
    F: AsyncFnOnce(&mut HistoryClient) -> Result<R> + Sync,
    R: atuin_daemon::grpc::VersionedReply,
{
    let client = match probe(settings).await {
        Probe::Ready(client) => client,
        Probe::NeedsRestart(reason) | Probe::Incompatible(reason) => return Err(reason.into()),
        Probe::Unreachable(err) => return Err(err),
    };
    send_checked(settings, client, send_request).await
}

/// Send a message to the daemon and ensure the response is [compatible](ensure_reply_compatible).
async fn send_checked<F, R>(
    settings: &Settings,
    mut client: HistoryClient,
    send_request: F,
) -> Result<R>
where
    F: AsyncFnOnce(&mut HistoryClient) -> Result<R> + Sync,
    R: atuin_daemon::grpc::VersionedReply,
{
    let resp = send_request(&mut client).await?;
    ensure_reply_compatible(settings, &resp)?;
    Ok(resp)
}

/// How long `history start` waits on the daemon. It runs synchronously before every command, and a
/// stuck daemon still accepts connections, so without a limit the prompt hangs. On timeout the
/// command is saved to the local database instead.
///
/// `history end` and `cancel` have no limit: the hooks run them in the background (on Nushell, from
/// 0.104), and giving up would lose a command that only exists in the daemon's memory.
const START_HISTORY_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn start_history(settings: &Settings, history: History) -> Result<HistoryId> {
    let start = try_with_restart(settings, async |client| client.start_history(history).await);
    let resp = tokio::time::timeout(START_HISTORY_TIMEOUT, start)
        .await
        .wrap_err("timed out waiting for the daemon")??;
    let id = resp.id.ok_or_else(|| eyre::eyre!("daemon reply is missing the history id"))?;
    Ok(HistoryId::try_from(id)?)
}

pub async fn end_history(
    settings: &Settings,
    id: HistoryId,
    duration: Option<std::time::Duration>,
    exit: i64,
) -> Result<()> {
    try_without_restart(settings, async |client| client.end_history(id, duration, exit).await)
        .await?;
    Ok(())
}

pub async fn cancel_history(settings: &Settings, id: HistoryId) -> Result<()> {
    try_without_restart(settings, async |client| client.cancel_history(id).await).await?;
    Ok(())
}

pub async fn delete_history(settings: &Settings, ids: Vec<HistoryId>) -> Result<u64> {
    let reply = try_with_restart(settings, async |client| client.delete_history(ids).await).await?;
    Ok(reply.deleted)
}

pub async fn rebuild_history(settings: &Settings) -> Result<()> {
    try_with_restart(settings, async |client| client.rebuild_history().await).await?;
    Ok(())
}

/// Have the daemon rebuild its AI sessions from the record store (see
/// `AiClient::rebuild_sessions`), first starting or replacing it (via [`ready_client`]) as the
/// other AI session commands do.
pub async fn rebuild_ai_sessions(settings: &Settings) -> Result<()> {
    ready_client(settings).await?;
    let mut client = atuin_daemon::AiClient::from_settings(settings).await?;
    client.rebuild_sessions().await
}

/// How long to look for a running daemon to project synced AI sessions: a stopped daemon's socket
/// still accepts connections but never answers.
const PROJECT_SYNCED_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a sync waits for the daemon to project the AI sessions it downloaded. A large store
/// takes longer; the daemon carries on regardless.
const PROJECT_SYNCED_WAIT: Duration = Duration::from_secs(10);

/// Have a running daemon project the AI session records this process synced into the record
/// store, returning once they are listed or, after [`PROJECT_SYNCED_WAIT`], leaving the daemon to
/// finish on its own. Never starts or replaces one: a daemon started later, or replacing one too
/// old to be asked, projects them as it starts.
pub async fn project_synced_ai_sessions(settings: &Settings) -> Result<()> {
    let probe = tokio::time::timeout(PROJECT_SYNCED_PROBE_TIMEOUT, probe(settings)).await;
    let Ok(Probe::Ready(_)) = probe else {
        return Ok(());
    };
    let mut client = atuin_daemon::AiClient::from_settings(settings).await?;
    match tokio::time::timeout(PROJECT_SYNCED_WAIT, client.project_synced()).await {
        Ok(Err(err)) if is_legacy_daemon_error(&err) => Ok(()),
        Ok(projected) => projected,
        Err(_) => {
            eprintln!("Indexing AI sessions in the background");
            Ok(())
        }
    }
}

pub async fn compact_store(settings: &Settings) -> Result<u64> {
    let reply = try_with_restart(settings, async |client| client.compact_store().await).await?;
    Ok(reply.rewritten)
}

async fn status_cmd(settings: &Settings) -> Result<()> {
    match probe(settings).await {
        Probe::Ready(mut client) => {
            let status = client.status().await?;
            println!("Daemon running");
            println!("  PID:      {}", status.pid);
            println!("  Version:  {}", status.version);
            println!("  Protocol: {}", status.protocol);
            println!("  Healthy:  {}", status.healthy);
            #[cfg(unix)]
            println!(
                "  Socket:   {}",
                atuin_daemon::client::socket_path(&settings.daemon).display()
            );
            #[cfg(not(unix))]
            println!("  Port:     {}", settings.daemon.tcp_port);
        }
        Probe::NeedsRestart(reason) => {
            println!("Daemon running (needs restart)");
            println!("  Reason: {reason}");
        }
        Probe::Incompatible(reason) => {
            println!("Daemon running (newer than this client)");
            println!("  Reason: {reason}");
        }
        Probe::Unreachable(_) => {
            println!("Daemon is not running");
        }
    }

    Ok(())
}

pub(super) async fn stop_cmd(settings: &Settings) -> Result<()> {
    let Some(agent) = ActiveAgent::find(settings).await? else {
        println!("Daemon is not running");
        return Ok(());
    };

    match agent.stop().await {
        Ok(()) => println!("Daemon stopped"),
        Err(StopError::Pidfile(PidfileWaitError::StillHeld { .. })) => {
            println!("Daemon may still be shutting down");
        }
        Err(err) => return Err(err.into()),
    }

    Ok(())
}

pub(super) async fn restart_cmd(settings: &Settings, only_if_running: bool) -> Result<()> {
    match ActiveAgent::find(settings).await? {
        Some(agent) => {
            println!("Restarting daemon...");
            agent.restart().await?;
        }
        None if only_if_running => {
            println!("No daemon running");
            return Ok(());
        }
        None => {
            println!("No daemon running");
            println!("Starting daemon...");
            ActiveAgent::find_or_spawn(settings).await?;
        }
    }

    let status = connect_client(settings).await?.status().await?;

    println!("Daemon restarted");
    println!("  PID:      {}", status.pid);
    println!("  Version:  {}", status.version);

    Ok(())
}

/// Daemonize the current process. Must be called before creating the tokio
/// runtime or opening database connections, since `fork()` inside an async
/// runtime corrupts its internal state.
#[cfg(unix)]
pub fn daemonize_current_process() -> Result<()> {
    let settings = Settings::new().wrap_err("could not load client settings")?;

    let working_directory = AgentWorkingDir::for_settings(&settings)
        .resolve()
        .wrap_err("could not determine current directory for daemon")?;

    Daemonize::new()
        .working_directory(working_directory)
        .start()
        .wrap_err("failed to daemonize process")?;

    Ok(())
}

async fn run(
    settings: Settings,
    store: SqliteStore,
    history_db: Sqlite,
    force: bool,
) -> Result<()> {
    if force {
        force_cleanup(&settings).await;
    }

    let _pidfile_guard = PidfileGuard::acquire(&settings.daemon)?;

    // A daemon that crashed left its socket behind, which would keep this one from listening.
    #[cfg(unix)]
    remove_stale_socket_if_present(&settings)?;

    atuin_daemon::boot(settings, store, history_db).await?;

    Ok(())
}

/// Force cleanup: kill existing daemon process and remove socket.
async fn force_cleanup(settings: &Settings) {
    let pidfile_path = Path::new(&settings.daemon.pidfile_path);

    // Read and kill the existing process if pidfile exists
    if pidfile_path.exists() {
        if let Some(pid) = pidfile::try_read_pid(pidfile_path)
            && pid != 0
            && let Err(e) =
                atuin_common::os::process::force_terminate(pid, Duration::from_secs(2)).await
        {
            tracing::warn!("could not terminate existing daemon (pid {pid}): {e}");
        }

        // Remove the pidfile
        if let Err(e) = fs::remove_file(pidfile_path)
            && e.kind() != ErrorKind::NotFound
        {
            tracing::warn!("failed to remove pidfile: {e}");
        }
    }

    // Remove the socket files
    #[cfg(unix)]
    if let Err(e) = remove_sockets(settings, |_| true) {
        tracing::warn!("{e}");
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[cfg(unix)]
    #[rstest]
    fn remove_socket_error_names_the_path_and_cause() {
        let err = RemoveSocketError {
            path: PathBuf::from("/run/atuin.sock"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert_eq!(
            err.to_string(),
            "failed to remove daemon socket /run/atuin.sock: permission denied"
        );
    }

    /// A socket nothing listens on is removed; one a daemon listens on, and a file that is no
    /// socket (a `socket_path` set wrong), are not.
    #[cfg(unix)]
    #[rstest]
    #[case::stale(false, true, false)]
    #[case::listening(true, true, true)]
    #[case::not_a_socket(false, false, true)]
    fn only_a_stale_socket_is_removed(
        #[case] listening: bool,
        #[case] socket: bool,
        #[case] kept: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("atuin.sock");
        let listener = socket.then(|| std::os::unix::net::UnixListener::bind(&path).unwrap());
        if !socket {
            fs::write(&path, "not a socket").unwrap();
        }
        if !listening {
            drop(listener);
        }
        let mut settings = Settings::default();
        settings.daemon.socket_path = Some(path.clone());
        remove_stale_socket_if_present(&settings).unwrap();
        assert_eq!(path.exists(), kept);
    }

    /// A stopped daemon's socket still accepts connections but never answers.
    #[cfg(unix)]
    #[rstest]
    #[tokio::test]
    async fn start_history_times_out_on_a_daemon_that_never_answers() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("stopped.sock");
        let _listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut settings = Settings::default();
        settings.daemon.socket_path = Some(socket);
        settings.daemon.pidfile_path = dir.path().join("d.pid").to_str().unwrap().to_owned();
        settings.daemon.autostart = false;
        let history = History::import()
            .timestamp(time::OffsetDateTime::now_utc())
            .command("true")
            .cwd("/")
            .build();

        let started = std::time::Instant::now();
        let err = start_history(&settings, history.into()).await.unwrap_err();

        assert!(started.elapsed() < START_HISTORY_TIMEOUT * 2, "{:?}", started.elapsed());
        assert!(format!("{err:#}").contains("timed out"), "{err:#}");
    }
}
