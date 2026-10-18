//! `berthq consume --exec`: runs a shell command per message with the body on stdin.

use std::cell::Cell;
use std::process::{ExitStatus, Stdio};

use berthq_client::Delivery;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::consume::body_text;
use crate::output;

/// Stderr kept from a failed command and sent as the nack error.
const ERROR_TAIL: usize = 2048;

/// Runs the command with the body on stdin. A non-zero exit is an error carrying the end of its
/// stderr, so the message is nacked with it. `running` holds the command's process id while it
/// runs.
pub async fn run_command(
    queue: &str,
    command: &str,
    delivery: &Delivery,
    running: &Cell<Option<u32>>,
) -> Result<(), String> {
    let mut child = shell(command)
        .env("BERTHQ_QUEUE", queue)
        .env("BERTHQ_CONSUMER", &delivery.consumer)
        .env("BERTHQ_MESSAGE_ID", &delivery.id)
        .env("BERTHQ_ATTEMPT", delivery.attempt.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to run {command}: {err}"))?;
    running.set(child.id());
    let _abandoned = KillIfAbandoned(running);
    // Read stderr while writing stdin: a command that writes a lot to stderr before reading its
    // input would otherwise wait for berthq while berthq waits for it.
    let stderr = child
        .stderr
        .take()
        .map(|stderr| tokio::spawn(stderr_tail(stderr)));
    if let Some(mut stdin) = child.stdin.take() {
        // A command that doesn't read its input closes the pipe early; that's fine.
        let _ = stdin.write_all(body_text(&delivery.body).as_bytes()).await;
    }
    let status = child.wait().await;
    running.set(None);
    let status = status.map_err(|err| format!("failed to run {command}: {err}"))?;
    let tail = match stderr {
        Some(task) => task.await.unwrap_or_default(),
        None => String::new(),
    };
    outcome(queue, &delivery.id, status, &tail)
}

/// Done on exit 0; otherwise an error with the end of the command's stderr.
fn outcome(queue: &str, id: &str, status: ExitStatus, tail: &str) -> Result<(), String> {
    if status.success() {
        output::note(&format!("{queue} {id} done"));
        return Ok(());
    }
    let code = status
        .code()
        .map_or("a signal".to_string(), |code| format!("code {code}"));
    output::note(&format!("{queue} {id} failed with {code}, will be retried"));
    Err(format!("command exited with {code}: {}", tail.trim())
        .trim_end_matches(": ")
        .to_string())
}

/// Kills the command when berthq stops waiting for it (the worker loop ended with an error), so
/// it can't run on unseen. A second Ctrl-C takes the process id first and interrupts instead.
struct KillIfAbandoned<'a>(&'a Cell<Option<u32>>);

impl Drop for KillIfAbandoned<'_> {
    fn drop(&mut self) {
        if let Some(command) = self.0.take() {
            kill(command);
        }
    }
}

/// Shows the command's stderr as it comes and keeps its last `ERROR_TAIL` bytes.
async fn stderr_tail(stderr: tokio::process::ChildStderr) -> String {
    let mut tail = String::new();
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        output::note(&line);
        tail.push_str(&line);
        tail.push('\n');
        if tail.len() > ERROR_TAIL {
            // Cut at a character boundary: draining in the middle of one would panic.
            let mut cut = tail.len() - ERROR_TAIL;
            while !tail.is_char_boundary(cut) {
                cut += 1;
            }
            tail.drain(..cut);
        }
    }
    tail
}

#[cfg(unix)]
fn shell(command: &str) -> tokio::process::Command {
    let mut shell = tokio::process::Command::new("sh");
    // Its own process group, so the terminal's Ctrl-C reaches only berthq, which lets the
    // command finish.
    shell.arg("-c").arg(command).process_group(0);
    shell
}

#[cfg(not(unix))]
fn shell(command: &str) -> tokio::process::Command {
    let mut shell = tokio::process::Command::new("cmd");
    shell.arg("/C").arg(command).kill_on_drop(true);
    shell
}

/// Sends the command the Ctrl-C its process group kept from it.
#[cfg(unix)]
pub fn interrupt(command: u32) {
    signal_group(command, rustix::process::Signal::INT);
}

/// The whole process group, so what the command started stops too.
#[cfg(unix)]
fn kill(command: u32) {
    signal_group(command, rustix::process::Signal::KILL);
}

#[cfg(unix)]
fn signal_group(command: u32, signal: rustix::process::Signal) {
    use rustix::process::{Pid, kill_process_group};

    if let Some(group) = i32::try_from(command).ok().and_then(Pid::from_raw) {
        let _ = kill_process_group(group, signal);
    }
}

/// Elsewhere the command shares the console, so it got the Ctrl-C already.
#[cfg(not(unix))]
pub fn interrupt(_command: u32) {}

/// Elsewhere `kill_on_drop` stops the command itself.
#[cfg(not(unix))]
fn kill(_command: u32) {}
