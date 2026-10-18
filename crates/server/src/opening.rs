//! A deadline for the first bytes of a connection. hyper reads them to tell HTTP/1.1 from
//! HTTP/2 and waits without a limit; its own timeouts only start once it knows.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum_server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Wraps another acceptor (plain or TLS) so its streams fail reads once `timeout` passes before
/// the client has shown which protocol it speaks.
#[derive(Clone)]
pub(crate) struct OpeningAcceptor<A> {
    pub inner: A,
    pub timeout: Duration,
}

impl<A, I, S> Accept<I, S> for OpeningAcceptor<A>
where
    A: Accept<I, S>,
    A::Future: Send + 'static,
{
    type Stream = OpeningStream<A::Stream>;
    type Service = A::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let accepted = self.inner.accept(stream, service);
        let deadline = Box::pin(tokio::time::sleep(self.timeout));
        Box::pin(async move {
            let (stream, service) = accepted.await?;
            let opening = OpeningStream {
                stream,
                matched: 0,
                deadline: Some(deadline),
            };
            Ok((opening, service))
        })
    }
}

pub(crate) struct OpeningStream<S> {
    stream: S,
    /// How many bytes so far match the HTTP/2 preface.
    matched: usize,
    deadline: Option<Pin<Box<Sleep>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for OpeningStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let read = Pin::new(&mut this.stream).poll_read(cx, buf);
        let Some(deadline) = &mut this.deadline else {
            return read;
        };
        match read {
            Poll::Ready(Ok(())) => {
                this.saw(&buf.filled()[before..]);
                Poll::Ready(Ok(()))
            }
            Poll::Pending if deadline.as_mut().poll(cx).is_ready() => Poll::Ready(Err(
                io::Error::new(io::ErrorKind::TimedOut, "client sent no request in time"),
            )),
            other => other,
        }
    }
}

impl<S> OpeningStream<S> {
    /// The protocol is known once the bytes stop matching the HTTP/2 preface (HTTP/1.1) or
    /// match all of it (HTTP/2), or the client closed the connection.
    fn saw(&mut self, bytes: &[u8]) {
        let rest = &H2_PREFACE[self.matched..];
        let compared = bytes.len().min(rest.len());
        self.matched += compared;
        let known = bytes.is_empty()
            || bytes[..compared] != rest[..compared]
            || self.matched == H2_PREFACE.len();
        if known {
            self.deadline = None;
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for OpeningStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
