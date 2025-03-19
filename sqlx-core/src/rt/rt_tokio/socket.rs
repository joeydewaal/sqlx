use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::io::ReadBuf;
use crate::net::Socket;

impl Socket for TcpStream {
    fn try_read(&mut self, mut buf: &mut dyn ReadBuf) -> io::Result<usize> {
        // Requires `&mut impl BufMut`
        self.try_read_buf(&mut buf)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (*self).try_write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (*self).poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (*self).poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self).poll_shutdown(cx)
    }
}

#[cfg(unix)]
impl Socket for tokio::net::UnixStream {
    fn try_read(&mut self, mut buf: &mut dyn ReadBuf) -> io::Result<usize> {
        self.try_read_buf(&mut buf)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (*self).try_write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (*self).poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (*self).poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self).poll_shutdown(cx)
    }
}

impl AsyncRead for Box<dyn Socket> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match <Box<dyn Socket> as Socket>::poll_read(self.get_mut(), cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Ok(())),
        }
    }
}
