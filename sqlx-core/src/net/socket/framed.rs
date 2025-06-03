use futures_core::Stream;
use futures_util::Sink;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::{io::Codec, net::Socket};

use super::{buffered::DEFAULT_BUF_SIZE, BufferedSocket, WriteBuffer};

const BACKPRESSURE_LIMIT: usize = DEFAULT_BUF_SIZE;

pub struct Framed<S, C> {
    socket: BufferedSocket<S>,
    codec: C,
    is_readable: bool,
}

impl<C: Codec, S: Socket> Framed<S, C> {
    pub fn new(socket: S, codec: C) -> Self {
        Framed {
            socket: BufferedSocket::new(socket),
            codec,
            is_readable: false,
        }
    }

    pub fn write_buffer(&self) -> &WriteBuffer {
        self.socket.write_buffer()
    }

    pub fn write_buffer_mut(&mut self) -> &mut WriteBuffer {
        self.socket.write_buffer_mut()
    }

    pub fn shrink_buffers(&mut self) {
        self.socket.shrink_buffers();
    }
}

impl<C: Codec + Unpin, S: Socket + Unpin> Sink<C::Request> for Framed<S, C> {
    type Error = crate::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        if self.socket.write_buffer().get().len() >= BACKPRESSURE_LIMIT {
            self.poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    #[inline(always)]
    fn start_send(mut self: Pin<&mut Self>, item: C::Request) -> crate::Result<()> {
        let this = &mut *self;
        this.socket.encode(&mut this.codec, item)
    }

    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.socket.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        ready!(self.as_mut().poll_flush(cx)?);
        self.get_mut().socket.poll_shutdown(cx).map_err(Into::into)
    }
}

impl<C: Codec + Unpin, S: Socket + Unpin> Stream for Framed<S, C> {
    type Item = crate::Result<C::Response>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if this.is_readable {
                if let Some(frame) = this.socket.decode(&mut this.codec)? {
                    return Poll::Ready(Some(Ok(frame)));
                }
                this.is_readable = false;
            }

            ready!(this.socket.poll_read(cx)?);
            this.is_readable = true;
        }
    }
}
