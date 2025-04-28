use std::{
    io,
    ops::ControlFlow,
    task::{ready, Context, Poll},
};

use bytes::BytesMut;
use tracing::{info, trace};

use crate::Error;

use super::{BufferedSocket, Socket};

pub struct Framed<S> {
    inner: BufferedSocket<S>,
    done_writing: bool,
    need: Option<usize>,
}

impl<S: Socket> Framed<S> {
    pub fn new(inner: BufferedSocket<S>) -> Framed<S> {
        Framed {
            inner,
            done_writing: false,
            need: None,
        }
    }

    #[inline(always)]
    pub fn write_raw(&mut self, value: &[u8]) {
        let buff = self.inner.write_buffer_mut();
        buff.put_slice(value)
    }

    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.done_writing {
            let _ = ready!(self.inner._poll_write(cx)?);
            self.done_writing = true
        }

        ready!(self.inner._poll_flush(cx)?);
        self.done_writing = false;
        Poll::Ready(Ok(()))
    }

    pub fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_shutdown(cx)
    }

    pub fn poll_try_read<F, R>(
        &mut self,
        cx: &mut Context<'_>,
        mut try_read: F,
    ) -> Poll<crate::Result<R>>
    where
        F: FnMut(&mut BytesMut) -> Result<ControlFlow<R, usize>, Error>,
    {
        loop {
            if let Some(need) = &self.need {
                if self
                    .inner
                    .read_buf
                    .poll_read(cx, *need, &mut self.inner.socket)?
                    .is_pending()
                {
                    info!("waiting on read");
                    return Poll::Pending;
                } else {
                    info!("done reading");
                }

                self.need.take();
            } else {
                self.need = match try_read(self.inner.read_buffer_mut())? {
                    ControlFlow::Continue(read_len) => Some(read_len),
                    ControlFlow::Break(ret) => {
                        trace!("poll_try_read: got response");
                        return Poll::Ready(Ok(ret));
                    }
                };
            }
        }
    }
}
