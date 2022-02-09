use std::{
    future::Future,
    marker::PhantomData,
    mem, panic,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::stream::Stream;
use hyper::{self, body, Body, Request};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

use crate::{
    error::{Error, Result},
    response::Response,
    row::{self, Row},
    rowbinary, Client, Compression,
};

#[cfg(feature = "gzip")]
use async_compression::tokio::bufread::GzipEncoder;
#[cfg(any(feature = "brotli", feature = "gzip", feature = "zlib"))]
use tokio_util::io::{ReaderStream, StreamReader};

const BUFFER_SIZE: usize = 128 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;

enum Inner<S> {
    Plain(S),
    #[cfg(feature = "gzip")]
    Gzip(ReaderStream<GzipEncoder<StreamReader<S, Bytes>>>),
}

struct Chunks<S>(Box<Inner<S>>);

impl<S, E> Chunks<S>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: Into<std::io::Error>,
{
    fn new(s: S, compression: Compression) -> Self {
        Chunks(Box::new(match compression {
            Compression::None => Inner::Plain(s),
            #[cfg(feature = "gzip")]
            Compression::Gzip => Inner::Gzip(ReaderStream::new(GzipEncoder::with_quality(
                StreamReader::new(s),
                async_compression::Level::Fastest,
            ))),
            _ => todo!(),
        }))
    }
}

impl<S, E> Stream for Chunks<S>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: Into<std::io::Error>,
{
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Inner::*;
        let res = match &mut *self.0 {
            Plain(inner) => Pin::new(inner).poll_next(cx).map_err(Into::into),
            #[cfg(feature = "gzip")]
            Gzip(inner) => Pin::new(inner).poll_next(cx).map_err(Into::into),
        };

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        use Inner::*;
        match &*self.0 {
            Plain(inner) => inner.size_hint(),
            #[cfg(feature = "gzip")]
            Gzip(inner) => inner.size_hint(),
        }
    }
}

#[must_use]
pub struct Insert<T> {
    buffer: BytesMut,
    sender: Option<mpsc::Sender<Result<Bytes, std::io::Error>>>,
    handle: JoinHandle<Result<()>>,
    _marker: PhantomData<fn() -> T>, // TODO: test contravariance.
}

impl<T> Insert<T> {
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self>
    where
        T: Row,
    {
        let mut url = Url::parse(&client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers
        let query = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);
        pairs.append_pair("query", &query);

        for (name, value) in &client.options {
            pairs.append_pair(name, value);
        }

        drop(pairs);

        let mut builder = Request::post(url.as_str());

        if let Some(user) = &client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        match client.compression {
            Compression::None => {}
            #[cfg(feature = "gzip")]
            Compression::Gzip => {
                builder = builder.header("Content-Encoding", "gzip");
            }
            v => todo!("{:?}", v),
        }

        let (sender, receiver) = mpsc::channel(1);
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

        let chunks = Chunks::new(stream, client.compression);

        let body = Body::wrap_stream(chunks);

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = client.client._request(request);
        let handle =
            tokio::spawn(async move { Response::new(future, Compression::None).finish().await });

        Ok(Insert {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            sender: Some(sender),
            handle,
            _marker: PhantomData,
        })
    }

    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        assert!(self.sender.is_some(), "write() after error");

        let result = rowbinary::serialize_into(&mut self.buffer, row);
        if result.is_err() {
            self.abort();
        }

        async move {
            result?;
            self.send_chunk_if_exceeds(MIN_CHUNK_SIZE).await?;
            Ok(())
        }
    }

    pub async fn end(mut self) -> Result<()> {
        self.send_chunk_if_exceeds(1).await?;
        self.wait_handle().await
    }

    async fn send_chunk_if_exceeds(&mut self, threshold: usize) -> Result<()> {
        if self.buffer.len() >= threshold {
            // Hyper uses non-trivial and inefficient (see benches) schema of buffering chunks.
            // It's difficult to determine when allocations occur.
            // So, instead we control it manually here and rely on the system allocator.
            let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

            if let Some(sender) = &mut self.sender {
                if sender.send(Ok(chunk.freeze())).await.is_err() {
                    self.abort();
                    self.wait_handle().await?; // real error should be here.
                    return Err(Error::Network("channel closed".into()));
                }
            }
        }

        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<()> {
        drop(self.sender.take());

        match (&mut self.handle).await {
            Ok(res) => res,
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => {
                // TODO
                Err(Error::Custom(format!("unexpected error: {}", err)))
            }
        }
    }

    fn abort(&mut self) {
        if let Some(sender) = self.sender.take() {
            //sender.abort();
            todo!()
        }
    }
}

impl<T> Drop for Insert<T> {
    fn drop(&mut self) {
        self.abort();
    }
}
