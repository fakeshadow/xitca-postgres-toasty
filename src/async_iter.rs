use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures_core::stream::Stream;
use toasty_core::{
    Result,
    stmt::{Type, Value, ValueRecord, ValueStream},
};
use xitca_postgres::RowStreamOwned;

pin_project_lite::pin_project! {
    pub(crate) struct RowStream {
        #[pin]
        stream: RowStreamOwned,
        types: Vec<Type>,
    }
}

impl RowStream {
    pub(crate) fn new(types: Vec<Type>, stream: RowStreamOwned) -> Self {
        Self { stream, types }
    }
}

impl From<RowStream> for ValueStream {
    fn from(stream: RowStream) -> Self {
        Self::from_stream(stream)
    }
}

impl Stream for RowStream {
    type Item = Result<Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = ready!(this.stream.poll_next(cx)?).map(|row| {
            let fields = row
                .columns()
                .iter()
                .enumerate()
                .map(|(i, column)| crate::value::from_sql(i, &row, column, &this.types[i]))
                .collect::<Vec<_>>();
            Ok(ValueRecord::from_vec(fields).into())
        });

        Poll::Ready(res)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        Stream::size_hint(&self.stream)
    }
}
