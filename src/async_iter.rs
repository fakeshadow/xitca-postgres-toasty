use toasty_core::stmt::{Type, Value, ValueRecord, ValueStream};
use xitca_postgres::{RowStreamOwned, iter::AsyncLendingIterator};

pub(crate) fn stream(mut stream: RowStreamOwned, types: Vec<Type>) -> ValueStream {
    ValueStream::from_stream(async_stream::try_stream! {
        while let Some(row) = stream.try_next().await? {
             let fields = row
                .columns()
                .iter()
                .enumerate()
                .map(|(i, column)| crate::value::from_sql(i, &row, column, &types[i]))
                .collect::<Vec<_>>();
            yield Value::from(ValueRecord::from_vec(fields));
        }
    })
}
