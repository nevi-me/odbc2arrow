use std::{convert::TryInto, sync::Arc};

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowType, DateUnit, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{buffer::Buffer, datatypes::ToByteSlice};
use log::{debug, info};
use odbc_api::{
    sys::USmallInt, sys::NULL_DATA, ColumnDescription, Cursor, DataType, Environment, Nullable,
    RowSetCursor,
};

use crate::odbc_buffer::{ColumnBufferDescription, OdbcBuffer};

pub struct OdbcArrowReader<'b, 'o> {
    schema: SchemaRef,
    row_set_cursor: RowSetCursor<'b, 'o, OdbcBuffer>,
    batch_size: usize,
    is_complete: bool,
}

pub enum ConnectionType {
    Dsn(String),
    ConnString(String),
}

impl<'b, 'o> OdbcArrowReader<'b, 'o> {
    pub fn try_new(
        connection: &ConnectionType,
        query: &str, // TODO: allow exec_direct and dsn via enum
        user: &str,
        pass: &str,
        batch_size: usize,
    ) -> Result<Self> {
        let mut odbc_conn = match connection {
            ConnectionType::Dsn(dsn) => {
                // unlike the author of ODBC, we know nothing about the safety here
                // TODO: read up on how this works
                let odbc_env = unsafe { Environment::new() }.unwrap();
                odbc_env.connect(&dsn, user, pass).unwrap()
            }
            ConnectionType::ConnString(conn) => {
                let odbc_env = unsafe { Environment::new() }.unwrap();
                odbc_env.connect_with_connection_string(conn).unwrap()
            }
        };

        if let Some(cursor) = odbc_conn.exec_direct(query).unwrap() {
            let (schema, buffer_description) = make_arrow_schema(&cursor)?;
            let mut odbc_buffer = OdbcBuffer::new(batch_size, buffer_description.iter().copied());
            let row_set_cursor = cursor.bind_row_set_buffer(&mut odbc_buffer).unwrap();
            Ok(Self {
                schema,
                row_set_cursor,
                batch_size,
                is_complete: false,
            })
        } else {
            Err(ArrowError::ComputeError(
                "Query returned no results, unable to determine schema".to_string(),
            ))
        }
    }

    fn cursor_to_arrow(&mut self) -> Result<Option<RecordBatch>> {
        info!("Batch size set to {}", self.batch_size);

        if let Some(buffer) = self.row_set_cursor.fetch().unwrap() {
            // construct record batches, and write them to file
            let arrays = self
                .schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    match field.data_type() {
                        ArrowType::Boolean => {
                            let buf = buffer.bool_it(index);
                            let array = BooleanArray::from(buf.collect::<Vec<Option<bool>>>());
                            Arc::new(array) as ArrayRef
                        }
                        ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 => {
                            let (values, indicators) = buffer.i32_column(index);
                            let array_data = values.to_byte_slice();
                            let array_buf = Buffer::from(array_data);
                            // TODO: is it faster if we create a non-null i64, then compare to create bools?
                            let nullity = indicators
                                .into_iter()
                                .map(|i| *i != NULL_DATA)
                                .collect::<Vec<bool>>();
                            let nullity = nullity.to_byte_slice();
                            let array_data = ArrayData::builder(ArrowType::Int32)
                                .add_buffer(array_buf)
                                .null_bit_buffer(Buffer::from(nullity))
                                .build();
                            let array = make_array(array_data);
                            arrow::compute::cast(&array, field.data_type())
                                .expect("Unable to cast i32")
                        }
                        ArrowType::Int64 => {
                            let (values, indicators) = buffer.i64_column(index);
                            let array_data = values.to_byte_slice();
                            let array_buf = Buffer::from(array_data);
                            // TODO: is it faster if we create a non-null i64, then compare to create bools?
                            let nullity = indicators
                                .into_iter()
                                .map(|i| *i != NULL_DATA)
                                .collect::<Vec<bool>>();
                            let nullity = nullity.to_byte_slice();
                            let array_data = ArrayData::builder(ArrowType::Int32)
                                .add_buffer(array_buf)
                                .null_bit_buffer(Buffer::from(nullity))
                                .build();
                            let array = make_array(array_data);
                            arrow::compute::cast(&array, field.data_type())
                                .expect("Unable to cast i64")
                        }
                        ArrowType::Float32 => {
                            let (values, indicators) = buffer.f32_column(index);
                            let array_data = values.to_byte_slice();
                            let array_buf = Buffer::from(array_data);
                            // TODO: is it faster if we create a non-null i64, then compare to create bools?
                            let nullity = indicators
                                .into_iter()
                                .map(|i| *i != NULL_DATA)
                                .collect::<Vec<bool>>();
                            let nullity = nullity.to_byte_slice();
                            let array_data = ArrayData::builder(ArrowType::Float32)
                                .add_buffer(array_buf)
                                .null_bit_buffer(Buffer::from(nullity))
                                .build();
                            make_array(array_data)
                        }
                        ArrowType::Float64 => {
                            let (values, indicators) = buffer.f64_column(index);
                            let array_data = values.to_byte_slice();
                            let array_buf = Buffer::from(array_data);
                            // TODO: is it faster if we create a non-null i64, then compare to create bools?
                            let nullity = indicators
                                .into_iter()
                                .map(|i| *i != NULL_DATA)
                                .collect::<Vec<bool>>();
                            let nullity = nullity.to_byte_slice();
                            let array_data = ArrayData::builder(ArrowType::Float64)
                                .add_buffer(array_buf)
                                .null_bit_buffer(Buffer::from(nullity))
                                .build();
                            make_array(array_data)
                        }
                        ArrowType::Timestamp(_, _) => todo!("Timestamps not yet implemented"),
                        ArrowType::Date32(_) => todo!("Dates not yet implemented"),
                        ArrowType::Utf8 => {
                            let buf = buffer.text_column_it(index);
                            let array = StringArray::from(
                                buf.map(|v| v.map(|v| std::str::from_utf8(v).unwrap()))
                                    .collect::<Vec<Option<&str>>>(),
                            );
                            Arc::new(array) as ArrayRef
                        }
                        ArrowType::LargeUtf8 => {
                            let buf = buffer.text_column_it(index);
                            let array = LargeStringArray::from(
                                buf.map(|v| v.map(|v| std::str::from_utf8(v).unwrap()))
                                    .collect::<Vec<Option<&str>>>(),
                            );
                            Arc::new(array) as ArrayRef
                        }
                        t => panic!("Unsupported column type {:?}", t),
                    }
                })
                .collect::<Vec<ArrayRef>>();
            let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

            return Ok(Some(batch));
        } else {
            return Ok(None);
        }
    }
}

impl<'b, 'o> RecordBatchReader for OdbcArrowReader<'b, 'o> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> arrow::error::Result<Option<RecordBatch>> {
        self.next().transpose()
    }
}

impl<'b, 'o> Iterator for OdbcArrowReader<'b, 'o> {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: fix these transposes
        self.cursor_to_arrow().transpose()
    }
}

fn make_arrow_schema(cursor: &Cursor) -> Result<(SchemaRef, Vec<ColumnBufferDescription>)> {
    let num_cols = cursor.num_result_cols().unwrap();

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

    for index in 1..=num_cols {
        let mut cd = ColumnDescription::default();
        // Reserving helps with drivers not reporting column name size correctly.
        cd.name.reserve(128);
        cursor.describe_col(index as USmallInt, &mut cd).unwrap();

        debug!("ODBC column description for column {}: {:?}", index, cd);

        let name = cd.name_to_string().unwrap();
        // Give a generated name, should we fail to retrieve one from the ODBC data source.
        let name = if name.is_empty() {
            format!("Column{}", index)
        } else {
            name
        };

        let (data_type, buffer_description) = match cd.data_type {
            DataType::Double => (ArrowType::Float64, ColumnBufferDescription::F64),
            DataType::Float | DataType::Real => (ArrowType::Float32, ColumnBufferDescription::F32),
            DataType::SmallInt => (ArrowType::Int16, ColumnBufferDescription::I32),
            DataType::Integer => (ArrowType::Int32, ColumnBufferDescription::I32),
            DataType::Date => (
                ArrowType::Date32(DateUnit::Millisecond),
                ColumnBufferDescription::Date,
            ),
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 10 =>
            {
                (ArrowType::Int32, ColumnBufferDescription::I32)
            }
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 19 =>
            {
                (ArrowType::Int64, ColumnBufferDescription::I64)
            }
            DataType::Timestamp { .. } => (
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                ColumnBufferDescription::Timestamp,
            ),
            DataType::Bigint => (ArrowType::Int64, ColumnBufferDescription::I64),
            DataType::Char { length } | DataType::Varchar { length } => (
                ArrowType::LargeUtf8,
                ColumnBufferDescription::Text {
                    max_str_len: length.try_into().unwrap(),
                },
            ),
            DataType::Bit => (ArrowType::Boolean, ColumnBufferDescription::Bit),
            DataType::Tinyint => (ArrowType::Int8, ColumnBufferDescription::I32),
            DataType::Unknown
            | DataType::Numeric { .. }
            | DataType::Decimal { .. }
            | DataType::Time { .. }
            | DataType::Other { .. } => {
                let max_str_len =
                    cursor.col_display_size(index.try_into().unwrap()).unwrap() as usize;
                (
                    ArrowType::Utf8,
                    ColumnBufferDescription::Text { max_str_len },
                )
            }
        };

        debug!(
            "ODBC buffer description for column {}: {:?}",
            index, buffer_description
        );

        fields.push(Field::new(
            name.as_ref(),
            data_type,
            cd.nullable != Nullable::NoNulls,
        ));
        odbc_buffer_desc.push(buffer_description);
    }

    // TODO: suggest a format for metadata, and add it (e.g. ODBC version, DSN?)
    let schema = Schema::new(fields);

    Ok((Arc::new(schema) as SchemaRef, odbc_buffer_desc))
}
