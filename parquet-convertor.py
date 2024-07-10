"""Module for converting MCAP files to Parquet format."""

import argparse
import os

from mcap.reader import make_reader

import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq

from tqdm import tqdm


def read_mcap(file_path):
    """
    Read MCAP file and convert to a pandas DataFrame.

    :param file_path: Path to the MCAP file.
    :return: DataFrame containing the MCAP data.
    """
    rows = []
    with open(file_path, 'rb') as f:
        reader = make_reader(f)
        messages = list(reader.iter_messages())
        for schema, channel, message in tqdm(
            messages, desc='Reading MCAP file'
        ):
            try:
                data = message.data.decode('utf-8')
            except UnicodeDecodeError:
                data = message.data  # Store as raw bytes if decoding fails

            row = {
                'timestamp': message.log_time,
                'channel': channel.topic,
                'data': data,
            }
            rows.append(row)
    return pd.DataFrame(rows)


def convert_mcap_to_parquet(mcap_file, compression_method):
    """
    Convert MCAP file to Parquet file.

    :param mcap_file: Path to the input MCAP file.
    :param compression_method: Compression method to use for Parquet file.
    """
    df = read_mcap(mcap_file)
    table = pa.Table.from_pandas(df)

    # Generate the output Parquet file path
    parquet_file = os.path.splitext(mcap_file)[0] + '.parquet'

    pq.write_table(table, parquet_file, compression=compression_method)
    print(
        f'Converted {mcap_file} to {parquet_file} using {compression_method}'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert MCAP file to Parquet format.'
    )
    parser.add_argument(
        'mcap_file', type=str, help='Path to the input MCAP file.'
    )
    parser.add_argument(
        '-compression',
        type=str,
        default='SNAPPY',
        help=(
            'Compression method to use for Parquet file '
            '(e.g., SNAPPY, GZIP, BROTLI, LZ4, ZSTD).'
        ),
    )

    args = parser.parse_args()

    convert_mcap_to_parquet(args.mcap_file, args.compression)
