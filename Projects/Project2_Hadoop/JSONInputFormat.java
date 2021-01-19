package com.company;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JSONInputFormat extends FileInputFormat {
    public static final String START_TAG_KEY = "{";
    public static final String END_TAG_KEY = "}";
    public static final String NUM_MAP_TASKS = "5";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new JsonRecordReader();
    }
    @Override
    public long computeSplitSize(long blockSize,long minSize,long maxSize) {
        return 200000;
    }

    public class JsonRecordReader extends RecordReader<LongWritable, Text> {

    private byte[] startTagKey;
    private byte[] endTagKey;
    private long start;
    private long end;
    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext tac) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        String START_TAG_KEY = "{";
        String END_TAG_KEY = "}";
        startTagKey = START_TAG_KEY.getBytes("utf-8");
        endTagKey = END_TAG_KEY.getBytes("utf-8");

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Path file = fileSplit.getPath();

        FileSystem fs = file.getFileSystem(tac.getConfiguration());
        fsin = fs.open(fileSplit.getPath());
        fsin.seek(start);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        String val = value.toString();
        val = val.replaceAll("[\r\n]+", " ");
        StringBuffer cur = new StringBuffer();
        for (int i = 0; i < val.length(); i++) {
            if (val.charAt(i) == '"' || val.charAt(i) == ' ' || val.charAt(i) == '\t') {
                continue;
            } else {
                cur.append(val.charAt(i));
            }
        }
        String finalText = cur.toString().substring(1, cur.length() - 1);
        return new Text(finalText);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fsin.getPos() < end) {
            if (readUntilMatch(startTagKey, false)) {
                try {
                    buffer.write(startTagKey);
                    if (readUntilMatch(startTagKey, true)) {

                        value.set(buffer.getData(), 0, buffer.getLength());
                        key.set(fsin.getPos());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (fsin.getPos() - start) / (float) (end - start);
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
            throws IOException {
        int i = 0;
        while (true) {
            int b = fsin.read();

            if (b == -1)
                return false;

            if (withinBlock)
                buffer.write(b);

            if (b == match[i]) {
                i++;
                if (i >= match.length)
                    return true;
            } else
                i = 0;

            if (!withinBlock && i == 0 && fsin.getPos() >= end)
                return false;
        }
    }
    }
}