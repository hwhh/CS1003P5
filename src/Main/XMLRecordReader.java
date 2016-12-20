package Main;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//The record reader breaks the data into key/value pairs for input to the Mapper.
class XMLRecordReader extends RecordReader<LongWritable, Text> {

    /**
     * Reads records that are delimited by a specific begin/end tag.
     */

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream FSIn;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable key = new LongWritable();
    private Text value = new Text();



    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        //Take a selection of the input file(s)
        FileSplit fileSplit = (FileSplit) is;
        //Set the start tag and end tag
        String START_TAG_KEY = "<tuv lang=\""+Main.gui.getLanguage()+"\">";
        String END_TAG_KEY = "</tuv>";
        //Encodes this String into a sequence of bytes using "utf-8", storing the result into a new byte array.
        startTag = START_TAG_KEY.getBytes("utf-8");
        endTag = END_TAG_KEY.getBytes("utf-8");
        //Get the position of the first byte in the file to process.
        start = fileSplit.getStart();
        //Get the number of bytes in the file to process.
        end = start + fileSplit.getLength();
        //Get the path of the input file
        Path file = fileSplit.getPath();
        //Gets the configuration object for the job and stores the file system that created this object
        FileSystem fs = file.getFileSystem(tac.getConfiguration());
        //Open an FSDataInputStream at the file containing this split's data.
        FSIn = fs.open(fileSplit.getPath());
        //Seek to the given offset from the start of the file.
        FSIn.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //Check if the current position in the input stream is not the end of the file
        if (FSIn.getPos() < end) {
            if (readUntilMatch(startTag, false)) {
                try {
                    buffer.write(startTag);
                    if (readUntilMatch(endTag, true)) {
                        //Set the data found between the start and end tags
                        value.set(buffer.getData(), 0, buffer.getLength());
                        key.set(FSIn.getPos());
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
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (FSIn.getPos() - start) / (float) (end - start);
    }

    @Override
    public void close() throws IOException {
        FSIn.close();
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = FSIn.read();
            // end of file:
            if (b == -1)
                return false;
            // save to buffer:
            if (withinBlock)
                buffer.write(b);
            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length)
                    return true;
            } else
                i = 0;
            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && FSIn.getPos() >= end)
                return false;
        }
    }

}