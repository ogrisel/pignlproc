package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pignlproc.format.WikipediaPageInputFormat;
import pignlproc.format.WikipediaPageInputFormat.WikipediaRecordReader;

/**
 * LoadFunc to load the title and raw markup of wikipedia articles from a pig
 * script.
 */
public class WikipediaLoader extends LoadFunc {

    private WikipediaRecordReader reader;

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new WikipediaPageInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean next = reader.nextKeyValue();
            if (!next) {
                return null;
            }
            byte[] title = reader.getCurrentKey().toString().getBytes("UTF-8");
            byte[] rawMarkup = reader.getCurrentValue().toString().getBytes(
                    "UTF-8");
            return TupleFactory.getInstance().newTupleNoCopy(
                    Arrays.asList(new DataByteArray(title), new DataByteArray(
                            rawMarkup)));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = (WikipediaPageInputFormat.WikipediaRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}
