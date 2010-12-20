package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pignlproc.format.WikipediaPageInputFormat;
import pignlproc.format.WikipediaPageInputFormat.WikipediaRecordReader;

/**
 * LoadFunc to load the title and raw markup of wikipedia articles from a pig
 * script.
 */
public class RawWikipediaLoader extends LoadFunc {

    protected WikipediaRecordReader reader;

    protected TupleFactory tupleFactory;

    protected BagFactory bagFactory;

    protected String languageCode = "en";

    public RawWikipediaLoader() {
    }

    public RawWikipediaLoader(String languageCode) {
        this.languageCode = languageCode;
    }

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

            String title = reader.getCurrentKey().toString();
            String rawMarkup = reader.getCurrentValue().toString();

            // TODO: check that the uri generation logic works on non trivial
            // cases (e.g. non latin words)
            String uri = String.format("http://%s.wikipedia.org/wiki/%s",
                    languageCode, title.replaceAll(" ", "_"));

            return tupleFactory.newTupleNoCopy(Arrays.asList(new DataByteArray(
                    title), new DataByteArray(uri),
                    new DataByteArray(rawMarkup)));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = (WikipediaPageInputFormat.WikipediaRecordReader) reader;
        tupleFactory = TupleFactory.getInstance();
        bagFactory = BagFactory.getInstance();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}
