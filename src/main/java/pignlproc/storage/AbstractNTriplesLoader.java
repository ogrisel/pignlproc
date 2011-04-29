package pignlproc.storage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.TupleFactory;

public abstract class AbstractNTriplesLoader extends LoadFunc implements
        LoadMetadata {

    protected RecordReader<Long, Text> reader;

    protected TupleFactory tupleFactory = TupleFactory.getInstance();

    protected String propertyUri;

    protected String subjectPrefix;

    protected String objectPrefix;

    protected String checkPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty() || prefix.endsWith(":")) {
            return prefix;
        }
        return prefix + ":";
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = reader;
    }

    protected String stripBrackets(String bracketedURI) {
        return bracketedURI.substring(1, bracketedURI.length() - 1);
    }

    protected String stripBrackets(String bracketedURI, String prefix) {
        String unbracketed = bracketedURI.substring(1,
                bracketedURI.length() - 1);
        if (prefix != null) {
            String[] uriParts = unbracketed.split("/");
            String lastPart = uriParts[uriParts.length - 1];
            return prefix + lastPart;
        }
        return unbracketed;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
    }

}
