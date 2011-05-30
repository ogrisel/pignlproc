package pignlproc.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * StoreFunc to save key-value pairs using the NTriples format (for a fixed
 * property URI).
 * 
 * @author ogrisel
 */
public abstract class AbstractNTriplesStorer extends StoreFunc {

    protected RecordWriter<Long, Text> writer = null;

    protected byte[] propertyUri;

    protected byte[] subjectNamespace;

    protected byte[] objectNamespace;

    private static final int BUFFER_SIZE = 1024;

    private static final String UTF8 = "UTF-8";

    public AbstractNTriplesStorer(String propertyUri, String subjectNamespace,
	    String objectNamespace) {
	super();
	this.propertyUri = propertyUri.getBytes(Charset.forName(UTF8));
	this.subjectNamespace = subjectNamespace
		.getBytes(Charset.forName(UTF8));
	this.objectNamespace = objectNamespace.getBytes(Charset.forName(UTF8));
    }

    ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);

    protected void putField(Object field) throws IOException {

	switch (DataType.findType(field)) {

	case DataType.BYTEARRAY:
	    byte[] b = ((DataByteArray) field).get();
	    buffer.write(b, 0, b.length);
	    break;

	case DataType.CHARARRAY:
	    // oddly enough, writeBytes writes a string
	    buffer.write(((String) field).getBytes(UTF8));
	    break;

	default:
	    int errCode = 2108;
	    String msg = "Could not determine data type of field: " + field;
	    throw new ExecException(msg, errCode, PigException.BUG);
	}
    }

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() {
	return new TextOutputFormat<WritableComparable, Text>();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepareToWrite(RecordWriter writer) {
	this.writer = writer;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
	job.getConfiguration().set("mapred.textoutputformat.separator", "");
	FileOutputFormat.setOutputPath(job, new Path(location));
	if (location.endsWith(".bz2")) {
	    FileOutputFormat.setCompressOutput(job, true);
	    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	} else if (location.endsWith(".gz")) {
	    FileOutputFormat.setCompressOutput(job, true);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	}
    }
}
