package pignlproc.storage;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;

/**
 * StoreFunc to save key-value pairs using the NTriples format (for a fixed property URI).
 * 
 * @author ogrisel
 */
public abstract class AbstractNTriplesStorer extends StoreFunc {

    protected RecordWriter<Long,String> writer = null;

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() {
        return new TextOutputFormat<WritableComparable,Text>();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
        if ("true".equals(job.getConfiguration().get("output.compression.enabled"))) {
            FileOutputFormat.setCompressOutput(job, true);
            String codec = job.getConfiguration().get("output.compression.codec");
            try {
                FileOutputFormat.setOutputCompressorClass(job,
                    (Class<? extends CompressionCodec>) Class.forName(codec));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + codec);
            }
        } else {
            if (location.endsWith(".bz2") || location.endsWith(".bz")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
            } else if (location.endsWith(".gz")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            } else {
                FileOutputFormat.setCompressOutput(job, false);
            }
        }
    }
}
