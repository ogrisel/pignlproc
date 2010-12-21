package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import pignlproc.markup.Annotation;
import pignlproc.markup.LinkAnnotationTextConverter;

public class ParsingWikipediaLoader extends RawWikipediaLoader implements
        LoadMetadata {

    public ParsingWikipediaLoader() {
        super();
    }

    public ParsingWikipediaLoader(String languageCode) {
        super(languageCode);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean next = reader.nextKeyValue();
            if (!next) {
                return null;
            }
            String title = reader.getCurrentKey().toString();
            String uri = LinkAnnotationTextConverter.titleToUri(title,
                    languageCode);
            String rawMarkup = reader.getCurrentValue().toString();

            LinkAnnotationTextConverter converter = new LinkAnnotationTextConverter(
                    languageCode);
            String text = converter.convert(rawMarkup);
            String redirect = converter.getRedirect();
            DataBag links = bagFactory.newDefaultBag();
            for (Annotation link : converter.getWikiLinks()) {
                links.add(tupleFactory.newTupleNoCopy(Arrays.asList(link.label,
                        link.value, link.begin, link.end)));
            }
            return tupleFactory.newTupleNoCopy(Arrays.asList(title, uri, text,
                    redirect, links));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Schema schema = new Schema();
        schema.add(new FieldSchema("title", DataType.CHARARRAY));
        schema.add(new FieldSchema("uri", DataType.CHARARRAY));
        schema.add(new FieldSchema("text", DataType.CHARARRAY));
        schema.add(new FieldSchema("redirect", DataType.CHARARRAY));
        Schema linkInfoSchema = new Schema();
        linkInfoSchema.add(new FieldSchema("label", DataType.CHARARRAY));
        linkInfoSchema.add(new FieldSchema("value", DataType.CHARARRAY));
        linkInfoSchema.add(new FieldSchema("begin", DataType.INTEGER));
        linkInfoSchema.add(new FieldSchema("end", DataType.INTEGER));
        schema.add(new FieldSchema("links", linkInfoSchema, DataType.BAG));
        return new ResourceSchema(schema);
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
