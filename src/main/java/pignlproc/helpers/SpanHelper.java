package pignlproc.helpers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import opennlp.tools.util.Span;

/**
 * Utility to convert text span from the native pig representation (int or bags
 * of ints + type) to the OpenNLP {@code Span} instances.
 */
public class SpanHelper {

    public static List<Span> tupleFieldsToSpans(Object fieldBegin,
            Object fieldEnd) throws ExecException {
        return tupleFieldsToSpans(fieldBegin, fieldEnd, null);
    }

    public static List<Span> tupleFieldsToSpans(Object beginField,
            Object endField, Object typeField) throws ExecException {
        List<Span> spans = new ArrayList<Span>();
        if (beginField == null || endField == null) {
            return spans;
        }
        if (beginField instanceof DataBag && endField instanceof DataBag) {
            Iterator<Tuple> beginIterator = ((DataBag) beginField).iterator();
            Iterator<Tuple> endIterator = ((DataBag) endField).iterator();
            Iterator<Tuple> typeIterator = null;
            String type = null;
            if (typeField instanceof DataBag) {
                typeIterator = ((DataBag) typeField).iterator();
            } else {
                type = (String) typeField;
            }
            while (beginIterator.hasNext() && endIterator.hasNext()) {
                int begin = (Integer) beginIterator.next().get(0);
                int end = (Integer) endIterator.next().get(0);
                if (typeIterator != null) {
                    type = (String) typeIterator.next().get(0);
                }
                spans.add(new Span(begin, end, type));
            }
        } else {
            spans.add(new Span((Integer) beginField, (Integer) endField,
                    (String) typeField));
        }
        return spans;
    }
}
