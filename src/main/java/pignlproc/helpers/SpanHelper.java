package pignlproc.helpers;

import java.util.ArrayList;
import java.util.List;

import opennlp.tools.util.Span;

/**
 * Utility to convert text span from the native pig representation (int or bags
 * of ints + type) to the OpenNLP {@code Span} instances.
 */
public class SpanHelper {

    public static List<Span> tupleFieldsToSpans(Object fieldBegin,
            Object fieldEnd) {
        return tupleFieldsToSpans(fieldBegin, fieldEnd, null);
    }

    public static List<Span> tupleFieldsToSpans(Object fieldBegin,
            Object fieldEnd, Object fieldType) {
        List<Span> spans = new ArrayList<Span>();
        // TODO: implement me!
        return spans;
    }

}
