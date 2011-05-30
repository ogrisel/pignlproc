/*
 *  (c) Copyright 2001, 2003, 2004, 2005, 2006, 2007, 2008, 2009 Hewlett-Packard Development Company, LP
 *  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.

 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package pignlproc.storage;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pig loader for NTriples formatted RDF files assuming that subject, predicate
 * are valid URIs (e.g. clear of any whitespace character) and that the object
 * is Literal.
 * 
 * Returns the subject and the object (parsed as a String) and the language code
 * if any (or null). The property value can be given as a filter.
 * 
 * The NTriples literal parsing is taken from the Jena project (which
 * unfortunately does not offer a pure String API that does not allocate Jena
 * specific java objects.
 */
public class UriStringLiteralNTriplesLoader extends AbstractNTriplesLoader {

    public UriStringLiteralNTriplesLoader() {
        this(null, null);
    }

    public UriStringLiteralNTriplesLoader(String propertyUri, String subjectPrefix) {
        super();
        this.propertyUri = propertyUri;
        this.subjectPrefix = subjectPrefix;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Schema schema = new Schema();
        schema.add(new FieldSchema("subject", DataType.CHARARRAY));
        schema.add(new FieldSchema("object", DataType.CHARARRAY));
        schema.add(new FieldSchema("lang", DataType.CHARARRAY));
        return new ResourceSchema(schema);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            String line = null;
            while (reader.nextKeyValue()) {
                line = reader.getCurrentValue().toString();
                String[] split = line.split(" ", 3);
                if (split.length != 3) {
                    // unexpected spaces, the object must be a literal, skip
                    continue;
                }
                if (propertyUri != null
                        && !propertyUri.equals(stripBrackets(split[1]))) {
                    // property value is not matching the filter
                    // TODO: log errors?
                    continue;
                }
                if (!split[2].endsWith(" .")) {
                    // TODO: log errors?
                    continue;
                }
                try {
                    String[] object = parseStringLiteral(split[2].substring(0,
                            split[2].length() - 2));
                    if (object == null) {
                        // TODO: log errors?
                        continue;
                    }
                    return tupleFactory.newTupleNoCopy(Arrays.asList(
                            stripBrackets(split[0], subjectPrefix), object[0], object[1]));
                } catch (IOException e) {
                    // TODO: log errors?
                    continue;
                }
            }
            if (line != null) {
                // we reached the end of the file split with an non matching
                // line: return the (null, null) tuple instead of just null
                // since pig was not expecting this split to be fully consumed
                // by introspecting the InputFormat progress data.
                return tupleFactory.newTupleNoCopy(Arrays.asList(null, null));
            }
            return null;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            throw new IOException(e);
        }
    }

    private boolean expect(String str, IStream in) throws IOException {
        for (int i = 0; i < str.length(); i++) {
            char want = str.charAt(i);
            if (in.eof()) {
                return false;
            }
            char inChar = in.readChar();
            if (inChar != want) {
                return false;
            }
        }
        return true;
    }

    private String[] parseStringLiteral(String ntString) throws IOException {
        IStream in = new IStream(new StringReader(ntString));

        StringBuffer lit = new StringBuffer(ntString.length());

        if (!expect("\"", in)) {
            return null;
        }

        while (true) {
            char inChar = in.readChar();
            if (in.eof()) {
                return null;
            }
            if (inChar == '\\') {
                char c = in.readChar();
                if (in.eof()) {
                    return null;
                }
                if (c == 'n') {
                    inChar = '\n';
                } else if (c == 'r') {
                    inChar = '\r';
                } else if (c == 't') {
                    inChar = '\t';
                } else if (c == '\\' || c == '"') {
                    inChar = c;
                } else if (c == 'u') {
                    inChar = readUnicode4Escape(in);
                    if (inChar == 0)
                        return null;
                } else {
                    return null;
                }
            } else if (inChar == '"') {
                String lang;
                if ('@' == in.nextChar()) {
                    expect("@", in);
                    lang = readLang(in);
                } else if ('-' == in.nextChar()) {
                    expect("-", in);
                    lang = readLang(in);
                } else {
                    lang = "";
                }
                return new String[] { lit.toString(), lang };
            }
            lit = lit.append(inChar);
        }
    }

    private char readUnicode4Escape(IStream in) throws IOException {
        char buf[] = new char[] { in.readChar(), in.readChar(), in.readChar(),
                in.readChar() };
        if (in.eof()) {
            return 0;
        }
        try {
            return (char) Integer.parseInt(new String(buf), 16);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String readLang(IStream in) throws IOException {
        StringBuffer lang = new StringBuffer(15);
        while (!in.eof()) {
            char inChar = in.nextChar();
            if (Character.isWhitespace(inChar) || inChar == '.'
                    || inChar == '^') {
                return lang.toString();
            }
            lang = lang.append(in.readChar());
        }
        return lang.toString();
    }

    public static class IStream {

        // simple input stream handler

        Reader in;

        char[] thisChar = new char[1];

        boolean eof;

        protected IStream(Reader in) throws IOException {
            this.in = in;
            eof = (in.read(thisChar, 0, 1) == -1);
        }

        protected char readChar() throws IOException {
            if (eof) {
                return '\000';
            }
            char rv = thisChar[0];
            eof = (in.read(thisChar, 0, 1) == -1);
            return rv;
        }

        protected char nextChar() {
            return eof ? '\000' : thisChar[0];
        }

        protected boolean eof() {
            return eof;
        }

    }
}
