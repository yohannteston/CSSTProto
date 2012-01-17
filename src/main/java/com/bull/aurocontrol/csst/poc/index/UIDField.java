package com.bull.aurocontrol.csst.poc.index;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.util.NumericUtils;

public class UIDField extends AbstractField {

    private final UIDTokenStream tokenStream;

    public UIDField(String name, int uid) {
        super(name, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);
        this.tokenStream = new UIDTokenStream(uid);
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    @Override
    public TokenStream tokenStreamValue() {
        return tokenStream;
    }

    private static class UIDTokenStream extends TokenStream {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);

        private final Payload payload;
        private static final char[] token = "_UID_".toCharArray();

        private boolean returnToken = true;

        public UIDTokenStream(int uid) {
            super();
            payload = new Payload(PayloadHelper.encodeInt(uid));
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (returnToken) {
                returnToken = false;
                char[] buffer = termAtt.resizeBuffer(token.length);
                System.arraycopy(token, 0, buffer, 0, token.length);
                termAtt.setLength(token.length);
                payAtt.setPayload(payload);
                return true;
            } else {
                return false;
            }
        }
    }

    

    public static int[] load(IndexReader indexReader, String name) {
        TermPositions tp = null;
        byte[] dataBuffer = new byte[4];
        int[] uidArray = new int[indexReader.maxDoc()];
        try {
            tp = indexReader.termPositions(new Term(name, "_UID_"));
            while (tp.next()) {
                int doc = tp.doc();
                tp.nextPosition();
                tp.getPayload(dataBuffer, 0);
                // convert buffer to int
                int uid = PayloadHelper.decodeInt(dataBuffer, 0);

                uidArray[doc] = uid;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (tp != null) {
                try {
                    tp.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return uidArray;
    }

}
