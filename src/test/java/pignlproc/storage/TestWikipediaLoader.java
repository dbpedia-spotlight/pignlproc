package pignlproc.storage;

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestWikipediaLoader {

    @Test
    public void testRawWikipediaLoader() throws Exception {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "enwiki-20090902-pages-articles-sample.xml");
        String filename = wikiDump.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + filename
                + "' USING pignlproc.storage.RawWikipediaLoader()"
                + " as (title: chararray, uri: chararray, markup: chararray);";
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (tuple == null) {
                throw new Exception("got unexpected null tuple");
            } else {
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(4, tupleCount);
    }

    @Test
    public void testParsingWikipediaLoader() throws Exception {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "enwiki-20090902-pages-articles-sample.xml");
        String filename = wikiDump.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + filename
                + "' USING pignlproc.storage.ParsingWikipediaLoader('en')"
                + " as (title: chararray, id: chararray, uri: chararray, text: chararray,"
                + " redirect: chararray, links, headers, paragraphs, boldforms);";
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (tuple == null) {
                throw new Exception("got unexpected null tuple");
            } else {
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(4, tupleCount);
    }
    
    @Test
    public void testBoldformExtraction() throws Exception {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "Brian_Eno.xml");
        String filename = wikiDump.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + filename
                + "' USING pignlproc.storage.ParsingWikipediaLoader('en')"
                + " as (title: chararray, id: chararray, uri: chararray, text: chararray,"
                + " redirect: chararray, links, headers, paragraphs, boldforms);";
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        int tupleCount = 0;
        int idx = 0;
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (tuple == null) {
                throw new Exception("got unexpected null tuple");
            } else {
                if (tuple.size() > 0) {
                    Object test = tuple.get(8);
                    if (!(test instanceof DataBag)) {
                        throw new Exception("Doesn't get proper Boldform DataBag");
                    }
                    DataBag boldforms = (DataBag) test;
                    for (Tuple b : boldforms) {
                        switch (idx) {
                        case 0 : 
                            assertEquals(4, ((Integer) b.get(1)));
                            assertEquals(59, (Integer) b.get(2));
                            break;
                        case 1 : 
                            assertEquals(111, (Integer) b.get(1));
                            assertEquals(133, (Integer) b.get(2));
                            break;
                        case 2 : 
                            assertEquals(160, (Integer) b.get(1));
                            assertEquals(169, (Integer) b.get(2));
                            break;
                        case 3 : 
                            assertEquals(180, (Integer) b.get(1));
                            assertEquals(183, (Integer) b.get(2));
                            break;
                        default : 
                            throw new Exception("got unexpected case in boldforms testing");
                        }
                        idx++;
                    }
                    tupleCount++;
                }
            }
            assertEquals(4, idx);
            assertEquals(1, tupleCount);
        }
    }
}
