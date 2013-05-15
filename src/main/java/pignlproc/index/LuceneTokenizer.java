package pignlproc.index;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 /**
 * @author chrishokamp
 * Take a bag of paragraphs (usage contexts) and emit a bag of tokens using the Lucene analyzer
 *
 */
public class LuceneTokenizer extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    //String language;

    //Hard-coded for the Lucene analyzer because this is unnecessary for this implementation
    String field = "paragraph";

    private String stoplist_path = ""; //the path to the stoplist
    private String stoplist_name = ""; //the name of the stoplist

    private Boolean hasStoplist = false;

    private HashSet<String> stopset = null;
    protected Analyzer analyzer;
    private TokenStream stream = null;
    private String analyzerClassName; //the name of the analyzer to use i.e. "org.EnglishAnalyzer"

    public LuceneTokenizer(String stopPath, String stopName, String langCode, String luceneAnalyzer) throws  IOException {
        stoplist_path = stopPath;
        stoplist_name = stopName;
        hasStoplist = true;
        analyzerClassName = "org.apache.lucene.analysis." + langCode + "." + luceneAnalyzer;
    }

    public LuceneTokenizer(String langCode, String luceneAnalyzer) {
        analyzerClassName = "org.apache.lucene.analysis." + langCode + "." + luceneAnalyzer;
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(stoplist_path);
        return list;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {

        if (hasStoplist == true) {
            try {
                //uses hadoop distributed cache (via getCacheFiles)
                FileReader fr = new FileReader("./" + stoplist_name);

                BufferedReader br = new BufferedReader(fr);
                String line = null;
                stopset = new HashSet<String>();
                while ((line = br.readLine()) != null)
                {
                    stopset.add(line);
                }
            } catch (FileNotFoundException e) {
                //String msg = "Couldn't find the stoplist file: %s".format(stoplist_name);
                e.printStackTrace();
            }
        }

        try {
            if (stopset != null) {
                analyzer = (Analyzer)Class.forName(analyzerClassName).getConstructor(Version.class, Set.class).newInstance(Version.LUCENE_36, stopset);
            } else {
                analyzer = (Analyzer)Class.forName(analyzerClassName).getConstructor(Version.class).newInstance(Version.LUCENE_36);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        DataBag out = bagFactory.newDefaultBag();

        Object t0 = input.get(0);
        if (!(t0 instanceof String)) {
          throw new IOException(
                 "Expected a String, but this is a "
                         + t0.getClass().getName());
         }

        stream = analyzer.reusableTokenStream(field, new StringReader((String)t0));

        try {
            while (stream.incrementToken()) {
                String token = stream.getAttribute(CharTermAttribute.class).toString();
                out.add(tupleFactory.newTuple(token));
            }
        }
        catch (IOException e) {
               throw e;
        }
        return out;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("token", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

}






