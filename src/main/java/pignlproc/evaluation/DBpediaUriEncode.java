package pignlproc.evaluation;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.dbpedia.extraction.util.WikiUtil;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * UDF to encode Wikipedia URLs as DBpedia URIs.
 */
public class DBpediaUriEncode extends EvalFunc<String> {

    private Pattern percentEncodedPattern = Pattern.compile("%\\d\\d");

    private String wikipediaUrlPrefix;
    private String dbpediaUriPrefix;
    private String dbpediaCanonicalUriPrefix;

    public DBpediaUriEncode(String language) {
        this.wikipediaUrlPrefix = "http://" + language + ".wikipedia.org/wiki/";
        this.dbpediaUriPrefix = "http://" + language + ".dbpedia.org/resource/";
        this.dbpediaCanonicalUriPrefix = "http://.dbpedia.org/resource/";
    }


    @Override
    public String exec(Tuple input) throws IOException {
        Object input1 = input.get(0);
        if (input1 == null) {
            return null;
        }
        String in = input1.toString();

        in = in.replace(this.wikipediaUrlPrefix, "");
        in = in.replace(this.dbpediaUriPrefix, "");
        in = in.replace(this.dbpediaCanonicalUriPrefix, "");

        if (percentEncodedPattern.matcher(in).find()) {
            in = WikiUtil.wikiDecode(in);
        }

        String encoded = WikiUtil.wikiEncode(in);
        return this.dbpediaUriPrefix + encoded;
    }

}
