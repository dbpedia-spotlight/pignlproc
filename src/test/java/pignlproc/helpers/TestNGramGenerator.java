package pignlproc.helpers;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.dbpedia.spotlight.db.model.Stemmer;
import org.dbpedia.spotlight.db.model.StringTokenizer;
import org.dbpedia.spotlight.db.tokenize.OpenNLPStringTokenizer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class TestNGramGenerator {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    private void test(String testString, Set<String> correctNgrams, int ngramLength) throws IOException {
        InputStream modelIs = this.getClass().getClassLoader().getResourceAsStream("opennlp/en-token.bin");
        StringTokenizer stringTokenizer = new OpenNLPStringTokenizer(
                new TokenizerME(new TokenizerModel(modelIs)),
                new Stemmer());
        RestrictedNGramGenerator generator = new RestrictedNGramGenerator(ngramLength, stringTokenizer);
        Tuple inputTuple = tupleFactory.newTuple(testString);
        DataBag resultBag = generator.exec(inputTuple);
        for (Tuple tuple : resultBag) {
            String ngram = (String) tuple.get(0);
            Assert.assertTrue("'" + ngram + "' not in corrects: " + correctNgrams, correctNgrams.contains(ngram));
            correctNgrams.remove(ngram);
        }
        Assert.assertSame("left-over corrects, nothing left in output", 0, correctNgrams.size());
    }

    @Test
    public void easy() throws IOException {
        String testString = "This is an easy example";
        Set<String> correctNgrams = new TreeSet<String>();
        // len 1
        correctNgrams.add("This");
        correctNgrams.add("is");
        correctNgrams.add("an");
        correctNgrams.add("easy");
        correctNgrams.add("example");
        // len 2
        correctNgrams.add("This is");
        correctNgrams.add("is an");
        correctNgrams.add("an easy");
        correctNgrams.add("easy example");
        // len 3
        correctNgrams.add("This is an");
        correctNgrams.add("is an easy");
        correctNgrams.add("an easy example");
        test(testString, correctNgrams, 3);
    }

    @Test
    public void medium() throws IOException {
        String testString = "That's a little harder.";
        Set<String> correctNgrams = new HashSet<String>();
        // len 1
        correctNgrams.add("That");
        correctNgrams.add("'s");
        correctNgrams.add("a");
        correctNgrams.add("little");
        correctNgrams.add("harder");
        correctNgrams.add(".");
        // len 2
        correctNgrams.add("That's");
        correctNgrams.add("'s a");
        correctNgrams.add("a little");
        correctNgrams.add("little harder");
        correctNgrams.add("harder.");
        // len 3
        correctNgrams.add("That's a");
        correctNgrams.add("'s a little");
        correctNgrams.add("a little harder");
        correctNgrams.add("little harder.");
        test(testString, correctNgrams, 3);
    }

}
