package Main;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static List<String> nGrams;


    private List<String> findNGram(String word, int nGramLength){
        //If the word input has more character in it than the specified n-gram value
        if (word.length() > nGramLength) {
           // if (word.substring(0, nGramLength).matches(" +"))
                nGrams.add(word.substring(0, nGramLength));
                findNGram(word.substring(1, word.length()),nGramLength);//Remove the first character then hand the result back to the function
        }
        else if(word.length() == nGramLength ) {
            nGrams.add(word);
        }
        return nGrams;
    }


    private static List<String> findWordGram(List<String> words, int nGramLength){
        String nWordGram = "";
        //If the array of words is less than the nGram length don't continue
        if (words.size() >= nGramLength) {
            for (int i = 0; i < nGramLength; i++) {
                nWordGram += words.get(i) + " ";
            }
            nGrams.add(nWordGram);
            words.remove(0);//Remove the first word from the array
            findWordGram(words, nGramLength);
        }
        return nGrams;
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //Reads the next byte of the data from the the input stream
            //And returns an int in the range of 0 to 255. If no byte is available because the end of the stream has been reached
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            //factory API that enables applications to obtain a parser that produces DOM object trees from XML documents.
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            //method parses the content of the given input source as an XML document and return a new DOM Document object
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            //Create a Document from a stream
            Document doc = dBuilder.parse(is);
            doc.getDocumentElement().normalize();
            //nodes are returned in the order in which they are specified in the XML document.
            NodeList nList = doc.getElementsByTagName("tuv");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                //Get the next node
                Node nNode = nList.item(temp);
                //If the node is the right element "tuv"
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    //Get the content held in  the node and remove everything that is not a letter
                    //[^\p{L}] = not just latin alphabet
                    String text = eElement.getElementsByTagName("seg").item(0).getTextContent().replaceAll("[^\\p{L}]", " ");
                    nGrams = new ArrayList<>();
                    //Remove white spaces
                    text = text.replaceAll("\\s+"," ").trim();
                    List<String> words = new ArrayList<>();
                    //Add individual words as elements to the list
                    Collections.addAll(words, text.split(" "));
                    //Find whether user wanted to get word grams or char grams
                    switch (context.getConfiguration().get("N_GRAM_TYPE")){
                        case("c"):{
                            for (String word : words) {
                                //Write the returned data
                                for (String nGram : findNGram(word,context.getConfiguration().getInt("N_GRAM_LENGTH",0))){
                                    context.write(new Text(nGram), one);
                                }
                            }
                            break;
                        }
                        case ("w"):{
                            //Write the returned data
                            for (String nGram : findWordGram(words,context.getConfiguration().getInt("N_GRAM_LENGTH",0))){
                                context.write(new Text(nGram), one);
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {
            System.out.println("Something bad happened here. " + ignored.getMessage());
        }

    }

}
