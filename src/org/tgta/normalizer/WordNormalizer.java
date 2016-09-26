/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.tgta.normalizer;

/**
 *
 * @author UdayaK
 */
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration.*;
import org.apache.commons.lang.exception.NestableException;

public class WordNormalizer {
    Dictionary wordList;
    Set<String> dictionary;
    private static String dicFile = "";
    
    public void init(){
        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);
            
            dicFile = config.getString("word-normalizer.normalization-dic");
            
            
            wordList = new Hashtable();
            dictionary = new HashSet<>();
            
            InputStream in = new FileInputStream(new File(dicFile));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] arr = line.toLowerCase().split(",");
                wordList.put(arr[0], arr[1]);
                dictionary.add(arr[0]);
            }
            reader.close();
        } catch (ConfigurationException | IOException ex) {
            System.out.println("Error occurred while init WordNormalizer....");
            System.out.println(ex);
            System.exit(0);
        }
    }
    
//    public static void main(String[] args) throws IOException {
//        WordNormalizer wn = new WordNormalizer();
//        wn.init();
//        
//        String input = "gr8a";
//        String rs = wn.getWord(input);
//        
//        if(rs != null){
//            System.out.println(input + " => " + rs);
//        }else{
//            System.out.println("No match !");
//        }       
//    }
    
    public String getWord(String input){
        // load the dictionary into a set for fast lookups
        
        // place to store list of results, each result is a list of strings
        List<List<String>> results = new ArrayList<List<String>>();

        // start the search, pass empty stack to represent words found so far
        search(input, dictionary, new Stack<String>(), results);
        
        if(results.size() > 0){
            return (String)wordList.get(input);
        }
        
        return null;
    }

    public static void search(String input, Set<String> dictionary,
            Stack<String> words, List<List<String>> results) {

        for (int i = 0; i < input.length(); i++) {
            // take the first i characters of the input and see if it is a word
            String substring = input.substring(0, i + 1);

            if (dictionary.contains(substring)) {
                // the beginning of the input matches a word, store on stack
                words.push(substring);

                if (i == input.length() - 1) {
                    // there's no input left, copy the words stack to results
                    results.add(new ArrayList<String>(words));
                } else {
                    // there's more input left, search the remaining part
                    search(input.substring(i + 1), dictionary, words, results);
                }

                // pop the matched word back off so we can move onto the next i
                words.pop();
            }
        }
    }
}
