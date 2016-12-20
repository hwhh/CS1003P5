package Main;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import javax.swing.*;
import javax.swing.text.Document;
import javax.swing.text.EditorKit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class Main {

    private static final String INTERMEDIATE = "intermediate";
    static GUI gui;

    public static void main(String[] args) {
        //Start GUI on new thread
        java.awt.EventQueue.invokeLater(() -> {
            gui = new GUI();
            gui.setVisible(true);
        });
    }


    static void startJob(){
        try {
            //Create new configuration object used to store data that can be retried elsewhere
            Configuration conf = new Configuration();
            //Set information input from the GUI
            conf.set("START_TAG_KEY", "<tuv lang=\""+gui.getLanguage()+"\">");
            conf.set("END_TAG_KEY", "</tuv>");
            conf.setInt("N_GRAM_LENGTH", gui.getnGramLength());
            conf.set("N_GRAM_TYPE",Main.gui.getnGramType());
            //Create new JOB object
            Job job = new Job(conf, "XML Processing");
            job.setJarByClass(Main.class);
            // The output of the mapper is a map from words (including duplicates) to the value 1.
            job.setMapperClass(MyMapper.class);
            // The output of the reducer is a map from unique words to their total counts.
            job.setCombinerClass(MyReducer.class);
            job.setReducerClass(MyReducer.class);
            //Input format set to custom made XML format
            job.setInputFormatClass(XMLInputFormat.class);
            //Output set to integer
            job.setOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            //Get the input and and output directories
            FileInputFormat.addInputPath(job, new Path(gui.getInputPath()));
            //If an "intermediate" file exists in current directory delete it
            if(!gui.getIsSortAlphabetically()) {
                FileUtils.deleteDirectory(new File(INTERMEDIATE));
                //Set the output directory to new directory
                FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE));
            }
            else{
                FileUtils.deleteDirectory(new File(gui.getOutputPath()));
                //If an "output" file exists in current directory delete it
                FileOutputFormat.setOutputPath(job, new Path(gui.getOutputPath()));
            }
            job.waitForCompletion(true);
            //Once the job is complete move on
            if(job.isComplete())
                if(gui.getIsSortAlphabetically()){
                    JOptionPane.showMessageDialog( Main.gui,"Job Complete. Output file stored at:\n"+ Main.gui.getOutputPath(),"Done.", JOptionPane.INFORMATION_MESSAGE);
                    populateTextArea(Main.gui.getOutputPath() + "/part-r-00000");
                }
                else
                    sort();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void sort (){
        try {
            // This produces an output file containing a single line with the most popular word followed by
            // its total number of occurrences in all the input files.
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Sort");
            job.setJarByClass(Main.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            // The output of the mapper is a map from counts (including duplicates) to words.
            job.setMapperClass(MyInverseMapper.class);
            // Output from reducer maps words to counts.
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            // The output of the reducer is the solely the first value/key pair encountered, swapped.
            // Since keys are ordered in decreasing order, the first is the most popular.


            FileInputFormat.setInputPaths(job, new Path(INTERMEDIATE));
            FileUtils.deleteDirectory(new File(gui.getOutputPath()));
            //If an "output" file exists in current directory delete it
            FileOutputFormat.setOutputPath(job, new Path(gui.getOutputPath()));
            job.waitForCompletion(true);
            if(job.isComplete()) {
                //Once the job is complete move on
                JOptionPane.showMessageDialog( Main.gui,"Job Complete. Output file stored at:\n"+ Main.gui.getOutputPath(),"Done.", JOptionPane.INFORMATION_MESSAGE);
                populateTextArea(Main.gui.getOutputPath() + "/part-r-00000");
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void populateTextArea(String output){
        Main.gui.getjTextAreaOut().setText("");
        try {
            //read in the data from the output files and add them to the text area object in the GUI =
            FileReader reader = new FileReader(output);
            BufferedReader br = new BufferedReader(reader);
            EditorKit kit = Main.gui.getjTextAreaOut().getUI().getEditorKit(Main.gui.getjTextAreaOut());
            Document doc = Main.gui.getjTextAreaOut().getDocument();
            kit.read(br, doc, doc.getLength());
            br.close();
        } catch (Exception e2) {
            System.out.println(e2);
        }

    }


}
