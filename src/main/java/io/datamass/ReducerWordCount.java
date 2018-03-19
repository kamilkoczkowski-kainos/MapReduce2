
package io.datamass;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerWordCount extends Reducer<IntWritable, Text, IntWritable, Text>
{
    public void reduce(IntWritable column, Iterable<Text> values, Context con) throws IOException, InterruptedException
    {
        Map<Text, Integer> myMap = new HashMap<Text, Integer>();
        int countInt = 0;
        for (Text word : values)
        {
            int count = myMap.containsKey(word) ? myMap.get(word) : 0;
            myMap.put(new Text(word), count + 1);
            countInt++;
        }
        for (Map.Entry<Text, Integer> entry : myMap.entrySet()) {
            String out = String.format("word: %s  how many: %s [%.2f %%]",
                    entry.getKey().toString(),
                    entry.getValue().toString(),
                    entry.getValue() * 100F / countInt);


            con.write(column, new Text(out));
        }

    }
}