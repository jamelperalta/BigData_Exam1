import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Jamel Peralta Coss
 */
public class KeywordToTweetReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String v = "";
        for (Text value : values){
          v += ", " + value;
        }
        context.write(key, new Text(v));
    }
}
