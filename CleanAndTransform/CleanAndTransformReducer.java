import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CleanAndTransformReducer extends Reducer <Text,Text,Text,Text>

{
	String links;
	
	public void reduce (Text baseUrl, Iterable <Text> values, Context context) throws IOException, InterruptedException {
	
		StringBuilder linkedUrls = null;
		
		for(Text value:values)
		{
			String val = value.toString();
			
			if (!val.equals(""))
			{
				if (linkedUrls==null)
				{
					linkedUrls=new StringBuilder();
					linkedUrls.append(val);
				}
				else
				{
					linkedUrls.append(","+val);

				}

			}

		}
		
		if (linkedUrls==null)
		{
			links = new String("1.0\t");	
		}
		else
		{
			linkedUrls.insert(0, "1.0\t");
			links=linkedUrls.toString();	
		}
		
		context.write(baseUrl,new Text(links));
		
		
	}
}
