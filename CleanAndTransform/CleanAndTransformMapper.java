import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class CleanAndTransformMapper extends Mapper <Text, Text, Text, Text> { 

	private Text baseUrl = new Text();
	private Text links = new Text();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
		StringBuilder stringLinks = null;
		baseUrl = key;
		

		String line = value.toString();
		JsonElement jelement = new JsonParser().parse(line);
		JsonObject  jobjectContent = jelement.getAsJsonObject();
		jobjectContent = jobjectContent.getAsJsonObject("content");

		if (jobjectContent!=null)
		{
			JsonArray jarrayLinks = jobjectContent.getAsJsonArray("links");
			if (jarrayLinks!=null&&jarrayLinks.size()>0)
			{
				for(int i=0;i<jarrayLinks.size();i++)
				{
					JsonObject temp = jarrayLinks.get(i).getAsJsonObject();
					String aLink = temp.get("href").toString();
					aLink = aLink.replace("\"", "");

					String type = temp.get("type").toString();
					if (type.equals("\"a\""))
					{	if (!aLink.contains(",") )

						{
							if (stringLinks==null)
							{
								stringLinks=new StringBuilder();
								stringLinks.append(aLink);
							}
							else
							{
								stringLinks.append(","+aLink);

							}
						}
					}
				}
			}

		}
		if (stringLinks==null)
		{
			links=new Text("");
			
		}
		else
		{
			links=new Text(stringLinks.toString());

		}
		
		
		context.write(baseUrl,links);

	}


}
