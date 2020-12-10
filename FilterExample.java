package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

//This filter implements the Serializable Funtions that inputs a String and returns a Boolean
class MyFilter implements SerializableFunction<String, Boolean>{
	
	@Override
	public Boolean apply(String input) {
		// TODO Auto-generated method stub
		
		return input.contains("Los Angeles");

	}
}

public class FilterExample {

	public static void main(String[] args) {

		Pipeline p = Pipeline.create();

		PCollection<String> pCustList = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section3/customer_pardo.csv"));

		// Using Filter

		PCollection<String> pOutput = pCustList.apply(Filter.by(new MyFilter()));
		
		pOutput.apply(TextIO.write().to("/home/sabb/Documents/Beam/Section3/customer_Filter_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		p.run();

	}

}