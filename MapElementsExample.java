package section3;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

//MapElements - changing names to uppercase
public class MapElementsExample {

	public static void main(String[] args) {
		
		Pipeline p = Pipeline.create();
		
		PCollection<String> pCustList= p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section3/customer.csv"));

		//Using TypeDescriptors

		PCollection<String> pOutput=pCustList.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));
				
		pOutput.apply(TextIO.write().to("/home/sabb/Documents/Beam/Section3/output_customer.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
		
	}
}