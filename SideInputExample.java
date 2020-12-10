package section3;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

//with the help of side input, you can provide additional inputs to a PARDO transform
//in the form of side inputs


//in this example we have a return.csv file with the custumer that returns a product and is included 
//in purchase order cust_order.csv. We will get the customers that never have returned the products 

public class SideInputExample {
	public static void main(String[] args) {
		Pipeline p = Pipeline.create();
		
		PCollection<KV<String,String>> pReturn = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section3/return.csv"))
		.apply(ParDo.of(new DoFn<String, KV<String, String>>(){
			
			@ProcessElement
			public void process(ProcessContext c) {
				String arr[] = c.element().split(",");
				c.output(KV.of(arr[0], arr[1]));

			}
		}));
		
		PCollectionView<Map<String,String>> pMap = pReturn.apply(View.asMap());
		
		PCollection<String> pCustList = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section3/cust_order.csv"));
		
		pCustList.apply(ParDo.of(new DoFn<String,Void>(){
			
			
			@ProcessElement
			public void process(ProcessContext c) {
				Map<String,String> psideInputView = c.sideInput(pMap);
				
				String arr[] = c.element().split(",");
				
				String custName = psideInputView.get(arr[0]);
				
				//System.out.println(custName);
				
				//return.csv values included in cust_order.csv
				//customers that aren't return the item
				if(custName==null) {
					System.out.println(c.element());
				}

			}
			
		}).withSideInputs(pMap));
		
		
		p.run();
	}
}
