package test.spark;

public class SampleDataMapper {
		
		private String input;
		
		SampleDataMapper(String inp) {
			this.input = inp;
		}
		
		public String modify() {
			return "Business date -> " + this.input;
		}

}
