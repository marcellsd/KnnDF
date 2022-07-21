package df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class KnnSparkDF {
	//double[] query = {5.1,3.5,1.4,0.2}; //iris-setosa
	//double[] query = {7.0,3.2,4.5,1.5}; //iris-versicolor
	static double[] query = {6.0,2.2,5.0,2.3}; //iris-virginica 
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("KnnDF from CSV")
				.master("local").getOrCreate();
		
		StructType schema = DataTypes.createStructType(new StructField[]{
			DataTypes.createStructField(
					"Attr1",
					DataTypes.DoubleType,true),
			
			DataTypes.createStructField(
					"Attr2",
					DataTypes.DoubleType,true),
			
			DataTypes.createStructField(
					"Attr3",
					DataTypes.DoubleType,true),
			
			DataTypes.createStructField(
					"Attr4",
					DataTypes.DoubleType,true),
			DataTypes.createStructField(
					"Class",
					DataTypes.StringType,true)
		});
		
		
		Dataset<Row> df = spark.read().format("csv").option("header", true).schema(schema)
				.load("/home/marcellsd/Desktop/Primeira Atividade Programação concorrente/csvIrisData50pwl");
		df.createOrReplaceTempView("csvRawData");
		
		spark.udf().register("calcEuclideanDistance",new CalculateEuclideanDistance(), DataTypes.DoubleType);
		
		Dataset<Row> finalDf = spark.sql(
				"SELECT Attr1, Attr2, Attr3, Attr4, Class, "
				+ "calcEuclideanDistance("
				+ "Attr1, Attr2,"
				+ "Attr3, Attr4)"
				+ "AS Distance FROM csvRawData"
				);
		finalDf.orderBy("Distance").show(5);
		
		
	}
	
	
	public static class CalculateEuclideanDistance implements UDF4<Double, Double, Double, Double, Double>{
		private static final long serialVersionUID = -216751L;

		@Override
		public Double call(Double at1, Double at2, Double at3, Double at4) throws Exception {
			    Double dist = 0.0;
				dist += Math.pow(at1 - query[0], 2);
				dist += Math.pow(at2 - query[1], 2);
				dist += Math.pow(at3 - query[2], 2);
				dist += Math.pow(at4 - query[3], 2);
				
				Double distance = Math.sqrt(dist);
				return distance;
			}
			
		}
}
