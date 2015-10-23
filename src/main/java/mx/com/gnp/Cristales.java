package mx.com.gnp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Cristales implements GNPConstants {
	
	public static final int RECLAMA 	= 13;
	private JavaSparkContext sc;
	
	public Cristales(JavaSparkContext sc){
		this.sc = sc;
	}
	/**
	 * Obtiene los valores para el CATALOGO DE CRISTALES
	 * @return
	 */
	public JavaPairRDD<String, String>  getCristales_Unicos(){
		JavaRDD<String> cristales = this. sc.textFile(CRISTALES);
		JavaPairRDD<String, String> cat_cristales = cristales.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > cat_cristales_1 = cat_cristales.groupByKey();
		JavaPairRDD<String, String> cat_cristales_U = cat_cristales_1.mapToPair(UNIQUE_FIRST);
		return cat_cristales_U;
	}
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			if(fields.length<14){
				valores +=WORDS_SEPARATOR_2+" ";
				fields = valores.split(WORDS_SEPARATOR_2);
			}
			tupla2 = new Tuple2<String, String>(fields[RECLAMA], fields.length+"");
			return tupla2;
		}
	};
	private static final PairFunction<Tuple2<String,Iterable<String>>,String,String> UNIQUE_FIRST = new PairFunction<Tuple2<String,Iterable<String>>,String,String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<String, Iterable<String>> valores) throws Exception {
			Iterable<String> posiblesValores = valores._2;
			String uniqueValue = posiblesValores.iterator().next();
			
			Tuple2<String, String> tupla2 = null;
			tupla2 = new Tuple2<String, String>( valores._1 , uniqueValue);
			return tupla2;
		}
	};

}
