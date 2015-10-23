package mx.com.gnp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Salvamentos implements GNPConstants{
	
	public static final int SINIESTRO 	= 2;
	public static final int AFE			=3;
	
private JavaSparkContext sc;
	
	public Salvamentos(JavaSparkContext sc){
		this.sc = sc;
	}
	/**
	 * Obtiene los valores para el ingresos nasa, indexado por siniestro
	 * @return
	 */
	public JavaPairRDD<String, String>  getSalvamentos_Unicos(){
		JavaRDD<String> salvamentos = this. sc.textFile(SALVAMENTOS);
		JavaPairRDD<String, String> cat_salvamentos = salvamentos.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > cat_salvamentos_1 = cat_salvamentos.groupByKey();
		JavaPairRDD<String, String> cat_salvamentos_U = cat_salvamentos_1.mapToPair(UNIQUE_FIRST);
		return cat_salvamentos_U;
	}
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			tupla2 = new Tuple2<String, String>(fields[SINIESTRO]+fields[AFE], fields[SINIESTRO]);
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
