package mx.com.gnp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CatalogoR1 implements GNPConstants {
	
	public static final int TV 			= 0;
	public static final int ARMADORA 	= 1;
	public static final int CARROCERIA	= 2;
	public static final int CATEGORIA	= 3;
	
	private JavaSparkContext sc;
	
	public CatalogoR1(JavaSparkContext sc){
		this.sc = sc;
	}
	
	/**
	 * Obtiene los valores para el catalogo r1, indexado por tv, carroceria y armadora y regresa valores unicos del catalogo
	 * @return
	 */
	public JavaPairRDD<String, String>  getCalogo_R1_Unico(){
		JavaRDD<String> catalogo_r1=this. sc.textFile(CATALOGO_R1);
		JavaPairRDD<String, String> catalogoR1 = catalogo_r1.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > catalogoR1_1 = catalogoR1.groupByKey();
		JavaPairRDD<String, String> catalogoR1_unique = catalogoR1_1.mapToPair(UNIQUE_FIRST);
		return catalogoR1_unique;
	}
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			tupla2 = new Tuple2<String, String>(fields[TV]+fields[ARMADORA]+fields[CARROCERIA] , fields[CATEGORIA]);
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
