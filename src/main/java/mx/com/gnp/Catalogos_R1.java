package mx.com.gnp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Catalogos_R1 implements GNPConstants{
	
	public static final int CATEGO_R1 	= 0;
	public static final int COB       	= 1;
	public static final int MODELO_A  	= 3;
	
	public static final int VALOR 		= 2;
	
	
	private JavaSparkContext sc;
	private String catalogName;
	
	public Catalogos_R1(JavaSparkContext sc, String catalogName){
		this.catalogName = catalogName;
		this.sc = sc;
	}
	
	/**
	 * Obtiene los valores para el CATALOGO DE CRISTALES
	 * @return
	 */
	public JavaPairRDD<String, String>  getCatalog_R1_Unicos(){
		JavaRDD<String> catalog = this. sc.textFile(this.catalogName);
		JavaPairRDD<String, String> catalog_c = catalog.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > catalog_c_1 = catalog_c.groupByKey();
		JavaPairRDD<String, String> catalog_c_U = catalog_c_1.mapToPair(UNIQUE_FIRST);
		return catalog_c_U;
	}
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			tupla2 = new Tuple2<String, String>(fields[CATEGO_R1]+fields[COB]+fields[MODELO_A], fields[VALOR]);
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
