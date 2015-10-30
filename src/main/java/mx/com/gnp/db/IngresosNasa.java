package mx.com.gnp.db;

import mx.com.gnp.GNPConstants;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class IngresosNasa implements GNPConstants {
	
	public static final int RECLAMA = 1;
	
	private JavaSparkContext sc;
	
	public IngresosNasa(JavaSparkContext sc){
		this.sc = sc;
	}
	
	/**
	 * Obtiene los valores para el ingresos nasa, indexado por siniestro
	 * @return
	 */
	public JavaPairRDD<String, String>  getIngresosNasa_Unicos(){
		JavaRDD<String> ingresos_nasa = this. sc.textFile(INGRESOS_NASA);
		JavaPairRDD<String, String> cat_ingresos_nasa = ingresos_nasa.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > cat_ingresos_nasa_1 = cat_ingresos_nasa.groupByKey();
		JavaPairRDD<String, String> cat_ingresos_nasa_unique = cat_ingresos_nasa_1.mapToPair(UNIQUE_FIRST);
		return cat_ingresos_nasa_unique;
	}
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			tupla2 = new Tuple2<String, String>(fields[RECLAMA], fields[RECLAMA]);
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
