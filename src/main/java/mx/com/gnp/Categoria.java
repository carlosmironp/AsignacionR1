package mx.com.gnp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Categoria implements GNPConstants {
	
	public static int RECLAMA 	= 0;
	public static int AFE		= 1;
	public static int POLIZA	= 2;
	public static int FTE_INFO	= 3;
	public static int MODULO	= 4;
	public static int TCTIPVEH	= 5;
	public static int ARMADORA	= 6;
	public static int MODELO2	= 7;
	public static int DSMARCA	= 8;
	public static int LNC_CVE	= 9;
	public static int INI_VIG	= 10;
	public static int FIN_VIG	= 11;
	public static int CARROCERIA= 12;
	public static int VERSION	= 13;
	public static int CATEGORIA	= 14;
	public static int CATEGORIA_R1= 15;
	public static int CATEGO_SAL= 16;
	public static int MODELO	= 17;
	public static int PERIODO	= 18;
	public static int FEC_OCU	= 19;
	public static int CUENTA	= 20;
	
	private JavaSparkContext sc;
	
	public Categoria(JavaSparkContext sc){
		this.sc = sc;
	}
	
	public JavaPairRDD<String,String> getCatalogoCategoriaUnico(){
		JavaRDD<String> categoria		= sc.textFile(CATALOGO_CATEGORIA);
		JavaPairRDD<String, String> categoriaO = categoria.mapToPair(INDEX);
		JavaPairRDD<String, Iterable<String> > categoriaU = categoriaO.groupByKey();
		JavaPairRDD<String, String> categoriaUnique = categoriaU.mapToPair(UNIQUE_FIRST);
		return categoriaUnique;
	}
	
	private static final PairFunction<String, String, String> INDEX = new PairFunction<String, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(String valores) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] fields = valores.split(WORDS_SEPARATOR_2);
			Long modelo = Long.parseLong(fields[MODELO]);
			Long anti = null;
			if(modelo<MODELO_DEFAULT) anti = ANTIGUEDAD_DEFAULT-modelo;
			
			tupla2 = new Tuple2<String, String>(fields[RECLAMA]+fields[FTE_INFO]+fields[AFE] , anti+WORDS_SEPARATOR+
																							   fields[TCTIPVEH]+WORDS_SEPARATOR+
																							   fields[ARMADORA]+WORDS_SEPARATOR+
																							   fields[CARROCERIA]);
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
