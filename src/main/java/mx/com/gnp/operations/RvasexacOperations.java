package mx.com.gnp.operations;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import mx.com.gnp.GNPConstants;
import mx.com.gnp.util.GenericUtils;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class RvasexacOperations implements GNPConstants{
	
	
	//CAMPOS INICIALES
	public static final int ID			=0;
	public static final int RECLAMA		=1;
	public static final int POLIZA		=2;
	public static final int CTO_MTO		=3;
	public static final int CAU_CTO		=4;
	public static final int FEC_OCU		=5;
	public static final int FEC_MOV		=6;
	public static final int AFE			=7;
	public static final int RAMO		=8;
	public static final int IMP_MTO		=9;
	public static final int IDEREG		=10;
	public static final int COB			=11;
	public static final int TPOMOVEXA	=12;
	public static final int FECREPEXA	=13;
	public static final int NOAPE		=14;
	public static final int CVE_PER		=15;
	public static final int CSUBRAMO	=16;
	public static final int FTE_INFO	=17;
	public static final int FEC_FIN		=18;
	public static final int FEC_INI		=19;
	public static final int LITIGIO		=20;
	
	//CATEGORIA
	public static final int ANTI		=21;
	public static final int TCTIPVEH	=22;
	public static final int ARMADORA	=23;
	public static final int CARROCERIA	=24;
	public static final int MODELO_A	=25;
	
	//CATALOGO R1
	public static final int CATEGO_R1	=26;
	public static final int CTO_MTO2	=27;
	public static final int R1			=28;
	public static final int DIAS		=29;
	public static final int SALDO		=30;
	public static final int CVE_PER_UM	=31;
	
	//SALVAMENTOS E INGRESOS NASA
	public static final int SALV_N		=32;
	
	//Cristales
	public static final int CRIS		=33;
	
	//R1_NUEVA
	public static final int R1_NUEVA	=34;
	
	//SALDO1
	public static final int SALDO1		=35;
	public static final int IMP1		=36;
	public static final int CASO		=37;
	
	private JavaSparkContext sc;
	
	/**
	 * Constructor principal, el cual depende de un contexto de spark para que funcione
	 * @param sc
	 */
	public RvasexacOperations(JavaSparkContext sc){
		this.sc=sc;
	}
	
	/**
	 * Devuleve el rvasoriginal con un consecutivo unico para cada registro.
	 * @return
	 */
	public JavaPairRDD<Long, String> getRvasexacOriginal(){
		JavaRDD<String> rvasexacOriginal=this. sc.textFile(RVASEXAC);
		return rvasexacOriginal.zipWithUniqueId().mapToPair(CREATE_UNIQUE_ID); 
	}
	
	
	private static final PairFunction<Tuple2<String,Long>,Long, String> CREATE_UNIQUE_ID = new PairFunction<Tuple2<String,Long>, Long, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Long> valores) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			tupla2 = new Tuple2<Long, String>(valores._2, valores._2+WORDS_SEPARATOR+valores._1);
			return tupla2;
		}
	};
	
	/**
	 * Filtra los ingresos del primer conjunto de base_r1
	 * @param rvasexac
	 * @return
	 */
	public JavaPairRDD<Long, String> filtraIngresos(JavaPairRDD<Long,String> rvasexac){
		return rvasexac.filter(FILTER_INGRESOS);
		
	}
	private static final Function<Tuple2<Long, String>, Boolean> FILTER_INGRESOS = new Function<Tuple2<Long, String>, Boolean>() {
		private static final long serialVersionUID = 1L;
		public Boolean call(Tuple2<Long, String> keyValue) throws Exception {
			String[] record = keyValue._2.split(WORDS_SEPARATOR);
			String cto_mto = record[CTO_MTO];
			return ( !cto_mto.equals(INGRESOS) && !cto_mto.equals(INGRESOS_SALVAMENTO) && !cto_mto.equals(INGRESOS_RESERVA_SALVAMENTO));
		}
	};
	
	/**
	 * Pasandole el rvasexac original, nos devuelve los movimientos filtrados por la FECHA_INICIO_CICLO - FECHA_FINAL_CICLO
	 * @param rvasexac
	 * @return
	 */
	public JavaPairRDD<Long, String> filtraMovimientosCiclo(JavaPairRDD<Long,String> rvasexac){
		return rvasexac.filter(FILTER_TRABAJO);
		
	}
	private static final Function<Tuple2<Long, String>, Boolean> FILTER_TRABAJO = new Function<Tuple2<Long, String>, Boolean>() {
		private static final long serialVersionUID = 1L;
		public Boolean call(Tuple2<Long, String> keyValue) throws Exception {
			String[] record = keyValue._2.split(WORDS_SEPARATOR);
			String fec_mov_string = record[FEC_MOV];
			Long fec_mov=0L;
			if(fec_mov_string!=null && fec_mov_string.length()>0)
				fec_mov = Long.parseLong(fec_mov_string);
			return (fec_mov > FECHA_INICIAL_CICLO && fec_mov<FECHA_FINAL_CICLO);
		}
	};
	
	/**
	 * Indexa un rdd por poliza, fte_info y reclama, sirve para rvasexac original y trabajo
	 * @param rdd
	 * @return
	 */
	public JavaPairRDD<String, String> indexByPolizaFteInfoReclama(JavaPairRDD<Long,String> rdd){
		return rdd.mapToPair(INDEX_POLIZA_FTEINFO_RECLAMA); 
	}
	private static final PairFunction<Tuple2<Long, String>, String, String> INDEX_POLIZA_FTEINFO_RECLAMA = new PairFunction<Tuple2<Long, String>, String, String>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String[] record = tuple._2.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( record[RECLAMA]+record[FTE_INFO]+record[POLIZA] , tuple._2);
			return tupla2;
		}
	};
	
	
	/**
	 * Indexa un rdd por poliza, fte_info y reclama, sirve para rvasexac original y trabajo
	 * @param rdd
	 * @return
	 */
	public JavaPairRDD<Long, String> createBase_R1(JavaPairRDD<String, Tuple2<Iterable<String>,String>> base_r1){
		return base_r1.mapToPair(CREATE_BASE_R1); 
	}
	private static final PairFunction<Tuple2<String,Tuple2<Iterable<String>,String>>, Long ,String> CREATE_BASE_R1 = 
			new PairFunction<Tuple2<String,Tuple2<Iterable<String>,String>>, Long ,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2< String, Tuple2<Iterable<String>,String> > values) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			Tuple2< Iterable<String>,String > record = values._2;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			tupla2 = new Tuple2<Long, String>( id , value);
			return tupla2;
		}
	};
	
	/**
	 * Toma la base_r1 y la indexa segun los campos de reclama, fte_info y afe para hacer un join contra el catalogo de categoria
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> indexCategoria(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(INDEX_CATEGORIA);
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> INDEX_CATEGORIA = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[RECLAMA]+fields[FTE_INFO]+fields[AFE] , record._2);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de categoria
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoin(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_categoria){
		return base_r1_categoria.mapToPair(REPLACEABSENT);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			String modelo_a="";
			
			Tuple2<String,Optional<String>> base_r1_categoria = record._2;
			String base_r1_values = base_r1_categoria._1;
			String[]fields = base_r1_values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			
			Optional<String> categoria = base_r1_categoria._2;
			String recordReturn=record._1;
			if(categoria.isPresent()){
				String categoriaValues = categoria.get();
				String categoriaFields[] = categoriaValues.split(WORDS_SEPARATOR);
				String antiS = categoriaFields[0];
				if(antiS!=null && !antiS.equals("null")){
					Long anti = Long.parseLong(categoriaFields[0]);
					if(anti>=0 && anti<=3 && categoriaFields[1].equals("AUT")){ 
						modelo_a=ANT03;
					}else{
						modelo_a=ANTRE;
					}
				}else{
					modelo_a=ANTRE;
				}
				recordReturn=categoriaValues;
			}else{
				recordReturn =",,,"; 
				modelo_a=ANTRE;
			}
			
			if(fields[COB].equals(RESPONSABILIDAD_CIVIL) && fields[AFE].equals(AFECTADO_PRINCIPAL)){
				modelo_a=ANTRE;
			}
			
			recordReturn += WORDS_SEPARATOR+modelo_a;
			tupla2 = new Tuple2<Long, String>( id , base_r1_values+WORDS_SEPARATOR+recordReturn);
			return tupla2;
		}
	};

	/**
	 * Toma la base_r1_2 y la indexa segun los campos de reclama, fte_info y afe para hacer un join contra el catalogo de categoria
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> indexCatalogoR1(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(INDEX_CATALOGO_R1);
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> INDEX_CATALOGO_R1 = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[TCTIPVEH]+fields[ARMADORA]+fields[CARROCERIA] , record._2);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinR1(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_R1);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_R1 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> base_r1_cat_r1 = record._2;
			String base_r1_values = base_r1_cat_r1._1;
			String[]fields = base_r1_values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			
			Optional<String> cat_r1 = base_r1_cat_r1._2;
			String recordReturn=record._1;
			
			
			if(cat_r1.isPresent()){
				String catR1Value = cat_r1.get();
				recordReturn=catR1Value;
			}else{
				recordReturn = ""; 
			}
			
			if( (!fields[AFE].equals(AFECTADO_PRINCIPAL) || fields[COB].equals(RESPONSABILIDAD_CIVIL)) || 
				(!cat_r1.isPresent() && fields[COB].equals(DANIOS_MATERIALES)) ){
				recordReturn = "Otros";
			}
			
			String cto_mto2 =fields[CTO_MTO];
			
			if(fields[CTO_MTO].equals(A_MAS) || fields[CTO_MTO].equals(RE)){cto_mto2=A_MAS;	}
			if(fields[CTO_MTO].equals(RESERVA_INICIAL) ){cto_mto2=RESERVA_INICIAL;}
			if(fields[CTO_MTO].equals(A_MENOS)){cto_mto2=A_MENOS;}
			if(fields[CTO_MTO].equals(CP) || fields[CTO_MTO].equals(PAGOS) || fields[CTO_MTO].equals(PF)){cto_mto2=PAGOS;}
			
			recordReturn += WORDS_SEPARATOR+cto_mto2;
			tupla2 = new Tuple2<Long, String>( id , base_r1_values+WORDS_SEPARATOR+recordReturn);
			return tupla2;
		}
	};
	
	/**
	 * Indexa base_r1 con el indice principal reclama,cob,afe,fte_info,fec_mov,idereg y lo regresamos ordenado
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> indexAndSort(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(MAIN_INDEX).sortByKey();
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> MAIN_INDEX = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[RECLAMA]+fields[COB]+fields[AFE]+fields[FTE_INFO]+fields[FEC_MOV]+fields[IDEREG] , record._2);
			return tupla2;
		}
	};
	
	/**
	 * Prepara base_r1 ordenado de tal forma que RI quede al inicio, ayudado por el Idereg y lo deja de la forma <Long,String>
	 * @param rdd
	 * @return
	 */
	public JavaPairRDD<Long, String> prepareBaseR1(JavaPairRDD<String, String> base_r1){
		return base_r1.mapToPair(PREPARE_BASE_R1); 
	}
	private static final PairFunction<Tuple2<String,String>, Long ,String> PREPARE_BASE_R1 = 
			new PairFunction<Tuple2<String,String>, Long ,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,String> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			String values = record._2;
			String fields[] = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			tupla2 = new Tuple2<Long, String>( id , values);
			return tupla2;
		}
	};
	
	/**
	 * Indexa base_r1 con el indice principal reclama,cob,afe,fte_info
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> mainIndex(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(MAIN_INDEX_2).sortByKey();
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> MAIN_INDEX_2 = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[RECLAMA]+fields[COB]+fields[AFE]+fields[FTE_INFO], record._2);
			return tupla2;
		}
	};
	
	/**
	 * Calcula el ultimo movimiento y propaga la clave de perdida en cada historia de movimientos agrupados
	 * @param historias
	 * @return
	 */
	public JavaPairRDD<String, Iterable<String>> ultimoMovCvePer(JavaPairRDD<String, Iterable<String>> historias){
		return historias.mapToPair(ULTIMO_MOV_CVE_PER);
		 	
	}
	private static final PairFunction<Tuple2<String,Iterable<String>>,String,Iterable<String>> ULTIMO_MOV_CVE_PER = 
												new PairFunction<Tuple2<String,Iterable<String>>,String,Iterable<String>>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, Iterable<String>> call(Tuple2<String,Iterable<String>> historia) throws Exception {
			Tuple2<String, Iterable<String>> historiaUM;
			String 				index 	 = historia._1;
			Iterable<String> movimientos = historia._2;
			
			@SuppressWarnings("unchecked")
			List<String> ml  = IteratorUtils.toList(movimientos.iterator());
			List<String> mln = new ArrayList<String>();
			String cve_per=" ";
			
			String m1 = ml.get(0);
			String fields1[] = m1.split(WORDS_SEPARATOR);
			Date fec_mov = GenericUtils.convertStringToDate(fields1[FEC_MOV]);
			
			//Variables calculo de saldo
			Double saldo = 0.0;
		    
			for(int i =0; i<ml.size();i++){
				String m = ml.get(i);
				String fields[] = m.split(WORDS_SEPARATOR);
				Date fec_mov2 = GenericUtils.convertStringToDate(fields[FEC_MOV]);
				Long dias = GenericUtils.getDifferenceDays(fec_mov, fec_mov2);
				Double imp_mto = Double.parseDouble(fields[IMP_MTO].trim()) ;
				saldo += imp_mto;
				if(i==ml.size()-1){
					m += WORDS_SEPARATOR +"U"+ WORDS_SEPARATOR+ dias+ WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo)  + WORDS_SEPARATOR;
					cve_per = fields[CVE_PER];
				}
				else{
					m += WORDS_SEPARATOR +" "+ WORDS_SEPARATOR+ dias+ WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo) + WORDS_SEPARATOR;
					
				}
				
				fec_mov = fec_mov2;
				mln.add(m);
			}
			List<String> mlnc = new ArrayList<String>();
			for(int i =0; i<mln.size();i++){
				String m = mln.get(i);
				m+=cve_per;
				mlnc.add(m);
			}
			
			
			historiaUM = new Tuple2<String, Iterable<String>>(index, mlnc);
			return historiaUM;
		}
	};
	
	/**
	 * Crea un flat map de los valores que teniamos agrupados, para regresar a la forma K,V de todos los valores
	 * @param historias
	 * @return
	 */
	public JavaPairRDD<String, String> desagrupaValores(JavaPairRDD<String, Iterable<String>> historias){
		return historias.flatMapValues(UNGROUP);
	}
	
	private static final Function<Iterable<String>,Iterable<String>> UNGROUP = new Function<Iterable<String>,Iterable<String>>() {
		private static final long serialVersionUID = 1L;
		public Iterable<String> call(Iterable<String> records) throws Exception {
			return records;
		}
	};
	
	
	public JavaPairRDD<Long, String> eliminateMainIndex(JavaPairRDD<String, String> base_r1){
		return base_r1.mapToPair(ELIMINATE_MAIN_INDEX); 
	}
	private static final PairFunction<Tuple2<String,String>, Long ,String> ELIMINATE_MAIN_INDEX = 
			new PairFunction<Tuple2<String,String>, Long ,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,String> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			String values = record._2;
			String[] fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			String value =  fields[ID]+WORDS_SEPARATOR+ 
							fields[RECLAMA]+WORDS_SEPARATOR+
							fields[POLIZA]+WORDS_SEPARATOR+
							fields[CTO_MTO]+WORDS_SEPARATOR+
							fields[CAU_CTO]+WORDS_SEPARATOR+
							fields[FEC_OCU]+WORDS_SEPARATOR+
							fields[FEC_MOV]+WORDS_SEPARATOR+
							fields[AFE]+WORDS_SEPARATOR+
							fields[RAMO]+WORDS_SEPARATOR+
							fields[IMP_MTO]+WORDS_SEPARATOR+
							fields[IDEREG]+WORDS_SEPARATOR+
							fields[COB]+WORDS_SEPARATOR+
							fields[TPOMOVEXA]+WORDS_SEPARATOR+
							fields[FECREPEXA]+WORDS_SEPARATOR+
							fields[NOAPE]+WORDS_SEPARATOR+
							fields[CVE_PER]+WORDS_SEPARATOR+
							fields[CSUBRAMO]+WORDS_SEPARATOR+
							fields[FTE_INFO]+WORDS_SEPARATOR+
							fields[FEC_FIN]+WORDS_SEPARATOR+
							fields[FEC_INI]+WORDS_SEPARATOR+
							fields[LITIGIO]+WORDS_SEPARATOR+ 
							fields[ANTI]+WORDS_SEPARATOR+
							fields[TCTIPVEH]+WORDS_SEPARATOR+
							fields[ARMADORA]+WORDS_SEPARATOR+
							fields[CARROCERIA]+WORDS_SEPARATOR+
							fields[MODELO_A]+WORDS_SEPARATOR+
							fields[CATEGO_R1]+WORDS_SEPARATOR+
							fields[CTO_MTO2]+WORDS_SEPARATOR+
							fields[R1]+WORDS_SEPARATOR+
							fields[DIAS]+WORDS_SEPARATOR+
							fields[SALDO]+WORDS_SEPARATOR+
							fields[CVE_PER_UM];
							
			tupla2 = new Tuple2<Long, String>( id, value);
			
			return tupla2;
		}
		
	};
	
	/**
	 * Indexa por reclamacion, util para hacer join contra catalogos que usan solo reclamacion
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> reclamaIndex(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(RECLAMA_INDEX);
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> RECLAMA_INDEX = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[RECLAMA], record._2);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinIN(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_IN);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_IN = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> ingresos_nasa = record._2;
			String ingresos_nasa_values = ingresos_nasa._1;
			String[]fields = ingresos_nasa_values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			
			Optional<String> in = ingresos_nasa._2;
			String recordReturn=NO;
			String cve_per = fields[CVE_PER];
			if(in.isPresent()){
				
				if(fields[COB].equals(DANIOS_MATERIALES) && fields[FTE_INFO].equals(NASA)){
					cve_per = "T";
					recordReturn=SI;
				}
			}else{
				recordReturn = NO; 
			}
			
			String value =  fields[ID]+WORDS_SEPARATOR+ 
					fields[RECLAMA]+WORDS_SEPARATOR+
					fields[POLIZA]+WORDS_SEPARATOR+
					fields[CTO_MTO]+WORDS_SEPARATOR+
					fields[CAU_CTO]+WORDS_SEPARATOR+
					fields[FEC_OCU]+WORDS_SEPARATOR+
					fields[FEC_MOV]+WORDS_SEPARATOR+
					fields[AFE]+WORDS_SEPARATOR+
					fields[RAMO]+WORDS_SEPARATOR+
					fields[IMP_MTO]+WORDS_SEPARATOR+
					fields[IDEREG]+WORDS_SEPARATOR+
					fields[COB]+WORDS_SEPARATOR+
					fields[TPOMOVEXA]+WORDS_SEPARATOR+
					fields[FECREPEXA]+WORDS_SEPARATOR+
					fields[NOAPE]+WORDS_SEPARATOR+
					fields[CVE_PER]+WORDS_SEPARATOR+
					fields[CSUBRAMO]+WORDS_SEPARATOR+
					fields[FTE_INFO]+WORDS_SEPARATOR+
					fields[FEC_FIN]+WORDS_SEPARATOR+
					fields[FEC_INI]+WORDS_SEPARATOR+
					fields[LITIGIO]+WORDS_SEPARATOR+ 
					fields[ANTI]+WORDS_SEPARATOR+
					fields[TCTIPVEH]+WORDS_SEPARATOR+
					fields[ARMADORA]+WORDS_SEPARATOR+
					fields[CARROCERIA]+WORDS_SEPARATOR+
					fields[MODELO_A]+WORDS_SEPARATOR+
					fields[CATEGO_R1]+WORDS_SEPARATOR+
					fields[CTO_MTO2]+WORDS_SEPARATOR+
					fields[R1]+WORDS_SEPARATOR+
					fields[DIAS]+WORDS_SEPARATOR+
					fields[SALDO]+WORDS_SEPARATOR+
					cve_per;
			
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+recordReturn);
			return tupla2;
		}
	};
	
	/**
	 * Indexa por reclamacion, util para hacer join contra catalogos que usan solo reclamacion y afectado
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> reclamaAfeIndex(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(RECLAMA_AFE_INDEX);
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> RECLAMA_AFE_INDEX = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[RECLAMA]+fields[AFE], record._2);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinSal(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_SAL);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_SAL = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> ingresos_nasa = record._2;
			String ingresos_nasa_values = ingresos_nasa._1;
			String[]fields = ingresos_nasa_values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			
			Optional<String> in = ingresos_nasa._2;
			String salvamento = fields[SALV_N]; 
			String cve_per = fields[CVE_PER];
			if(in.isPresent()){
				if(fields[COB].equals(DANIOS_MATERIALES) && fields[FTE_INFO].equals(NASA)){
					cve_per = "T";
					salvamento=SI;
				}
			}else{
				salvamento=NO;
			}
			
			String value =  fields[ID]+WORDS_SEPARATOR+ 
					fields[RECLAMA]+WORDS_SEPARATOR+
					fields[POLIZA]+WORDS_SEPARATOR+
					fields[CTO_MTO]+WORDS_SEPARATOR+
					fields[CAU_CTO]+WORDS_SEPARATOR+
					fields[FEC_OCU]+WORDS_SEPARATOR+
					fields[FEC_MOV]+WORDS_SEPARATOR+
					fields[AFE]+WORDS_SEPARATOR+
					fields[RAMO]+WORDS_SEPARATOR+
					fields[IMP_MTO]+WORDS_SEPARATOR+
					fields[IDEREG]+WORDS_SEPARATOR+
					fields[COB]+WORDS_SEPARATOR+
					fields[TPOMOVEXA]+WORDS_SEPARATOR+
					fields[FECREPEXA]+WORDS_SEPARATOR+
					fields[NOAPE]+WORDS_SEPARATOR+
					fields[CVE_PER]+WORDS_SEPARATOR+
					fields[CSUBRAMO]+WORDS_SEPARATOR+
					fields[FTE_INFO]+WORDS_SEPARATOR+
					fields[FEC_FIN]+WORDS_SEPARATOR+
					fields[FEC_INI]+WORDS_SEPARATOR+
					fields[LITIGIO]+WORDS_SEPARATOR+ 
					fields[ANTI]+WORDS_SEPARATOR+
					fields[TCTIPVEH]+WORDS_SEPARATOR+
					fields[ARMADORA]+WORDS_SEPARATOR+
					fields[CARROCERIA]+WORDS_SEPARATOR+
					fields[MODELO_A]+WORDS_SEPARATOR+
					fields[CATEGO_R1]+WORDS_SEPARATOR+
					fields[CTO_MTO2]+WORDS_SEPARATOR+
					fields[R1]+WORDS_SEPARATOR+
					fields[DIAS]+WORDS_SEPARATOR+
					fields[SALDO]+WORDS_SEPARATOR+
					cve_per;
			
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+salvamento);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCris(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CRIS);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CRIS = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cris = record._2;
			String values = cris._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			String imp_mto = fields[IMP_MTO];
			Optional<String> in = cris._2;
			String cris_ = NO;
			if(in.isPresent()){
				cris_ = SI;
			}
			if(fields[CTO_MTO].equals(RESERVA_INICIAL) && fields[COB].equals(DANIOS_MATERIALES) && imp_mto.equals(RI_DEFAULT_CRISTAL)) {
				cris_= SI;
			}
			
			tupla2 = new Tuple2<Long, String>( id , values+WORDS_SEPARATOR+cris_);
			return tupla2;
		}
	};
	
	/**
	 * Indexa por catego_r1,cob y modelo_a util para hacer join contra catalogos r1 y asignar su respectivo valor 
	 * @param base_r1
	 * @return
	 */
	public JavaPairRDD<String, String> indexCatR1(JavaPairRDD<Long,String> base_r1){
		return base_r1.mapToPair(INDEX_CAT_R1);
	}
	private static final PairFunction<Tuple2<Long,String>, String, String> INDEX_CAT_R1 = new PairFunction<Tuple2<Long,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<Long,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			tupla2 = new Tuple2<String, String>( fields[CATEGO_R1]+fields[COB]+fields[MODELO_A], record._2);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201101(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201101);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201101 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva="0.0";
			Optional<String> valor_r1 = cat_r1._2;
			
			if(cto_mto.equals(RESERVA_INICIAL) && fec_mov < 20110701 ){
				
				if(valor_r1.isPresent()){
					r1_nueva = valor_r1.get();
				}else{
					r1_nueva = imp_mto;
				}
				
				if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
					r1_nueva = COSTO_DEFAULT_CRISTAL;
				}
				
				if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
					r1_nueva = imp_mto;
				}
				if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
					r1_nueva = imp_mto;
				}
			}
			
			
			tupla2 = new Tuple2<Long, String>( id , values+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201107(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201107);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201107 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20110701 && fec_mov< 20120101 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
			}
			
			String value =  getRecord(fields);
			
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201201(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201201);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201201 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20120101 && fec_mov< 20120701 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201207(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201207);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201207 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20120701 && fec_mov< 20130101 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
				
				
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201301(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201301);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201301 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20130101 && fec_mov< 20130701 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
				
				
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201307(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201307);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201307 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20130701 && fec_mov< 20140101 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201401(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201401);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201401 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20140101 && fec_mov< 20140701 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
				
				
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201407(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201407);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201407 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && fec_mov >= 20140701 && fec_mov< 20150101 ){
					
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
				
				
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	/**
	 * Toma el resultado de un left join y complementa la abtraccion para determinar valores nulos y 
	 * completar campos en base_r1 con el join de catalogo_r1
	 * @param base_r1_categoria
	 * @return
	 */
	public JavaPairRDD<Long, String> replaceLeftJoinCatR1201501(JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_cat_r1){
		return base_r1_cat_r1.mapToPair(REPLACEABSENT_CAT_R1_201501);
	}
	private static final PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, Long, String> REPLACEABSENT_CAT_R1_201501 = 
												new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>,Long,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<Long, String> call(Tuple2<String,Tuple2<String,Optional<String>>> record) throws Exception {
			Tuple2<Long, String> tupla2 = null;
			
			Tuple2<String,Optional<String>> cat_r1 = record._2;
			String values = cat_r1._1;
			String[]fields = values.split(WORDS_SEPARATOR);
			Long id = Long.parseLong(fields[ID]);
			Long fec_mov = Long.parseLong(fields[FEC_MOV]);
			String cto_mto = fields[CTO_MTO];
			String cob = fields[COB];
			String cris = fields[CRIS];
			String cve_per = fields[CVE_PER_UM];
			String imp_mto = fields[IMP_MTO];
			String r1_nueva=fields[R1_NUEVA];
			String salv_n = fields[SALV_N];
			Optional<String> valor_r1 = cat_r1._2;
			
			if(r1_nueva.equals("0.0")){
				if(cto_mto.equals(RESERVA_INICIAL) && cve_per.contains("P")  && fec_mov >= 20150101 ){
					if(valor_r1.isPresent()){
						r1_nueva = valor_r1.get();
					}else{
						r1_nueva = imp_mto;
					}
					if(cris.equals(SI) && cto_mto.equals(RESERVA_INICIAL) && cob.equals(DANIOS_MATERIALES) && cve_per.contains("P")){
						r1_nueva = COSTO_DEFAULT_CRISTAL;
					}
					
					if( (!cob.equals(DANIOS_MATERIALES) && !cob.equals(RESPONSABILIDAD_CIVIL)) && cto_mto.equals(RESERVA_INICIAL)){
						r1_nueva = imp_mto;
					}
					if( !valor_r1.isPresent() && cto_mto.equals(RESERVA_INICIAL) && (cob.equals(DANIOS_MATERIALES) || cob.equals(RESPONSABILIDAD_CIVIL)) ){
						r1_nueva = imp_mto;
					}
				}
				if(cto_mto.equals(RESERVA_INICIAL) && !cve_per.contains("P")  && fec_mov >= 20150101 ){
					r1_nueva = imp_mto;
				}
				//Cuando hay un salvamento y se cumplen estas condiciones se cambia la cve_per=T, aqui lo asumimos implicitamente
				if(salv_n.equals(SI) && fields[COB].equals(DANIOS_MATERIALES) && fields[FTE_INFO].equals(NASA)){
					r1_nueva = imp_mto;
				}
			}
			String value =  getRecord(fields);
			tupla2 = new Tuple2<Long, String>( id , value+WORDS_SEPARATOR+r1_nueva);
			return tupla2;
		}
	};
	
	private static String getRecord(String[] fields){
		return fields[ID]+WORDS_SEPARATOR+ 
				fields[RECLAMA]+WORDS_SEPARATOR+
				fields[POLIZA]+WORDS_SEPARATOR+
				fields[CTO_MTO]+WORDS_SEPARATOR+
				fields[CAU_CTO]+WORDS_SEPARATOR+
				fields[FEC_OCU]+WORDS_SEPARATOR+
				fields[FEC_MOV]+WORDS_SEPARATOR+
				fields[AFE]+WORDS_SEPARATOR+
				fields[RAMO]+WORDS_SEPARATOR+
				fields[IMP_MTO]+WORDS_SEPARATOR+
				fields[IDEREG]+WORDS_SEPARATOR+
				fields[COB]+WORDS_SEPARATOR+
				fields[TPOMOVEXA]+WORDS_SEPARATOR+
				fields[FECREPEXA]+WORDS_SEPARATOR+
				fields[NOAPE]+WORDS_SEPARATOR+
				fields[CVE_PER]+WORDS_SEPARATOR+
				fields[CSUBRAMO]+WORDS_SEPARATOR+
				fields[FTE_INFO]+WORDS_SEPARATOR+
				fields[FEC_FIN]+WORDS_SEPARATOR+
				fields[FEC_INI]+WORDS_SEPARATOR+
				fields[LITIGIO]+WORDS_SEPARATOR+ 
				fields[ANTI]+WORDS_SEPARATOR+
				fields[TCTIPVEH]+WORDS_SEPARATOR+
				fields[ARMADORA]+WORDS_SEPARATOR+
				fields[CARROCERIA]+WORDS_SEPARATOR+
				fields[MODELO_A]+WORDS_SEPARATOR+
				fields[CATEGO_R1]+WORDS_SEPARATOR+
				fields[CTO_MTO2]+WORDS_SEPARATOR+
				fields[R1]+WORDS_SEPARATOR+
				fields[DIAS]+WORDS_SEPARATOR+
				fields[SALDO]+WORDS_SEPARATOR+
				fields[CVE_PER_UM]+WORDS_SEPARATOR+
				fields[SALV_N]+WORDS_SEPARATOR+
				fields[CRIS];
	}
	
	
	
	///////////////////////////
	/////filtro saldo original/
	///////////////////////////
	/**
	 * Filtra los ingresos del primer conjunto de base_r1
	 * @param rvasexac
	 * @return
	 */
	public JavaPairRDD<String, Iterable<String>> filtraSaldosCero(JavaPairRDD<String, Iterable<String>> base_r1){
		return base_r1.filter(FILTER_SALDOS_CERO);
		
	}
	private static final Function<Tuple2<String,Iterable<String>>,Boolean> FILTER_SALDOS_CERO = new Function<Tuple2<String,Iterable<String>>,Boolean>() {
		private static final long serialVersionUID = 1L;
		public Boolean call(Tuple2<String,Iterable<String>> historia) throws Exception {
			Iterable<String> movimientos = historia._2;
			@SuppressWarnings("unchecked")
			List<String> ml  = IteratorUtils.toList(movimientos.iterator());
			
			//Variables calculo de saldo
			Double saldo = 0.0;
		    
			for(int i =0; i<ml.size();i++){
				String m = ml.get(i);
				String fields[] = m.split(WORDS_SEPARATOR);
				Double imp_mto = Double.parseDouble(fields[IMP_MTO].trim()) ;
				saldo += imp_mto;
			}
			
			return ( saldo>=-1 );
		}
	};
	
	/**
	 * Calcula el ultimo movimiento y propaga la clave de perdida en cada historia de movimientos agrupados
	 * @param historias
	 * @return
	 */
	public JavaPairRDD<String, Iterable<String>> saldo1(JavaPairRDD<String, Iterable<String>> historias){
		return historias.mapToPair(SALDO_1);
		 	
	}
	private static final PairFunction<Tuple2<String,Iterable<String>>,String,Iterable<String>> SALDO_1 = 
												new PairFunction<Tuple2<String,Iterable<String>>,String,Iterable<String>>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, Iterable<String>> call(Tuple2<String,Iterable<String>> historia) throws Exception {
			Tuple2<String, Iterable<String>> historiaUM;
			String 				index 	 = historia._1;
			Iterable<String> movimientos = historia._2;
			
			@SuppressWarnings("unchecked")
			List<String> ml  = IteratorUtils.toList(movimientos.iterator());
			List<String> mln = new ArrayList<String>();
			
			
			//Variables calculo de saldo1
			Double saldo1 	= 0.0;
			Double imp1   	= 0.0;
		    Double sini 	= 0.0;
		    Long dif_dias_wk = 50L;
			for(int i =0; i<ml.size();i++){
				
				String sinteticMov = null;
				
				String m = ml.get(i);
				String fields[] = m.split(WORDS_SEPARATOR);
				String caso=" ";
				
				String cto_mto2 = fields[CTO_MTO2];
				Double saldo    = Double.parseDouble(fields[SALDO]);
				Double r1_nueva = Double.parseDouble(fields[R1_NUEVA]);
				Double imp_mto = Double.parseDouble(fields[IMP_MTO]);
				Long dias = Long.parseLong(fields[DIAS]);
				String cob = fields[COB];
				
				if(cob.equals(DANIOS_MATERIALES)){
					dif_dias_wk = DIF_DIAS_WK_DM;
				}
				if(cob.equals(RESPONSABILIDAD_CIVIL)){
					dif_dias_wk = DIF_DIAS_WK_RC;
				}
				if(cob.equals(ROBO_TOTAL)){
					dif_dias_wk = DIF_DIAS_WK_RT;
				}
				if(cob.equals(GASTOS_MEDICOS)){
					dif_dias_wk = DIF_DIAS_WK_GM;
				}
				
				
				
				if(cto_mto2.equals(RESERVA_INICIAL)){
					caso = A;
					sini = 0.0;
					sini = saldo1 = imp1 = r1_nueva;
				}else if(cto_mto2.equals(A_MAS) && saldo<=sini){
					caso = B;
					imp1 = 0.0;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MAS) && saldo>sini){
					caso = C;
					imp1 = saldo-sini;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && saldo==0 && dias<dif_dias_wk && fields[R1].equals(ULTIMO_MOVIMIENTO)){
					caso = D;
					imp1 = sini * -1;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && saldo==0 && dias<dif_dias_wk && !fields[R1].equals(ULTIMO_MOVIMIENTO)){
					caso = E;
					imp1 = 0.0;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && saldo==0 && dias>=RANGO){
					caso = F;
					imp1 = sini * -1;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && saldo==0 && dias>=dif_dias_wk && dias < RANGO){
					caso = G;
					//Es el original y hay que revisar por que estando asi no cuadra
					//imp1 = 0.0 ;
					imp1 = sini*(-1);
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && saldo != sini && saldo != 0){
					caso = H;
					imp1 = saldo - sini ;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(A_MENOS) && dias<RANGO && saldo != 0){
					caso = I;
					imp1 = saldo - sini ;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(PAGOS) && sini == (imp_mto*-1) ){
					caso = J;
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(PAGOS) && sini > (imp_mto*-1) && ( fields[R1].equals(ULTIMO_MOVIMIENTO) || saldo!=0 ) ){
					caso = K;
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(PAGOS) && sini > (imp_mto*-1) && fields[R1].equals(ULTIMO_MOVIMIENTO) && saldo==0  ){
					caso = L;
					
					//Toma el movimiento anterior si es que existe
					String ms = ml.get(i);
					if(i>0){
						ms = ml.get(i-1);
					}
					
					String fields_s[] = ms.split(WORDS_SEPARATOR);
					Double saldo2 = Double.parseDouble(fields_s[SALDO]);
					sinteticMov = mapCasoSintetico(fields_s, A_MAS);
					
					sinteticMov += WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo2)  + WORDS_SEPARATOR +
							 String.format(Locale.ENGLISH, "%.2f", saldo2-saldo1) 	+ WORDS_SEPARATOR +
							 LS;
					
					
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}else 
				if(cto_mto2.equals(PAGOS) && sini < (imp_mto*-1) &&   saldo>=0  ){
					caso = L2;
					//Toma el movimiento anterior si es que existe
					String ms = ml.get(i);
					if(i>0){
						ms = ml.get(i-1);
					}
					String fields_s[] = ms.split(WORDS_SEPARATOR);
					Double saldo2 = Double.parseDouble(fields_s[SALDO]);
					
					sinteticMov = mapCasoSintetico(fields_s, A_MAS);
					
					sinteticMov += WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo2)  + WORDS_SEPARATOR +
							 String.format(Locale.ENGLISH, "%.2f", saldo2-saldo1) 	+ WORDS_SEPARATOR +
							 LS;
					
					
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}
				if(cto_mto2.equals(PAGOS) && sini < (imp_mto*-1) && saldo<0  ){
					caso = M;
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}else if(cto_mto2.equals(PAGOS) && sini > (imp_mto*-1) && fields[R1].equals(ULTIMO_MOVIMIENTO) && Math.abs(saldo)<1 ){
					caso = N;
					
					//Toma el movimiento anterior si es que existe
					String ms = ml.get(i);
					if(i>0){
						ms = ml.get(i-1);
					}
					String fields_s[] = ms.split(WORDS_SEPARATOR);
					Double saldo2 = Double.parseDouble(fields_s[SALDO]);
					sinteticMov = mapCasoSintetico(fields_s, A_MAS);
					
					sinteticMov += WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo2)  + WORDS_SEPARATOR +
							 String.format(Locale.ENGLISH, "%.2f", saldo2-saldo1) 	+ WORDS_SEPARATOR +
							 NS;
					
					imp1 = imp_mto ;
					sini += imp1;
					saldo1 = sini;
				}
				
				m += WORDS_SEPARATOR + String.format(Locale.ENGLISH, "%.2f", saldo1)  + WORDS_SEPARATOR +
					 String.format(Locale.ENGLISH, "%.2f", imp1) 	+ WORDS_SEPARATOR +
					 caso;
				if(sinteticMov != null) mln.add(sinteticMov);
				mln.add(m);
			}
			
			historiaUM = new Tuple2<String, Iterable<String>>(index, mln);
			return historiaUM;
		}
	};
	
	/**
	 * Pone en orden de struc depura cada unos d elos registros reconstruidos.
	 * @param movimientos
	 * @return
	 */
	public JavaPairRDD<String, String> strucDepura(JavaPairRDD<String, String> movimientos){
		return movimientos.mapToPair(STRUC_DEPURA);
		 	
	}
	private static final PairFunction<Tuple2<String,String>, String, String> STRUC_DEPURA = new PairFunction<Tuple2<String,String>,String,String>(){
		private static final long serialVersionUID = 1L;
		public Tuple2<String, String> call(Tuple2<String,String> record) throws Exception {
			Tuple2<String, String> tupla2 = null;
			String value = record._2;
			String[] fields = value.split(WORDS_SEPARATOR);
			
			String strucDepura = fields[CATEGO_R1]+WORDS_SEPARATOR+
								 fields[IDEREG]+WORDS_SEPARATOR+
								 fields[RECLAMA]+WORDS_SEPARATOR+
								 fields[CVE_PER]+WORDS_SEPARATOR+
								 fields[CTO_MTO]+WORDS_SEPARATOR+
								 fields[CTO_MTO2]+WORDS_SEPARATOR+
								 fields[FEC_OCU]+WORDS_SEPARATOR+
								 fields[FEC_MOV]+WORDS_SEPARATOR+
								 fields[AFE]+WORDS_SEPARATOR+
								 fields[COB]+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 fields[DIAS]+WORDS_SEPARATOR+
								 fields[R1]+WORDS_SEPARATOR+
								 fields[IMP_MTO]+WORDS_SEPARATOR+
								 fields[SALDO]+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 fields[IMP1]+WORDS_SEPARATOR+
								 fields[SALDO1]+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 fields[R1_NUEVA]+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+
								 fields[ID]+WORDS_SEPARATOR+
								 fields[CASO]+WORDS_SEPARATOR+
								 " "+WORDS_SEPARATOR+    //Depura
								 " "+WORDS_SEPARATOR+   //Cuenta
								 " "+WORDS_SEPARATOR+   //REGIS_NVO_ANT
								 fields[CRIS]+WORDS_SEPARATOR+ //CRIS
								 " ";          //RECONS_SD
			
			
			tupla2 = new Tuple2<String, String>( fields[RECLAMA]+fields[COB]+fields[AFE]  , strucDepura);
			return tupla2;
		}
	};
	
	
	/**
	 * Filtra los movimientos sinteticos
	 * @param rvasexac
	 * @return
	 */
	public JavaPairRDD<String, String> filtraSinteticos(JavaPairRDD<String,String> rvasexac){
		return rvasexac.filter(FILTER_SINTETICOS);
		
	}
	private static final Function<Tuple2<String, String>, Boolean> FILTER_SINTETICOS = new Function<Tuple2<String, String>, Boolean>() {
		private static final long serialVersionUID = 1L;
		public Boolean call(Tuple2<String, String> keyValue) throws Exception {
			String[] record = keyValue._2.split(WORDS_SEPARATOR);
			String caso = record[CASO];
			return ( !caso.equals(LS));
		}
	};
	
	
	private static String mapCasoSintetico(String[] fields, String cto_mto){
		return fields[ID]+WORDS_SEPARATOR+ 
				fields[RECLAMA]+WORDS_SEPARATOR+
				fields[POLIZA]+WORDS_SEPARATOR+
				fields[CTO_MTO]+WORDS_SEPARATOR+
				fields[CAU_CTO]+WORDS_SEPARATOR+
				fields[FEC_OCU]+WORDS_SEPARATOR+
				fields[FEC_MOV]+WORDS_SEPARATOR+
				fields[AFE]+WORDS_SEPARATOR+
				fields[RAMO]+WORDS_SEPARATOR+
				fields[IMP_MTO]+WORDS_SEPARATOR+
				fields[IDEREG]+WORDS_SEPARATOR+
				fields[COB]+WORDS_SEPARATOR+
				fields[TPOMOVEXA]+WORDS_SEPARATOR+
				fields[FECREPEXA]+WORDS_SEPARATOR+
				fields[NOAPE]+WORDS_SEPARATOR+
				fields[CVE_PER]+WORDS_SEPARATOR+
				fields[CSUBRAMO]+WORDS_SEPARATOR+
				fields[FTE_INFO]+WORDS_SEPARATOR+
				fields[FEC_FIN]+WORDS_SEPARATOR+
				fields[FEC_INI]+WORDS_SEPARATOR+
				fields[LITIGIO]+WORDS_SEPARATOR+ 
				fields[ANTI]+WORDS_SEPARATOR+
				fields[TCTIPVEH]+WORDS_SEPARATOR+
				fields[ARMADORA]+WORDS_SEPARATOR+
				fields[CARROCERIA]+WORDS_SEPARATOR+
				fields[MODELO_A]+WORDS_SEPARATOR+
				fields[CATEGO_R1]+WORDS_SEPARATOR+
				cto_mto+WORDS_SEPARATOR+
				" "+WORDS_SEPARATOR+
				fields[DIAS]+WORDS_SEPARATOR+
				fields[SALDO]+WORDS_SEPARATOR+
				fields[CVE_PER_UM]+WORDS_SEPARATOR+
				fields[SALV_N]+WORDS_SEPARATOR+
				fields[CRIS]+WORDS_SEPARATOR+
				fields[R1_NUEVA];
		
	}
	
}