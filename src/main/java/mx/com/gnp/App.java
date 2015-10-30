package mx.com.gnp;


import mx.com.gnp.db.CatalogoR1;
import mx.com.gnp.db.Catalogos_R1;
import mx.com.gnp.db.Categoria;
import mx.com.gnp.db.Cristales;
import mx.com.gnp.db.IngresosNasa;
import mx.com.gnp.db.Salvamentos;
import mx.com.gnp.operations.ActualizaMeses;
import mx.com.gnp.operations.RvasexacOperations;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 
 * @author mfragoso
 *
 */
public class App {
	
	static final String PATH_RVASEXAC_CSV_HDFS					= "/reconstruccion/rvasexac_id";
	static final String PATH_SALDO_CSV_HDFS						= "/reconstruccion/base_r1_spark";
	static final String PATH_CASOS_CSV_HDFS						= "/reconstruccion/base_r1_casos_spark";
	static final String PATH_BASE_CAMBIOS_TOT_CSV_HDFS			= "/reconstruccion/base_cambios_tot_spark";
	
    public static void main( String[] args ){
    	JavaSparkContext sc = new JavaSparkContext();
    	
    	RvasexacOperations rvasexacOps = new RvasexacOperations(sc);
    	
    	JavaPairRDD<Long, String> rvasexac = rvasexacOps.getRvasexacOriginal();
    	rvasexac.cache();
    	
    	rvasexac.values().saveAsTextFile(PATH_RVASEXAC_CSV_HDFS);
    	
    	JavaPairRDD<Long, String> trabajo  = rvasexacOps.filtraMovimientosCiclo(rvasexac);
    	
    	//Indexamos ambas tablas por reclama,fte_info y poliza para hacer un join.
    	JavaPairRDD<Long, String> rvasexacSinIngresos = rvasexacOps.filtraIngresos(rvasexac);
    	JavaPairRDD<String, String> rvasexac1    = rvasexacOps.indexByPolizaFteInfoReclama(rvasexacSinIngresos);
    	JavaPairRDD<String, String> trabajo1   = rvasexacOps.indexByPolizaFteInfoReclama(trabajo);
    	JavaPairRDD<String, Iterable<String>> trabajo2   = trabajo1.groupByKey();
    	JavaPairRDD<String, Tuple2<Iterable<String>,String>> base_r1_1	   = trabajo2.join(rvasexac1);
    	JavaPairRDD<Long, String> base_r1  = rvasexacOps.createBase_R1(base_r1_1);
    	base_r1.cache();
    	trabajo = null;
    	rvasexacSinIngresos = null;
    	rvasexac1 = null;
    	trabajo1 = null;
    	trabajo2 = null;
    	base_r1_1 = null;
    	
    	//Asignacion de catalogR1
    	Categoria categoria = new Categoria(sc);
    	JavaPairRDD<String, String> categoria_cat = categoria.getCatalogoCategoriaUnico();
    	JavaPairRDD<String,String> base_r1_index_categoria = rvasexacOps.indexCategoria(base_r1);
    	JavaPairRDD<String, Tuple2<String,Optional<String>>> base_r1_categoria = base_r1_index_categoria.leftOuterJoin(categoria_cat);
    	JavaPairRDD<Long, String> base_r1_2 = rvasexacOps.replaceLeftJoin(base_r1_categoria);
    	base_r1=null;
    	categoria_cat=null;
    	base_r1_index_categoria=null;
    	base_r1_categoria=null;
    	
    	//Asignacion Catalogo R1
    	JavaPairRDD<String, String> base_r1_2_index_cat_r1 = rvasexacOps.indexCatalogoR1(base_r1_2);
    	CatalogoR1 catalogoR1 = new CatalogoR1(sc);
    	JavaPairRDD<String, String> catalogo_r1 = catalogoR1.getCalogo_R1_Unico();
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_3 = base_r1_2_index_cat_r1.leftOuterJoin(catalogo_r1);
    	JavaPairRDD<Long, String> base_r1_4 = rvasexacOps.replaceLeftJoinR1(base_r1_3);
    	base_r1_2=null;
    	catalogo_r1=null;
    	base_r1_3=null;
    	
    	//Indexamos base_r1 una vez que esta ordenado para que calculemos el ultimo movimiento y propaguemos la cve_per
    	JavaPairRDD<String, String> base_r1_5 = rvasexacOps.indexAndSort(base_r1_4);
    	JavaPairRDD<Long, String>   base_r1_6 = rvasexacOps.prepareBaseR1(base_r1_5);
    	JavaPairRDD<String, String> base_r1_7 = rvasexacOps.mainIndex(base_r1_6);
    	base_r1_4=null;
    	base_r1_5=null;
    	base_r1_6=null;
    	
    	//Historias
    	//Calculamos el ultimo movimiento y propagamos la clave de perdida cve_per
    	JavaPairRDD<String, Iterable<String>> base_r1_8 = base_r1_7.groupByKey();
    	JavaPairRDD<String, Iterable<String>> base_r1_9 = rvasexacOps.ultimoMovCvePer(base_r1_8);
    	base_r1_7 = null;
    	base_r1_8 = null;
    	JavaPairRDD<String,String> base_r1_10 = rvasexacOps.desagrupaValores(base_r1_9);
    	JavaPairRDD<Long,String> base_r1_11 = rvasexacOps.eliminateMainIndex(base_r1_10);
    	
    	base_r1_9 = null;
    	base_r1_10 = null;
    	
    	//Calculamos los ingresos nasa
    	IngresosNasa ingresosNasa = new IngresosNasa(sc);
    	JavaPairRDD<String, String> ingresos_nasa = ingresosNasa.getIngresosNasa_Unicos();
    	JavaPairRDD<String,String> base_r1_12 = rvasexacOps.reclamaIndex(base_r1_11);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_13_IN = base_r1_12.leftOuterJoin(ingresos_nasa);
    	JavaPairRDD<Long, String> base_r1_14 = rvasexacOps.replaceLeftJoinIN(base_r1_13_IN);
    	base_r1_11 = null;
    	ingresos_nasa = null;
    	base_r1_12=null;
    	base_r1_13_IN = null;
    	
    	//Marcamos Salvamentos
    	Salvamentos salvamentos = new Salvamentos(sc);
    	JavaPairRDD<String, String> salvamentos_cat = salvamentos.getSalvamentos_Unicos();
    	JavaPairRDD<String,String> base_r1_15 = rvasexacOps.reclamaAfeIndex(base_r1_14);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_15_Sal = base_r1_15.leftOuterJoin(salvamentos_cat);
    	JavaPairRDD<Long, String> base_r1_16 = rvasexacOps.replaceLeftJoinSal(base_r1_15_Sal);
    	base_r1_14 = null;
    	salvamentos_cat = null;
    	base_r1_15 = null;
    	base_r1_15_Sal = null;
    	
    	//Marcamos cristales
    	Cristales cristales = new Cristales(sc);
    	JavaPairRDD<String, String> cristales_cat = cristales.getCristales_Unicos();
    	JavaPairRDD<String,String> base_r1_17 = rvasexacOps.reclamaIndex(base_r1_16);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_17_cris = base_r1_17.leftOuterJoin(cristales_cat);
    	JavaPairRDD<Long, String> base_r1_18 = rvasexacOps.replaceLeftJoinCris(base_r1_17_cris);
    	cristales_cat = null;
    	base_r1_17 = null;
    	base_r1_17_cris = null;
    	
    	//Asignamos la R1 Nueva
    	//R1_201101
    	Catalogos_R1 cat_r1_R1_201101 = new Catalogos_R1(sc, GNPConstants.R1_201101);
    	JavaPairRDD<String, String> cat_R1_201101 = cat_r1_R1_201101.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_19 = rvasexacOps.indexCatR1(base_r1_18);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201101 = base_r1_19.leftOuterJoin(cat_R1_201101);
    	JavaPairRDD<Long, String> base_r1_20 = rvasexacOps.replaceLeftJoinCatR1201101(base_r1_201101);
    	base_r1_18 = null;
    	cat_r1_R1_201101 = null;
    	cat_R1_201101 = null;
    	base_r1_19 = null;
    	base_r1_201101 = null;
    	
    	//R1_201107
    	Catalogos_R1 cat_r1_R1_201107 = new Catalogos_R1(sc, GNPConstants.R1_201107);
    	JavaPairRDD<String, String>cat_201107 = cat_r1_R1_201107.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_21 = rvasexacOps.indexCatR1(base_r1_20);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201107 = base_r1_21.leftOuterJoin(cat_201107);
    	JavaPairRDD<Long, String> base_r1_22 = rvasexacOps.replaceLeftJoinCatR1201107(base_r1_201107);
    	//R1_201201
    	Catalogos_R1 cat_r1_R1_201201 = new Catalogos_R1(sc, GNPConstants.R1_201201);
    	JavaPairRDD<String, String>cat_201201 = cat_r1_R1_201201.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_23 = rvasexacOps.indexCatR1(base_r1_22);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201201 = base_r1_23.leftOuterJoin(cat_201201);
    	JavaPairRDD<Long, String> base_r1_24 = rvasexacOps.replaceLeftJoinCatR1201201(base_r1_201201);
    	//R1_201207
    	Catalogos_R1 cat_r1_R1_201207 = new Catalogos_R1(sc, GNPConstants.R1_201207);
    	JavaPairRDD<String, String>cat_201207 = cat_r1_R1_201207.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_25 = rvasexacOps.indexCatR1(base_r1_24);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201207 = base_r1_25.leftOuterJoin(cat_201207);
    	JavaPairRDD<Long, String> base_r1_26 = rvasexacOps.replaceLeftJoinCatR1201207(base_r1_201207);
    	//R1_201301
    	Catalogos_R1 cat_r1_R1_201301 = new Catalogos_R1(sc, GNPConstants.R1_201301);
    	JavaPairRDD<String, String>cat_201301 = cat_r1_R1_201301.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_27 = rvasexacOps.indexCatR1(base_r1_26);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201301 = base_r1_27.leftOuterJoin(cat_201301);
    	JavaPairRDD<Long, String> base_r1_28 = rvasexacOps.replaceLeftJoinCatR1201301(base_r1_201301);
    	//R1_201307
    	Catalogos_R1 cat_r1_R1_201307 = new Catalogos_R1(sc, GNPConstants.R1_201307);
    	JavaPairRDD<String, String>cat_201307 = cat_r1_R1_201307.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_29 = rvasexacOps.indexCatR1(base_r1_28);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201307 = base_r1_29.leftOuterJoin(cat_201307);
    	JavaPairRDD<Long, String> base_r1_30 = rvasexacOps.replaceLeftJoinCatR1201307(base_r1_201307);
    	//R1_201401
    	Catalogos_R1 cat_r1_R1_201401 = new Catalogos_R1(sc, GNPConstants.R1_201401);
    	JavaPairRDD<String, String>cat_201401 = cat_r1_R1_201401.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_31 = rvasexacOps.indexCatR1(base_r1_30);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201401 = base_r1_31.leftOuterJoin(cat_201401);
    	JavaPairRDD<Long, String> base_r1_32 = rvasexacOps.replaceLeftJoinCatR1201401(base_r1_201401);
    	//R1_201407
    	Catalogos_R1 cat_r1_R1_201407 = new Catalogos_R1(sc, GNPConstants.R1_201407);
    	JavaPairRDD<String, String>cat_201407 = cat_r1_R1_201407.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_33 = rvasexacOps.indexCatR1(base_r1_32);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201407 = base_r1_33.leftOuterJoin(cat_201407);
    	JavaPairRDD<Long, String> base_r1_34 = rvasexacOps.replaceLeftJoinCatR1201407(base_r1_201407);
    	//R1_201501
    	Catalogos_R1 cat_r1_R1_201501 = new Catalogos_R1(sc, GNPConstants.R1_201501);
    	JavaPairRDD<String, String>cat_201501 = cat_r1_R1_201501.getCatalog_R1_Unicos();
    	JavaPairRDD<String,String> base_r1_35 = rvasexacOps.indexCatR1(base_r1_34);
    	JavaPairRDD<String,Tuple2<String,Optional<String>>> base_r1_201501 = base_r1_35.leftOuterJoin(cat_201501);
    	JavaPairRDD<Long, String> base_r1_36 = rvasexacOps.replaceLeftJoinCatR1201501(base_r1_201501);
    	
    	//base_r1 
    	// No se olvide borrar el directorio
    	// despues que se importe el archivo darle permisos al folder 
    	// para aplicar el comando LOAD 
    	// Esto se debe describir en la documentacion
    	///*******QUITAR COMENTARIOO******
    	base_r1_36.values().saveAsTextFile(PATH_SALDO_CSV_HDFS);
    	
    	//Filtramos solo aquellas historias en las cuales su saldo se netea, tal como se hace en el procedure de saldo_original
    	JavaPairRDD<String, String> base_r1_37 = rvasexacOps.indexAndSort(base_r1_36);
    	JavaPairRDD<Long, String>   base_r1_38 = rvasexacOps.prepareBaseR1(base_r1_37);
    	JavaPairRDD<String, String> base_r1_39 = rvasexacOps.mainIndex(base_r1_38);
    	JavaPairRDD<String, Iterable<String>> base_r1_40 = base_r1_39.groupByKey();
    	JavaPairRDD<String, Iterable<String>> base_r1_41 = rvasexacOps.filtraSaldosCero(base_r1_40);
    	
    	JavaPairRDD<String, Iterable<String>> base_r1_42 = rvasexacOps.saldo1(base_r1_41);
    	JavaPairRDD<String,String> base_r1_43 = rvasexacOps.desagrupaValores(base_r1_42);
    	
    	//Ponemos la base final con los casos en el formato de struct depura
    	JavaPairRDD<String,String> base_r1_44 = rvasexacOps.strucDepura(base_r1_43);
    	
    	
    	//Primer punto de control para totalizar y revisar Casos de base_r1
    	base_r1_43.values().saveAsTextFile(PATH_CASOS_CSV_HDFS);
    	
    	//Actualiza meses
    	ActualizaMeses actualizaMeses = new ActualizaMeses(sc);
    	JavaPairRDD<String, String> base_cambios_tot_ant = actualizaMeses.getDM_1213().union(actualizaMeses.getGM_1213().union(actualizaMeses.getRC_1213().union(actualizaMeses.getRT_1213())));
    	
    	//Base anterior unida limpia
    	JavaPairRDD<String, String> base_r1_45 = base_cambios_tot_ant.subtractByKey(base_r1_44);
    	    	
    	//Unimos la base total anterior limpia con la reconstruida
    	JavaPairRDD<String, String> base_r1_46 = base_r1_45.union(base_r1_44);
    	
    	base_r1_46.values().saveAsTextFile(PATH_BASE_CAMBIOS_TOT_CSV_HDFS);
    	
    	System.out.println("base_r1_46 contiene "+base_r1_46.count()+" registros");
    	System.out.println(base_r1_46.take(10));
    	
    	sc.close();
    }
    
    
	
	
    
}
