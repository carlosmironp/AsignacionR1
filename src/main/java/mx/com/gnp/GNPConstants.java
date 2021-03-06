package mx.com.gnp;

public interface GNPConstants {
		
	static final String WORDS_SEPARATOR = ",";
	public static final String WORDS_SEPARATOR_2 = "\u0001";
	
	static final String ENERO 		= "01";
	static final String FEBRERO 	= "02";
	static final String MARZO 		= "03";
	static final String ABRIL 		= "04";
	static final String MAYO 		= "05";
	static final String JUNIO 		= "06";
	static final String JULIO 		= "07";
	static final String AGOSTO 		= "08";
	static final String SEPTIEMBRE 	= "09";
	static final String OCTUBRE 	= "10";
	static final String NOMBIEMBRE 	= "11";
	static final String DICIEMBRE 	= "12";
	
	static final String ANIO_C = "2015";
	static final String ANIO = "15";
	static final String MES  = MAYO;
	
	//BASES
	static final String RVASEXAC 			= "/insumos/bases/rvasexac"+MES+ANIO+"/rvasexac"+MES+ANIO+".csv";
	static final String INGRESOS_NASA 		= "/insumos/bases/ingresos_nasa_"+ANIO_C+"_sf_sm/ingresos_nasa_"+ANIO_C+"_sf_sm.csv";
	static final String SALVAMENTOS 		= "/insumos/bases/rsalv_"+MES+ANIO+"/rsalv_"+MES+ANIO+".csv";
	static final String CRISTALES 			= "/insumos/bases/cristales_2010_"+ANIO_C+"_"+MES+ANIO+"/cristales_2010_"+ANIO_C+"_"+MES+ANIO+".csv";
	
	//BASES ANTERIORES
	static final String DM_1213 				= "/insumos/bases/dm_1213/dm_1213.csv";
	static final String GM_1213 				= "/insumos/bases/GM_1213/GM_1213.csv";
	static final String RC_1213 				= "/insumos/bases/RC_1213/RC_1213.csv";
	static final String RT_1213 				= "/insumos/bases/RT_1213/RT_1213.csv";
	
	//CATALOGOS
	static final String CATALOGO_CATEGORIA 	= "/insumos/catalogos/cat_rvasexac_categoria_"+MES+ANIO+"/cat_rvasexac_categoria_"+MES+ANIO+".csv";
	static final String CATALOGO_R1 		= "/insumos/catalogos/catalogo_r1/catalogo_r1.csv";
	
	public static final String R1_201101 	= "/insumos/catalogos/catalogos_r1_201101/catalogos_r1_201101.csv";
	public static final String R1_201107 	= "/insumos/catalogos/catalogos_r1_201107/catalogos_r1_201107.csv";
	public static final String R1_201201 	= "/insumos/catalogos/catalogos_r1_201201/catalogos_r1_201201.csv";
	public static final String R1_201207 	= "/insumos/catalogos/catalogos_r1_201207/catalogos_r1_201207.csv";
	public static final String R1_201301 	= "/insumos/catalogos/catalogos_r1_201301/catalogos_r1_201301.csv";
	public static final String R1_201307 	= "/insumos/catalogos/catalogos_r1_201307/catalogos_r1_201307.csv";
	public static final String R1_201401 	= "/insumos/catalogos/catalogos_r1_201401/catalogos_r1_201401.csv";
	public static final String R1_201407 	= "/insumos/catalogos/catalogos_r1_201407/catalogos_r1_201407.csv";
	public static final String R1_201501 	= "/insumos/catalogos/Catalogos_r1_201501/Catalogos_r1_201501.csv";
	public static final String HURACANES	= "/insumos/catalogos/siniestros_huracanes_final_3103"+ANIO_C+"/siniestros_huracanes_final_3103"+ANIO_C+".csv";
	
	
	
	//Constantes para identificar los salvamentos
	static final String INGRESOS 					= "IN";
	static final String INGRESOS_SALVAMENTO 		= "IS";
	static final String INGRESOS_RESERVA_SALVAMENTO = "RS";
	
	static final Long MODELO_DEFAULT = 9999L;
	static final Long ANTIGUEDAD_DEFAULT = 2014L;
	static final String ANTRE = "ANTRE";
	static final String ANT03 = "ANT03";
	static final String AFECTADO_PRINCIPAL="01";
	
	static final String RESPONSABILIDAD_CIVIL 	= "RC";
	static final String DANIOS_MATERIALES 		= "DM";
	static final String ROBO_TOTAL 				= "RT";
	static final String GASTOS_MEDICOS 			= "GM";
	static final String NASA 					= "S";
	
	static final String SI = "S";
	static final String NO = "N";
	
	//CICLO PRINCIPAL determina el rango de fechas para obtener solo las historias de movimientos que se quieren procesar
	static final Long FECHA_INICIAL_CICLO =20140101L;
	static final Long FECHA_FINAL_CICLO   =20150701L;
	
	//Diferentes tipos de movimiento CTO_MTO
	static final String RESERVA_INICIAL 	= "RI";
	static final String RE 					= "RE";
	static final String PAGOS 				= "PP";
	static final String A_MAS 				= "A+";
	static final String A_MENOS 			= "A-";
	static final String CP 					= "CP";
	static final String PF 					= "PF";
	static final String P 					= "P";  //Pagos resumidos
	
	static final String RI_DEFAULT_CRISTAL 		= "1759";
	static final String COSTO_DEFAULT_CRISTAL 	= "1860";
	
	//CASOS
	static final String A 					= "A";
	static final String B 					= "B";
	static final String C 					= "C";
	static final String D 					= "D";
	static final String E 					= "E";
	static final String F 					= "F";
	static final String G 					= "G";
	static final String H 					= "H";
	static final String I 					= "I";
	static final String J 					= "J";
	static final String K 					= "K";
	static final String L 					= "L";
	static final String LS 					= "LS";
	static final String L2 					= "L2";
	static final String L2S 				= "L2S";
	static final String M 					= "M";
	static final String N 					= "N";
	static final String NS 					= "NS";
	
	static final Long DIF_DIAS_WK_DM 		= 60L;
	static final Long DIF_DIAS_WK_RC 		= 70L;
	static final Long DIF_DIAS_WK_RT 		= 70L;
	static final Long DIF_DIAS_WK_GM 		= 90L;
	
	static final Long RANGO 				= 120L;
	static final String ULTIMO_MOVIMIENTO 	= "U";
	
	
	//ORDEN DE CAMPOS DE STRUC_DEPURA.DBF
	static final int CATEGO_R1_SD 	= 0;
	static final int IDEREG_SD 		= 1;
	static final int RECLAMA_SD 	= 2;
	static final int CVE_PER_SD 	= 3;
	static final int CTO_MTO_SD 	= 4;
	static final int CTO_MTO2_SD 	= 5;
	static final int FEC_OCU_SD 	= 6;
	static final int FEC_MOV_SD 	= 7;
	static final int AFE_SD 		= 8;
	static final int COB_SD 		= 9;
	static final int CERO_SD 		= 10;
	static final int DIAS_SD 		= 11;
	static final int R1_SD 			= 12;
	static final int IMP_MTO_SD 	= 13;
	static final int SALDO_SD 		= 14;
	static final int CTO1_SD 		= 15;
	static final int IMP1_SD 		= 16;
	static final int SALDO1_SD 		= 17;
	static final int R1_B_SD 		= 18;
	static final int FEC_MOV1_SD 	= 19;
	static final int DIAS1_SD 		= 20;
	static final int R1_NUEVA_SD 	= 21;
	static final int FECMOV2_SD 	= 22;
	static final int ES_R1_SD 		= 23;
	static final int CONSECU_SD 	= 24;
	static final int CASO_SD 		= 25;
	static final int DEPURA_SD 		= 26;
	static final int CUENTA_SD 		= 27;
	public static int REGIS_NVO_SD	= 28;
	static final int CRIS_SD 		= 29;
	static final int RECONS_SD 		= 30;

}
