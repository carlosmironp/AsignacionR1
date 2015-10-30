use insumos;

drop table if exists insumos.rvasexac_spark;
drop table if exists insumos.base_r1_spark;
drop table if exists insumos.base_r1_casos_spark;


create table insumos.rvasexac_spark
(id      bigint,reclama string,poliza  string,
cto_mto string,cau_cto string,fec_ocu int,
fec_mov int,afe string,ramo  string,
imp_mto decimal(12,2),idereg  string,cob string,
tpomovexa string,fecrepexa int,noape decimal(1,0),
cve_per string,csubramo  decimal(1,0),fte_info  string,
fec_fin int,fec_ini int,litigio string)
row format delimited
fields terminated by ','
stored as textfile;

create table insumos.base_r1_spark (
ID              bigint, RECLAMA         string,
POLIZA          string, CTO_MTO         string,
CAU_CTO         string, FEC_OCU         int,
FEC_MOV         int, AFE             string,
RAMO            int, IMP_MTO         decimal(12,2),
IDEREG          int, COB             string,
TPOMOVEXA       string, FECREPEXA       string,
NOAPE           string, CVE_PER_O       string,
CSUBRAMO        string, FTE_INFO        string,
FEC_FIN         string, FEC_INI         string,
LITIGIO         string, ANTI_           int,
TCTIPVEH        string, ARMADORA        string,
CARROCERIA      string, MODELO_A        string,
CATEGO_R1       string, CTO_MTO2        string,
R1              string, DIAS            int,
SALDO           decimal(12,2), CVE_PER         string,
SALV_N          string, CRIS            string,
R1_NUEVA        decimal(12,2))
row format delimited
fields terminated by ','
stored as textfile;

create table insumos.base_r1_casos_spark
(id  bigint ,reclama string,poliza  string,
cto_mto string,cau_cto string,fec_ocu int,
fec_mov int,afe string,ramo  int,
imp_mto decimal(12,2),idereg  int,cob string,
tpomovexa string,fecrepexa string,noape string,
cve_per_o string,csubramo  string,fte_info  string,
fec_fin string,fec_ini string,litigio string,
anti_ int,tctipveh  string,armadora  string,
carroceria  string,modelo_a  string,catego_r1 string,
cto_mto2  string,r1  string,dias  int,
saldo decimal(12,2),cve_per string,salv_n  string,
cris  string,r1_nueva  decimal(12,2),saldo1 decimal(12,2),
imp1 decimal(12,2),caso string )
row format delimited
fields terminated by ','
stored as textfile;

create table insumos.base_cambios_tot(
CATEGO_R1 	string,
IDEREG 		bigint,
RECLAMA 	string,
CVE_PER 	string,
CTO_MTO 	string,
CTO_MTO2 	string,
FEC_OCU 	int,
FEC_MOV 	int,
AFE 		string,
COB 		string,
CERO 		string,
DIAS 		int,
R1 			string,
IMP_MTO 	decimal(12,2),
SALDO 		decimal(12,2),
CTO1 		string,
IMP1 		decimal(12,2),
SALDO1 		decimal(12,2),
R1_B 		string,
FEC_MOV1 	int,
DIAS1 		int,
R1_NUEVA 	decimal(12,2),
FECMOV2 	int,
ES_R1 		string,
CONSECU 	string,
CASO 		string,
DEPURA 		string,
CUENTA 		string,
REGIS_NVO	string,
CRIS		string,
RECONS 		string
)
row format delimited
fields terminated by ','
stored as textfile;


LOAD DATA INPATH '/reconstruccion/rvasexac_id' OVERWRITE INTO TABLE insumos.rvasexac_spark;
LOAD DATA INPATH '/reconstruccion/base_r1_spark' OVERWRITE INTO TABLE insumos.base_r1_spark;
LOAD DATA INPATH '/reconstruccion/base_r1_casos_spark' OVERWRITE INTO TABLE insumos.base_r1_casos_spark;
LOAD DATA INPATH '/reconstruccion/base_cambios_tot_spark' OVERWRITE INTO TABLE insumos.base_cambios_tot;