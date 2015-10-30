use insumos;


drop table if exists insumos.base_cambios_tot_ant_ca;
drop table if exists insumos.base_r1_ca_Recons_Ante;
drop table if exists insumos.base_cambios_tot;
drop table if exists insumos.rvas_limpio_esta_c;
drop table if exists insumos.rvasexac_sin_ingresos;
drop table if exists insumos.base_cambios_tot_rec_campos;
drop table if exists insumos.base_cambios_tot_camb_proceden;
drop table if exists insumos.rvas_limpio_marca_periodos;
drop table if exists insumos.rvas_limpio_para_borrar;
drop table if exists insumos.rvasexac_c30_ca;
drop table if exists insumos.rvasexac_c30_ca_tot;
drop table if exists insumos.base_cambios_tot_ant_ca_l;
drop table if exists insumos.siniestros_huracanes_final_31032015_l;
drop table if exists insumos.rvasexac_sin_cat_antes_mov20141025_ca;
drop table if exists insumos.ri_25102014_ca;
drop table if exists insumos.ri_20141025_todos_mov_ca;
drop table if exists insumos.ri_20141025_mov_ant_ca;
drop table if exists insumos.ri_20141025_mov_ant_ca;
drop table if exists insumos.tot_mov_nac_20141025_ca;
drop table if exists insumos.rvasexac_sin_cat_reconstruccion_final_ca;


 -- De la base anterior sacamos los campos que solo corresponden para que podamos hacer el union
create table insumos.base_cambios_tot_ant_ca as 
select * from insumos.dm_1213
union 
select *,'' as cris from insumos.gm_1213
union 
select *,'' as cris from insumos.rc_1213
union 
select *,'' as cris from insumos.rt_1213;
compute stats insumos.base_cambios_tot_ant_ca;


-- Calculamos cuantas llaves de la reconstruccion actual estan en la base_anterior_unida 
create table insumos.base_r1_ca_Recons_Ante as 
select base_r1_spark.reclama,base_r1_spark.cob,base_r1_spark.afe, count(*) from base_cambios_tot_ant_ca bases_ant
join  insumos.base_r1_casos_spark base_r1_spark on 
base_r1_spark.reclama=bases_ant.reclama and base_r1_spark.cob=bases_ant.cob and base_r1_spark.afe=bases_ant.afe
group by base_r1_spark.reclama,base_r1_spark.cob,base_r1_spark.afe;

-- Base anterior sin los movimientos que estamos reconstruyendo
create table insumos.base_cambios_tot_ant_ca_l as
select ant.* from base_cambios_tot_ant_ca ant
left join insumos.base_r1_ca_Recons_Ante ra on ra.cob=ant.cob and ra.reclama=ant.reclama and ra.afe=ant.afe
where ra.reclama is null;


--Hacemos un union entre la base_r1_ca_cob_ant_limpio y base_r10515_cloudera_saldo bases_unidas_tot
create table insumos.base_cambios_tot as
select 
0 as id, ant.catego_r1,cast(ant.idereg as bigint) as idereg,ant.reclama,ant.cve_per,ant.cto_mto,ant.cto_mto2,ant.fec_ocu,
ant.fec_mov,ant.afe,ant.cob,ant.cero,ant.dias,ant.r1,ant.imp_mto,ant.saldo,ant.cto1,ant.imp1,ant.saldo1,
ant.r1_b,ant.fec_mov1,ant.dias1,cast(ant.r1_nueva as decimal(12,2)) as r1_nueva,cast (ant.fecmov2 as bigint) as fec_mov2,
ant.es_r1,ant.consecu,ant.caso,ant.depura,ant.cuenta,ant.regis_nvo,' ' as fte_info
from insumos.base_cambios_tot_ant_ca_l ant
union
select 
cloud.id, cloud.catego_r1,cast(cloud.idereg as bigint) as idereg,cloud.reclama,cloud.cve_per,cloud.cto_mto,cloud.cto_mto2,cloud.fec_ocu,
cloud.fec_mov,cloud.afe,cloud.cob,'' as cero,cloud.dias,cloud.r1,cloud.imp_mto,cloud.saldo,'' as cto1,
cloud.imp1,cloud.saldo1,'' as r1_b,null as fec_mov1,null as dias1,cast(cloud.r1_nueva as decimal(12,2)),
cast(cloud.fec_mov as bigint) as fec_mov2,'' es_r1,'' as consecu,cloud.caso,'' as depura,null as cuenta,'' as regis_nvo, cloud.fte_info
from insumos.base_r1_casos_spark cloud;

-- Filtramos de revas limpio los ingresos y guardamos la tabla temporal en
create table insumos.rvasexac_sin_ingresos as
select * from rvasexac_accion where accion<>'IN' and accion<>'NC';

-- Recuperamos los campos calculados en limpia revas aqui pudimos hacer join por el idereg lo cual facilito la relacion uno a uno
create table insumos.base_cambios_tot_rec_campos as
select 
rt.catego_r1,rt.idereg, rt.reclama,rt.cve_per,
rt.cto_mto as cto_mto_o,
rt.fec_ocu,rt.fec_mov,rt.afe,rt.cob,rt.cero,rt.dias,rt.r1,
rt.imp_mto as imp_to_o,
rt.saldo, rt.cto1,
rt.saldo1,rt.r1_b,rt.fec_mov1,
rt.r1_nueva,rt.fec_mov2,rt.es_r1,rt.consecu,rt.caso,
rt.cuenta,rt.regis_nvo,rsi.cris,rsi.csubramo,rsi.fte_info,
rsi.poliza,rsi.cau_cto,rsi.ramo,rsi.noape,rsi.mov_pagos,
rsi.pag_dif_c,rsi.es_cero,rsi.litigio,
case 
  when rt.fec_mov>=20150501 then 'S'
  else ' '
end as mov_pos,
case
  when (rt.cto_mto='RI' and rt.fec_mov>=20150501) then 'A'
  else ' '
end as c12,
case
  when (rt.cto_mto='RI' and rt.fec_mov>=20130701) then 'B'
  else ' '
end as c24,
case
  when rt.cto_mto='RI' and rt.fec_mov>20120101 then 'C'
  else ' '
end as c30,
case 
   when (rt.cto_mto='RI' and rt.fec_mov>=20150501) then 'ABC'
   when (rt.cto_mto='RI' and rt.fec_mov>=20130701) then 'BC'
   when rt.cto_mto='RI' and rt.fec_mov>20120101 then 'C' 
end as llave,
rsi.menor_6,
rsi.accion,
'S' as es_rvas,
case 
  when rt.cto_mto='A-' then 'AjN'
  when rt.cto_mto='A+' then 'AjP'
  when rt.cto_mto='RS' then 'Slv'
  when rt.cto_mto='PP' then 'Pag'
  when rt.cto_mto='RI' then 'Ape'
  when rt.cto_mto='RE' then 'AjP'
  else ''
end as tpomovexa,
rt.imp1 as imp_mto,
rt.imp1,
rt.cto_mto2 as cto_mto
from base_cambios_tot rt
left join rvasexac_sin_ingresos rsi on
rsi.reclama=rt.reclama and rsi.cob=rt.cob and rsi.afe=rt.afe
and cast(rsi.idereg as bigint)=rt.idereg and rsi.fec_mov=rt.fec_mov and rsi.cto_mto=rt.cto_mto;

-- Sacamos a base cambios los movimientos que proceden y que se van a sustituir como lo marca el procedimiento cambios proceden
create table insumos.base_cambios_tot_camb_proceden as 
select *  from base_cambios_tot_rec_campos base_cambios_tot 
where base_cambios_tot.imp1<>0 and base_cambios_tot.mov_pos<>'S' and base_cambios_tot.es_rvas='S';


-- Marcacmos los periodos en el rvas limpio 
create table insumos.rvas_limpio_marca_periodos as
select rvas_limpio.*, 
case
  when (rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>=20150501)  then 'A'
  else ' '
end as c12,
case
  when (rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>=20130701) then 'B'
  else ' '
end as c24,
case
  when rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>20120101 then 'C'
  else ' '
end as c30,
case 
   when (rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>=20150501) then 'ABC'
   when (rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>=20130701) then 'BC'
   when rvas_limpio.cto_mto='RI' and rvas_limpio.fec_mov>20120101 then 'C' 
end as llave
from rvasexac_accion rvas_limpio;

-- calculamos el campo esta_c sobre reservas limpio
create table insumos.rvas_limpio_esta_c as
select rl.*,
case 
  when bc.reclama is not null and ( rl.cto_mto<>'IN' AND rl.cto_mto<>'IS' AND rl.cto_mto<>'RS' AND rl.accion<>'NC') then 'S'
  else ''
end esta_c
from rvas_limpio_marca_periodos rl
left join base_cambios_tot_camb_proceden bc on
bc.reclama=rl.reclama and bc.cob=rl.cob and bc.afe=rl.afe and bc.fec_mov=rl.fec_mov and bc.idereg=cast(rl.idereg as bigint);


create table insumos.rvas_limpio_para_borrar as
select reclama,cob, afe, count(*) as cuenta from rvas_limpio_esta_c
where (c30='C' and accion='SA') OR (c30='C' and accion='CA' and esta_c='S')
group by reclama,cob, afe;


-- Una vez calculado el campo esta_c filtramos para obtener rvasexac_c30_ca
create table insumos.rvasexac_c30_ca as 
select rl.* from rvas_limpio_esta_c rl
left join rvas_limpio_para_borrar borrar on 
borrar.reclama=rl.reclama and borrar.cob=rl.cob and borrar.afe=rl.afe
where borrar.reclama is null;

-- unimos rvasexac_c30 y base cambios tot proceden
create table insumos.rvasexac_c30_ca_tot as 
select 
c_30.cau_cto,
c_30.reclama,
c_30.poliza,
c_30.cto_mto,
c_30.cto_mto_o,
c_30.fec_ocu,
c_30.fec_mov,
c_30.afe,
c_30.ramo,
c_30.imp_mto,
cast(c_30.idereg as bigint) as idereg,
c_30.cob,
c_30.tpomovexa,
c_30.fecrepexa,
c_30.noape,
c_30.cve_per,
c_30.csubramo,
c_30.fte_info,
c_30.fec_fin,
c_30.fec_ini,
c_30.litigio,
c_30.menor_6,
c_30.mov_pagos,
c_30.pag_dif_c,
c_30.es_cero,
c_30.cris,
c_30.accion,
c_30.p_men_6,
c_30.c12,
c_30.c24,
c_30.c30,
c_30.llave,
c_30.esta_c
from rvasexac_c30_ca c_30
union 
select 
bct_cp.cau_cto,
bct_cp.reclama,
bct_cp.poliza,
bct_cp.cto_mto,
bct_cp.cto_mto_o,
bct_cp.fec_ocu,
bct_cp.fec_mov,
bct_cp.afe,
bct_cp.ramo,
bct_cp.imp1 as imp_mto,
bct_cp.idereg,
bct_cp.cob,
bct_cp.tpomovexa,
null as fecrepexa,
bct_cp.noape,
bct_cp.cve_per,
bct_cp.csubramo,
bct_cp.fte_info,
null as fec_fin,
null as fec_ini,
bct_cp.litigio,
bct_cp.menor_6,
bct_cp.mov_pagos,
bct_cp.pag_dif_c,
bct_cp.es_cero,
bct_cp.cris,
bct_cp.accion,
'' as p_men_6,
bct_cp.c12,
bct_cp.c24,
bct_cp.c30,
bct_cp.llave,
'' as esta_c
from base_cambios_tot_camb_proceden bct_cp where  bct_cp.c30='C' and bct_cp.es_rvas='S';

-- Marca y eliminamos los catastroficos
create table insumos.siniestros_huracanes_final_31032015_l as 
select reclama, fte_info, count(*) as cuenta from siniestros_huracanes_final_31032015 group by reclama,fte_info;

create table insumos.rvasexac_sin_cat_antes_mov20141025_ca as
select 
c_30.*
from rvasexac_c30_ca_tot c_30
left join siniestros_huracanes_final_31032015_l cat on cat.reclama=c_30.reclama and cat.fte_info=c_30.fte_info
where cat.reclama is null;


-- movimientos programa
create table insumos.ri_25102014_ca as 
select distinct reclama from rvasexac0515 
where cto_mto='RI' and fec_mov>=20141025;

create table ri_20141025_todos_mov_ca as 
select rvas.* from insumos.ri_25102014_ca ri 
left join rvasexac0515 rvas on ri.reclama=rvas.reclama;

create table insumos.ri_20141025_mov_ant_ca as
select * from ri_20141025_todos_mov_ca where fec_mov<20141025;

create table insumos.tot_mov_nac_20141025_ca as 
select todos.* from ri_20141025_todos_mov_ca todos 
left join ri_20141025_mov_ant_ca ant on ant.reclama=todos.reclama
where ant.reclama is null;


-- Unimos las bases finales 
create table insumos.rvasexac_sin_cat_reconstruccion_final_ca as
select  
sin_cat.reclama,
sin_cat.poliza,
sin_cat.cto_mto_o,
sin_cat.cau_cto,
sin_cat.fec_ocu,
sin_cat.fec_mov,
sin_cat.afe,
sin_cat.imp_mto,
sin_cat.cob,
sin_cat.cve_per,
sin_cat.csubramo,
sin_cat.fte_info,
sin_cat.fec_fin,
sin_cat.fec_ini,
sin_cat.litigio,
sin_cat.c12,
sin_cat.c24,
sin_cat.c30,
sin_cat.es_cero,
sin_cat.menor_6,
sin_cat.llave,
'' as es_rvas,
sin_cat.mov_pagos,
sin_cat.pag_dif_c,
sin_cat.esta_c,
sin_cat.accion,
sin_cat.cto_mto,
sin_cat.p_men_6,
sin_cat.cris,
'' as lit,
sin_cat.ramo,
'' as cat,
'' as mov_man
from 
rvasexac_sin_cat_antes_mov20141025_ca sin_cat
union 
select 
tot.reclama,
tot.poliza,
tot.cto_mto,
tot.cau_cto,
tot.fec_ocu,
tot.fec_mov,
tot.afe,
tot.imp_mto,
tot.cob,
tot.cve_per,
tot.csubramo,
tot.fte_info,
tot.fec_fin,
tot.fec_ini,
tot.litigio,
'' as c12,
'' as c24,
'' as c30,
'' as es_cero,
'' as menor_6,
'' as llave,
'' as es_rvas,
'' as mov_pagos,
'' as pag_dif_c,
'' as esta_c,
'' as accion,
tot.cto_mto,
'' as p_men_6,
'' as cris,
'' as lit,
tot.ramo,
'' as cat,
'' as mov_man
from tot_mov_nac_20141025_ca tot;

-- BOrramos las bases que no son necesarias
---drop table if exists insumos.dm2_1213_ca;
drop table if exists insumos.base_cambios_tot_ant_ca;
drop table if exists insumos.base_r1_ca_Recons_Ante;
drop table if exists insumos.base_cambios_tot;
drop table if exists insumos.rvasexac_sin_ingresos;
drop table if exists insumos.base_cambios_tot_camb_proceden;
drop table if exists insumos.rvas_limpio_marca_periodos;
drop table if exists insumos.rvas_limpio_para_borrar;
drop table if exists insumos.rvasexac_c30_ca;
drop table if exists insumos.siniestros_huracanes_final_31032015_l;
drop table if exists insumos.rvasexac_sin_cat_antes_mov20141025_ca;
drop table if exists insumos.ri_25102014_ca;
drop table if exists insumos.ri_20141025_todos_mov_ca;
drop table if exists insumos.ri_20141025_mov_ant_ca;
drop table if exists insumos.ri_20141025_mov_ant_ca;
drop table if exists insumos.tot_mov_nac_20141025_ca;
drop table if exists insumos.base_r1_ca_adi_campos;
drop table if exists insumos.base_cambios_tot_ant_ca_l;
drop table if exists insumos.base_r1__spark_temp;
