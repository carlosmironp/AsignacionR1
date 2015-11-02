use insumos;


drop table if exists insumos.rvasexac_sin_ingresos;
drop table if exists insumos.base_cambios_tot_rec_campos;
drop table if exists insumos.prop_periodo;
drop table if exists insumos.base_cambios_tot_rec_campos2;
drop table if exists insumos.historias_cristales;
drop table if exists insumos.base_cambios_to_cris_t;
drop table if exists insumos.es_cero_cto_o;
drop table if exists insumos.no_r2_saldos;
drop table if exists insumos.base_cambios_to_cris_t_2;
drop table if exists insumos.base_cambios_tot_camb_proceden;
drop table if exists insumos.base_cambios_tot_camb_proceden_borrar;
drop table if exists insumos.base_cambios_tot_camb_proceden_limpia;
drop table if exists insumos.rvasexac_original_limpia_periodos;
drop table if exists insumos.propaga_marca_periodos;
drop table if exists insumos.rvasexac_limpio_periodos;
drop table if exists insumos.rvasexac_limpia_esta_c;
drop table if exists insumos.base_cambios_tot_index;
drop table if exists insumos.rvasexac_para_borrar;
drop table if exists insumos.rvasexac_c30;
drop table if exists insumos.base_cambios;
drop table if exists insumos.rvasexac_c30_union_base_cambios;
drop table if exists insumos.catastroficos_unicos;
drop table if exists insumos.rvasexac_c30_marca_catastroficos;
drop table if exists insumos.rvasexac0515_sin_cat_antes_mov20141025;
drop table if exists insumos.ri_25102014_ca;
drop table if exists insumos.ri_20141025_todos_mov_ca;
drop table if exists insumos.reclamaciones_anteriores;
drop table if exists insumos.tot_mov_nac_20141025_ca;
drop table if exists insumos.rvasexac0515_sin_cat_antes_mov20141025_2;
drop table if exists insumos.rvasexac_sin_cat_reconstruccion_final;

-- Filtramos de revas limpio los ingresos y guardamos la tabla temporal en
create table insumos.rvasexac_sin_ingresos as
select * from rvasexac_accion where accion<>'IN' and accion<>'NC';

-- Recuperamos los campos calculados en limpia revas aqui pudimos hacer join por el idereg,cto_mto y fec_mov lo cual facilito la relacion uno a uno
create table insumos.base_cambios_tot_rec_campos as
select 
rt.catego_r1,rt.idereg, rt.reclama,rt.cve_per,
rt.cto_mto as cto_mto_o,
rt.fec_ocu,rt.fec_mov,rt.afe,rt.cob,rt.cero,rt.dias,rt.r1,
rt.imp_mto as imp_to_o,
rt.saldo, rt.cto1,
rt.saldo1,rt.r1_b,rt.fec_mov1,
rt.r1_nueva,rt.fecmov2,rt.es_r1,rt.consecu,rt.caso,
rt.cuenta,rt.regis_nvo,rsi.cris,rsi.csubramo,rsi.fte_info,
rsi.poliza,rsi.cau_cto,rsi.ramo,rsi.noape,rsi.mov_pagos,
rsi.pag_dif_c,rsi.es_cero,rsi.litigio,
case 
  when rt.fec_mov>=20150501 then 'S'
end as mov_pos,
case
  when (rt.cto_mto='RI' and rt.fec_mov>=20150601) then 'A'
end as c12,
case
  when (rt.cto_mto='RI' and rt.fec_mov>=20130601) then 'B'
end as c24,
case
  when rt.cto_mto='RI' and rt.fec_mov>20121201 then 'C'
end as c30,
case 
   when (rt.cto_mto='RI' and rt.fec_mov>=20150501) then 'ABC'
   when (rt.cto_mto='RI' and rt.fec_mov>=20130501) then 'BC'
   when rt.cto_mto='RI' and rt.fec_mov>20121201 then 'C' 
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

-- Propagar el c12,24 o 30
create table insumos.prop_periodo as
select * from base_cambios_tot_rec_campos
where llave is not null;

create table insumos.base_cambios_tot_rec_campos2 as
select  
a.catego_r1,a.idereg,a.reclama,a.cve_per,
a.cto_mto_o,a.fec_ocu,a.fec_mov,a.afe,
a.cob,a.cero,a.dias, a.r1,a.imp_to_o as imp_mto_o,
a.saldo,a.cto1,a.saldo1,a.r1_b,a.fec_mov1,
a.r1_nueva,a.fecmov2,a.es_r1,a.consecu,
a.caso, a.cuenta,
a.regis_nvo,a.cris,a.csubramo,a.fte_info,
a.poliza, a.cau_cto, a.ramo,a.noape,a.mov_pagos,
a.pag_dif_c,a.es_cero,a.litigio,a.mov_pos,
b.c12,
b.c24,
b.c30,
b.llave,
a.menor_6,a.accion,a.es_rvas,a.tpomovexa,
a.imp_mto,a.imp1,a.cto_mto
from base_cambios_tot_rec_campos a 
left join prop_periodo b on 
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe;


-- Propagamos la marca de cristal a los demas movimientos en cada historia
create table insumos.historias_cristales as 
select reclama, cob, afe, count(*) 
from base_cambios_tot_rec_campos2
where cris ='S' group by reclama, cob, afe;

create table insumos.base_cambios_to_cris_t as
select tot.*, 
case
  when cris.reclama is not null then 'S'
end as cris_t
from base_cambios_tot_rec_campos2 tot
left join historias_cristales cris on cris.reclama=tot.reclama and cris.cob=tot.cob and cris.afe=tot.afe;



-- NO R2
create table insumos.es_cero_cto_o as 
select reclama, cob, afe, count(*) as movs  from base_cambios_to_cris_t
where es_cero='S' and cto_mto is not null 
group by reclama, cob, afe
having movs=2;

-- Marcamos las no R2
create table insumos.no_r2_saldos as 
select 
a.*
from base_cambios_to_cris_t a
left join es_cero_cto_o b on
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe
where b.reclama is not null and a.cto_mto='RI';

--Marcamos la no_r2 ya con su importe ajusta pora netearlas a cero
create table insumos.base_cambios_to_cris_t_2 as
  select 
a.catego_r1,a.idereg,a.reclama,a.cve_per,a.cto_mto_o,a.fec_ocu,
a.fec_mov,a.afe,a.cob,a.cero,a.dias,
case 
   when b.reclama is not null and a.cto_mto='A-' and a.r1='U' then ' '
   else a.r1
end as r1,
a.imp_mto_o,a.saldo,a.cto1,a.saldo1,a.r1_b,a.fec_mov1,a.r1_nueva,
a.fecmov2,a.es_r1,a.consecu,a.caso,a.cuenta,a.regis_nvo,a.cris,
a.csubramo,a.fte_info,a.poliza,a.cau_cto,a.ramo,a.noape,a.mov_pagos,
a.pag_dif_c,a.es_cero,a.litigio,a.mov_pos,a.c12,a.c24,a.c30,a.llave,
a.menor_6,a.accion,a.es_rvas,a.tpomovexa,
case 
   when b.reclama is not null and a.cto_mto='A-' and a.r1='U' then (b.saldo1*(-1))
   else a.imp_mto
end as imp_mto,
case 
   when b.reclama is not null and a.cto_mto='A-' and a.r1='U' then (b.saldo1*(-1))
   else a.imp1
end as imp1,
a.cto_mto,
a.cris_t,
case 
   when b.reclama is not null then 'X'
end as no_r2
from base_cambios_to_cris_t a
left join no_r2_saldos b on
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe;



-- Sacamos a base cambios los movimientos que proceden y que se van a sustituir como lo marca el procedimiento cambios proceden
create table insumos.base_cambios_tot_camb_proceden as 
select *  from base_cambios_to_cris_t_2 base_cambios_tot 
where base_cambios_tot.imp1<>0 and base_cambios_tot.mov_pos is null and base_cambios_tot.es_rvas='S';

--Identificamos aquellos movimientos para borrar que no se tomaran encuenta 
create table insumos.base_cambios_tot_camb_proceden_borrar as
select reclama, cob, afe, count(*) from base_cambios_tot_camb_proceden
where cob='DM' and menor_6='S' and cris_t<>'S' and es_cero='S' and pag_dif_c<>'S'
group by reclama, cob, afe;

--Quitamos los movimientos 
create table insumos.base_cambios_tot_camb_proceden_limpia as
select a.* from base_cambios_tot_camb_proceden a
left join base_cambios_tot_camb_proceden_borrar  b on
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe
where b.reclama is null;

-- Marcamos los periodos en el rvasexac limpio original
create table insumos.rvasexac_original_limpia_periodos as 
select *,
case
  when (cto_mto='RI' and fec_mov>=20150601) then 'A'
end as c12,
case
  when (cto_mto='RI' and fec_mov>=20130601) then 'B'
end as c24,
case
  when cto_mto='RI' and fec_mov>20121201 then 'C'
end as c30,
case 
   when (cto_mto='RI' and fec_mov>=20150601) then 'ABC'
   when (cto_mto='RI' and fec_mov>=20130601) then 'BC'
   when cto_mto='RI' and  fec_mov>20121201 then 'C' 
end as llave
from rvasexac_accion;

-- Sacamos del rvas limpio con los periodos marcados aqullos movimientos donde su llave NO sea null y generamos un indice
create table insumos.propaga_marca_periodos as
select * from rvasexac_original_limpia_periodos 
where llave is not null;

--Propagamos la marca del periodo a los demas movimientos de cada historia
create table insumos.rvasexac_limpio_periodos as 
select 
a.cau_cto,
a.reclama,
a.poliza,
a.cto_mto,
a.cto_mto_o,
a.fec_ocu,
a.fec_mov,
a.afe,
a.ramo,
a.imp_mto,
a.idereg,
a.cob,
a.tpomovexa,
a.fecrepexa,
a.noape,
a.cve_per,
a.csubramo,
a.fte_info,
a.fec_fin,
a.fec_ini,
a.litigio,
a.menor_6,
a.mov_pagos,
a.pag_dif_c,
a.es_cero,
a.cris,
a.accion,
a.p_men_6,
b.c12,
b.c24,
b.c30,
b.llave
from 
rvasexac_original_limpia_periodos a
left join propaga_marca_periodos b on
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe;

--Marcamos en el rvas original limpio aquellos movimientos que tambien estan el la reconstrccion
create table insumos.base_cambios_tot_index as
select reclama, cob, afe, count(*) as movs from base_cambios_tot_camb_proceden_limpia
group by reclama, cob, afe;

create table insumos.rvasexac_limpia_esta_c as
select a.*,
case
   when b.reclama is not null and ( a.cto_mto<>'IN' AND a.cto_mto<>'IS' AND a.cto_mto<>'RS' AND a.accion<>'NC') then 'S'
end as esta_c
from rvasexac_limpio_periodos a
left join base_cambios_tot_index b on 
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe;

--Estos movimientos se van a borrar del rvas original ya que son los movimientos que estan reconstruidos
create table insumos.rvasexac_para_borrar as 
select * from rvasexac_limpia_esta_c
where (c30='C' and accion='SA') OR (c30='C' and accion='CA' and esta_c='S');

--Del rvas original limpiamos los registros que estamos reconstruyendo y generemos la base a 30 meses
create table insumos.rvasexac_c30 as
select a.* from rvasexac_limpia_esta_c a
left join rvasexac_para_borrar b on 
a.reclama=b.reclama and a.cob=b.cob and a.afe=b.afe and a.fec_mov=b.fec_mov and cast(a.idereg as string)=b.idereg and a.cto_mto=b.cto_mto
where b.reclama is null;


--Base cambios son los movimientos reconstruidos NOTA que aqui solo se esta llevando las RI, cabezas de las historias de movimientos 
create table insumos.base_cambios as 
select * from base_cambios_tot_camb_proceden_limpia
where c30='C' and es_rvas='S';

-- Unimos ambas bases para obtener la base rvas original mas los movimientos (primer movimiento) que ya reconstruimos
create table insumos.rvasexac_c30_union_base_cambios as 
select 
c_30.cau_cto,
c_30.reclama,
c_30.poliza,
c_30.cto_mto,
c_30.cto_mto_o,
c_30.fec_ocu,
c_30.fec_mov,
c_30.afe,
'R' as ramo,
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
from insumos.rvasexac_c30 c_30
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
case
   when bct_cp.ramo <> 'R' then 'C'
end as ramo,
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
from insumos.base_cambios bct_cp;

-- Guarda los catastroficos y  los pone en un indice
create table insumos.catastroficos_unicos as 
select 
reclama,
fte_info,
count(*) as cuenta
from siniestros_huracanes_final_31032015
group by reclama, fte_info;

-- Marca las historias que son catastroficos en la base de rvasexac_30 unida con base cambios
create table insumos.rvasexac_c30_marca_catastroficos as
select 
a.*,
case
   when b.reclama is not null then 'X'
end as cat
from rvasexac_c30_union_base_cambios a
left join catastroficos_unicos b on a.reclama=b.reclama and a.fte_info=b.fte_info;

-- Solo copiamos a una base las historias que no son catastroficos
create table insumos.rvasexac0515_sin_cat_antes_mov20141025 as
select * from rvasexac_c30_marca_catastroficos
where cat is null;



-- PROGRAMA MOVIMIENTOS  Obtenemos las RI que fueron posterior a la fecha de cambio de operacion
create table insumos.ri_25102014_ca as 
select distinct reclama from rvasexac0515 
where cto_mto='RI' and fec_mov>=20141025;

-- Obtenemos todos los movimientos de las RI posteriores ala fecha de operacion
create table insumos.ri_20141025_todos_mov_ca as 
select rvas.* from insumos.ri_25102014_ca ri 
left join rvasexac0515 rvas on ri.reclama=rvas.reclama;

--Identificamos aquellas reclamaciones que fueron antes del cambio de operacion
create table insumos.reclamaciones_anteriores as 
select distinct reclama from ri_20141025_todos_mov_ca
where fec_mov < 20141025;

--Copiamos solo aquellos movimientos que no son movimientos anteriores
create table insumos.tot_mov_nac_20141025_ca as 
select a.* 
from ri_20141025_todos_mov_ca a
left join reclamaciones_anteriores b on a.reclama=b.reclama
where b.reclama is null;

--Quitamos los movimientos posteriores
create table insumos.rvasexac0515_sin_cat_antes_mov20141025_2 as
select a.*
from rvasexac0515_sin_cat_antes_mov20141025 a
left join tot_mov_nac_20141025_ca b on
a.reclama=b.reclama and 
a.cob=b.cob and 
a.afe=b.afe and 
a.fte_info=b.fte_info and 
a.fec_mov=b.fec_mov and 
cast(a.idereg as string)=b.idereg and 
a.cto_mto=b.cto_mto
where b.reclama is null;

-- SE GENERA LA BASE FINAL
create table insumos.rvasexac_sin_cat_reconstruccion_final as
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
rvasexac0515_sin_cat_antes_mov20141025_2 sin_cat
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