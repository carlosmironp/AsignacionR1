use insumos;

drop table if exists insumos.rvasexac0515_adiciona_campos;
drop table if exists insumos.rvasexac0515_pagos_total;
drop table if exists insumos.rvasexac0515_adiciona_campos_pagos;
drop table if exists insumos.rvasexac0515_movs_total;
drop table if exists insumos.rvasexac0515_es_cero;
drop table if exists insumos.rvasexac0515_ri_menor_6;
drop table if exists insumos.rvasexac0515_menor_6;
drop table if exists insumos.rvasexac0515_cristales;
drop table if exists insumos.rvasexac0515_LCT;
drop table if exists insumos.cristales2;
drop table if exists insumos.rvasexac0515_accion;

--Adiciona campos
create table insumos.rvasexac0515_adiciona_campos as
	select 
rvas.id, rvas.reclama,rvas.poliza,rvas.cto_mto as cto_mto_o,
case
 when cto_mto='CP'  or cto_mto='PP'   or  cto_mto='PF' then 'PP'
 else cto_mto
end as cto_mto,
rvas.cau_cto,rvas.fec_ocu,rvas.fec_mov,rvas.afe,
rvas.ramo,rvas.imp_mto,rvas.idereg,rvas.cob,rvas.tpomovexa,
rvas.fecrepexa,rvas.noape,rvas.cve_per,
rvas.csubramo,rvas.fte_info,rvas.fec_fin,rvas.fec_ini,
rvas.litigio
from rvasexac0515_spark rvas; 
compute stats insumos.rvasexac0515_adiciona_campos;

--Totaliza pagos 
create table insumos.rvasexac0515_pagos_total as
select reclama, cob, afe,fte_info, sum(imp_mto) as sum_imp_mto
from rvasexac0515_adiciona_campos 
where cto_mto ='PP'
group by reclama, cob, afe, fte_info;
compute stats insumos.rvasexac0515_pagos_total;

--Calcula campos pagos
create table insumos.rvasexac0515_adiciona_campos_pagos as
select 
ac.id, ac.cau_cto, ac.reclama,ac.poliza,ac.cto_mto,ac.fec_ocu,ac.fec_mov,ac.cto_mto_o,
ac.afe,ac.ramo,ac.imp_mto,ac.idereg,ac.cob,
ac.tpomovexa,ac.fecrepexa,ac.noape,ac.cve_per,
ac.csubramo,ac.fte_info,ac.fec_fin,ac.fec_ini,
ac.litigio,
case 
   when pt.reclama is not null then 'S'
end mov_pagos,
case 
   when pt.sum_imp_mto is not null and pt.sum_imp_mto <>0 then 'S'
end pag_dif_c,
case 
   when pt.sum_imp_mto is not null and pt.sum_imp_mto <= 6000 then 'S'
end p_men_6
from rvasexac0515_adiciona_campos ac
left join rvasexac0515_pagos_total pt on
pt.reclama=ac.reclama and pt.cob=ac.cob and pt.fte_info=ac.fte_info and pt.afe=ac.afe;

--Totaliza movimiento RI,RE,A+,A-
create table insumos.rvasexac0515_movs_total as
select reclama, cob, afe,fte_info, sum(imp_mto) as sum_imp_mto
from rvasexac0515_adiciona_campos 
where cto_mto ='RI' or cto_mto ='A+' or cto_mto ='A-' or cto_mto ='RE'
group by reclama, cob, afe, fte_info;

--Calcula campos es_cero
create table insumos.rvasexac0515_es_cero as
select 
ac.id, ac.cau_cto, ac.reclama,ac.poliza,ac.cto_mto,ac.cto_mto_o,
ac.fec_ocu,ac.fec_mov,
ac.afe,ac.ramo,ac.imp_mto,ac.idereg,ac.cob,
ac.tpomovexa,ac.fecrepexa,ac.noape,ac.cve_per,
ac.csubramo,ac.fte_info,ac.fec_fin,ac.fec_ini,
ac.litigio,
case 
    when pt.reclama is not null and pt.sum_imp_mto=0 then 'S'
end es_cero,
ac.mov_pagos,ac.pag_dif_c,ac.p_men_6
from rvasexac0515_adiciona_campos_pagos ac
left join rvasexac0515_movs_total pt on
pt.reclama=ac.reclama and pt.cob=ac.cob and pt.fte_info=ac.fte_info and pt.afe=ac.afe;

--RI menor a 6000
create table insumos.rvasexac0515_ri_menor_6 as
select reclama, cob, afe,fte_info, sum(imp_mto) as sum_imp_mto
from rvasexac0515_adiciona_campos 
where cto_mto ='RI' and imp_mto <=6000 and cob ='DM'
group by reclama, cob, afe, fte_info;

--Calcula ri menor 6000
create table insumos.rvasexac0515_menor_6 as
select 
ac.id, ac.cau_cto,ac.reclama,ac.poliza,ac.cto_mto,ac.cto_mto_o,
ac.fec_ocu,ac.fec_mov,
ac.afe,ac.ramo,ac.imp_mto,ac.idereg,ac.cob,
ac.tpomovexa,ac.fecrepexa,ac.noape,ac.cve_per,
ac.csubramo,ac.fte_info,ac.fec_fin,ac.fec_ini,
ac.litigio,ac.es_cero,
case
    when pt.reclama is not null and pt.sum_imp_mto<=6000 then 'S'
end as menor_6,
ac.mov_pagos,ac.pag_dif_c,ac.p_men_6
from rvasexac0515_es_cero ac
left join rvasexac0515_ri_menor_6 pt on
pt.reclama=ac.reclama and pt.cob=ac.cob and pt.fte_info=ac.fte_info and pt.afe=ac.afe;

--Buscamos aquellos registros con las caracteristicas especificas, para determinar si es un cristal Muchos registros no se encuentran en la tabla de cristales pero son identificados implicitamente por que son:DM, RI, 1759
create table cristales2 as select distinct reservas.reclama from rvasexac0515 reservas where reservas.cto_mto='RI' and reservas.cob='DM' and imp_mto=1759;
COMPUTE STATS insumos.cristales2;


--Marca los cristales
create table insumos.rvasexac0515_cristales as
select rvas.*,
case
   when cris1.reclama is not null then 'S'
   when cris2.reclama is not null then 'S'
end as cris
from rvasexac0515_menor_6 rvas
left join cristales_2010_2015_0515_l cris1 on cris1.reclama=rvas.reclama
left join cristales2 cris2 on cris2.reclama=rvas.reclama;

--Limpia litigios, condusef y terminados
create table insumos.litigios_l as 
select reclama_b, count(*) as cuenta from litigios group by reclama_b;

create table insumos.condusef_l as 
select reclama_b, count(*) as cuenta from condusef group by reclama_b;

create table insumos.terminados_l as 
select reclama_b, count(*) as cuenta from terminados group by reclama_b;

--Marca litigios, condusef y terminados
create table insumos.rvasexac0515_LCT as
select 
rvas.*,
case 
   when liti.reclama_b is not null then 'L' 
   when cond.reclama_b is not null then 'C'
   when term.reclama_b is not null then 'T'
end as lit
from rvasexac0515_cristales rvas
left join litigios_l liti on liti.reclama_b=rvas.reclama
left join condusef_l cond on cond.reclama_b=rvas.reclama
left join terminados_l term on term.reclama_b=rvas.reclama;

--Calcula la accion a tomar para la reconstruccion
create table insumos.rvasexac0515_accion as
select
ac.id, ac.cau_cto,ac.reclama,ac.poliza,ac.cto_mto,ac.cto_mto_o,
ac.fec_ocu,ac.fec_mov,
ac.afe,ac.ramo,ac.imp_mto,ac.idereg,ac.cob,
ac.tpomovexa,ac.fecrepexa,ac.noape,ac.cve_per,
ac.csubramo,ac.fte_info,ac.fec_fin,ac.fec_ini,
ac.litigio,ac.menor_6,ac.mov_pagos,ac.pag_dif_c,
ac.es_cero,ac.cris,
case
   when ac.cto_mto='IN' or ac.cto_mto='IS' or ac.cto_mto='RS'  then 'IN'
   when ac.cob='DM' and ac.menor_6='S' and ac.cris<>'S' and ac.pag_dif_c<>'S' and ac.cto_mto<>'IN' then 'SA'
   when ac.cob='DM' and ac.menor_6='S' and ac.p_men_6='S' and ac.pag_dif_c<>'S' and ac.cris<>'S'  then 'NC'
   when ac.lit='L' or lit='C' then 'NC'
   when ac.cto_mto<>'IN' then 'CA'
end as accion,
ac.p_men_6
from rvasexac0515_LCT ac;
compute stats insumos.rvasexac0515_accion;

drop table if exists inusmos.litigios_l;
drop table if exists inusmos.condusef_l;
drop table if exists inusmos.terminados_l;
drop table if exists insumos.rvasexac0515_adiciona_campos;
drop table if exists insumos.rvasexac0515_pagos_total;
drop table if exists insumos.rvasexac0515_adiciona_campos_pagos;
drop table if exists insumos.rvasexac0515_movs_total;
drop table if exists insumos.rvasexac0515_es_cero;
drop table if exists insumos.rvasexac0515_ri_menor_6;
drop table if exists insumos.rvasexac0515_menor_6;
drop table if exists insumos.rvasexac0515_cristales;
drop table if exists insumos.rvasexac0515_LCT;