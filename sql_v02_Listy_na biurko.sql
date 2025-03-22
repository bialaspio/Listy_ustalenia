--*************************************************************************************************************************************************
-- Listy na dzien :
--*************************************************************************************************************************************************

alter table ZAWIADOMIENIA_listy add ogc_fid serial;
drop table if exists pwpg_ZAWIADOMIENIA_listy;
create table pwpg_ZAWIADOMIENIA_listy as 
select ogc_fid ,regexp_split_to_table(pwpg, E',') as pwpg from ZAWIADOMIENIA_listy;
select data, count (data) from ZAWIADOMIENIA_listy group by data order by data;

--30.06.2022	57
--20220630
drop table if exists lv2_nazw_lp_20220630_A;
create table lv2_nazw_lp_20220630_A AS select DISTINCT substring(A.s from 3 for 1) AS stan, A.data,(SELECT string_agg (B.ddd,', ') from ZAWIADOMIENIA_listy B where b.data like '30.06.2022' and A.wl=B.wl and A.rodzice = B.rodzice and substring(B.s from 3 for 1) like 'A') AS dzialki, ''::varchar::varchar  AS PESEL, ''::varchar AS Numer_Dowodu, wl,rodzice,(select string_agg(pwpg, ',' order by pwpg::int) from pwpg_ZAWIADOMIENIA_listy where ogc_fid in (SELECT ogc_fid from ZAWIADOMIENIA_listy C where C.data like '30.06.2022' and A.wl=C.wl and A.rodzice = C.rodzice and substring(C.s from 3 for 1) like 'A')) AS pwpg from ZAWIADOMIENIA_listy A where A.data like '30.06.2022' and substring(A.s from 3 for 1) like 'A' order by wl;

--04.07.2022	54
--20220704
drop table if exists lv2_nazw_lp_20220704_A;
create table lv2_nazw_lp_20220704_A AS select DISTINCT substring(A.s from 3 for 1) AS stan, A.data,(SELECT string_agg (B.ddd,', ') from ZAWIADOMIENIA_listy B where b.data like '04.07.2022' and A.wl=B.wl and A.rodzice = B.rodzice and substring(B.s from 3 for 1) like 'A') AS dzialki, ''::varchar::varchar  AS PESEL, ''::varchar AS Numer_Dowodu, wl,rodzice,(select string_agg(pwpg, ',' order by pwpg::int) from pwpg_ZAWIADOMIENIA_listy where ogc_fid in (SELECT ogc_fid from ZAWIADOMIENIA_listy C where C.data like '04.07.2022' and A.wl=C.wl and A.rodzice = C.rodzice and substring(C.s from 3 for 1) like 'A')) AS pwpg from ZAWIADOMIENIA_listy A where A.data like '04.07.2022' and substring(A.s from 3 for 1) like 'A' order by wl;

--**********************************************************************************************************************************************************
COPY  lv2_nazw_lp_20220630_A (data, dzialki,pesel,numer_dowodu, wl, rodzice, pwpg) TO 'c:\PB\Krakowski\Dojazdow_0003\lv2_nazw_lp_20220630_A.csv' DELIMITER ';' CSV HEADER;
COPY  lv2_nazw_lp_20220704_A (data, dzialki,pesel,numer_dowodu, wl, rodzice, pwpg) TO 'c:\PB\Krakowski\Dojazdow_0003\lv2_nazw_lp_20220704_A.csv' DELIMITER ';' CSV HEADER;
